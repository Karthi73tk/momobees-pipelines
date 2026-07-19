#!/usr/bin/env python3
"""
Rebalance: for each active portfolio whose cadence says it's due, compare
current holdings against today's momentum Top-N (from strategies.momentum —
ranking still comes from Supabase, only pricing comes from TVDatafeed) and:
  - sell tickers that fell out of the Top-N
  - buy new Top-N tickers not currently held, equal-weighted with freed cash
  - leave holdings that are still in the Top-N untouched (no re-weighting)

This script runs daily, but internally self-gates using last_rebalanced_at
and rebalance_frequency.
"""
import os
import sys
import pathlib
import json
import logging
import argparse
import datetime
from datetime import date

# Ensure scripts directory is in python search path so we can import momentum_scanner
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from supabase import create_client, Client
from dotenv import load_dotenv

# Try to load local env file for local testing
try:
    load_dotenv(".env.local")
except ImportError:
    pass

from typing import Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# We import the TVDatafeed fetch helper from momentum_scanner to reuse its rate limiting and retries.
try:
    from momentum_scanner import fetch_history, MAX_WORKERS
except ImportError as e:
    raise SystemExit(
        "Could not import from momentum_scanner. "
        "Ensure this script is run from the repo root or scripts/ directory."
    ) from e

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("portfolio_rebalance")

# --- Configuration ---
SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

BENCHMARK_TICKER = os.environ.get("BENCHMARK_TICKER", "NIFTY")
BENCHMARK_EXCHANGE = os.environ.get("BENCHMARK_EXCHANGE", "NSE")
STOCK_EXCHANGE = os.environ.get("STOCK_EXCHANGE", "NSE")

ACTIVE_STATUSES = ["virtual", "real"]
CADENCE_DAYS = {"weekly": 7, "monthly": 28}


def parse_ticker(ticker: str) -> tuple[str, str]:
    """
    'NSE:RELIANCE' -> ('RELIANCE', 'NSE')
    'RELIANCE'     -> ('RELIANCE', STOCK_EXCHANGE)
    """
    if ":" in ticker:
        exchange, symbol = ticker.split(":", 1)
        return symbol, exchange
    return ticker, STOCK_EXCHANGE


def fetch_ticker_prices(tickers: set[str]) -> dict[str, float]:
    """
    Fetch close price for a set of tickers. Deduplicates across all portfolios
    and queries TradingView exactly once per unique ticker per run.
    """
    unique_tickers = sorted(list(tickers))
    if not unique_tickers:
        return {}

    prices = {}

    def fetch_one(ticker: str) -> Tuple[str, Optional[float]]:
        symbol, exchange = parse_ticker(ticker)
        try:
            df = fetch_history(symbol, exchange, n_bars=5)
            if df is not None and not df.empty:
                return ticker, float(df["close"].iloc[-1])
            else:
                log.warning("No price data returned for %s:%s", exchange, symbol)
        except Exception as exc:
            log.error("Failed to fetch price for %s:%s: %s", exchange, symbol, exc)
        return ticker, None

    log.info("Fetching %d unique tickers in parallel using %d workers", len(unique_tickers), MAX_WORKERS)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ticker = {executor.submit(fetch_one, t): t for t in unique_tickers}
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                t, val = future.result()
                if val is not None:
                    prices[t] = val
                    log.info("Fetched price for %s: %.2f", t, val)
            except Exception as exc:
                log.error("Unhandled thread exception for %s: %s", ticker, exc)

    return prices


def _is_due(portfolio: dict, today: date) -> bool:
    last = portfolio.get("last_rebalanced_at")
    last_date = date.fromisoformat(last) if last else None
    strategy_key = portfolio.get("strategy_key")
    freq = portfolio.get("rebalance_frequency", "weekly")

    if strategy_key == "momentum_n750" and freq == "monthly":
        day = portfolio.get("rebalance_day_of_month")
        if day is None:
            log.warning("Portfolio %s is missing rebalance_day_of_month. Falling back to rolling 28-day logic.", portfolio.get("id"))
            if not last_date: return True
            return (today - last_date).days >= 28
        
        if today.day == day:
            if not last_date:
                return True
            return (today.year != last_date.year) or (today.month != last_date.month)
        return False

    if strategy_key == "momentum_weekly":
        if today.weekday() == 0:
            if not last_date:
                return True
            return last_date < today
        return False

    # Default fallback: rolling cadence
    if not last_date:
        return True
    cadence_days = CADENCE_DAYS.get(freq, 7)
    return (today - last_date).days >= cadence_days


def _fetch_top_n_tickers(momentum_client, n: int, strategy_key: str) -> list[str]:
    table_name = "momentum_weekly" if strategy_key == "momentum_weekly" else "momentum"
    resp = (
        momentum_client.table(table_name)
        .select("ticker, rank")
        .lte("rank", n)
        .order("rank")
        .execute()
    )
    return [row["ticker"] for row in (resp.data or [])]


def _rebalance_one(sb, momentum_client, portfolio: dict, today: date) -> None:
    pid = portfolio["id"]
    target_size = portfolio["target_size"]
    strategy_key = portfolio.get("strategy_key")

    holdings = sb.table("holdings").select("*").eq("portfolio_id", pid).eq("status", "open").execute().data
    held_tickers = {h["ticker"] for h in holdings}

    is_go_cash = False
    top_n_tickers = set()

    if strategy_key == "momentum_weekly":
        regime_resp = momentum_client.table("momentum_weekly_regime").select("regime_action, uncorrelated_asset_symbol").order("asof_date", desc=True).limit(1).execute()
        regime_data = regime_resp.data[0] if regime_resp.data else None
        
        if regime_data and regime_data.get("regime_action") == "GO_CASH":
            is_go_cash = True
            symbol = regime_data.get("uncorrelated_asset_symbol")
            if symbol:
                top_n_tickers = {symbol}
        else:
            top_n_tickers = set(_fetch_top_n_tickers(momentum_client, target_size, strategy_key))
    else:
        top_n_tickers = set(_fetch_top_n_tickers(momentum_client, target_size, strategy_key))

    if not top_n_tickers and not is_go_cash:
        log.warning("No momentum picks available — skipping rebalance for portfolio %s", pid)
        raise ValueError("No momentum picks available")

    if len(top_n_tickers) < target_size and not is_go_cash:
        log.warning("[%s] Only %d momentum picks available for target size %d", pid, len(top_n_tickers), target_size)

    to_exit = [h for h in holdings if h["ticker"] not in top_n_tickers]
    to_enter_tickers = top_n_tickers - held_tickers

    all_needed_prices = {h["ticker"] for h in to_exit} | to_enter_tickers
    prices = fetch_ticker_prices(all_needed_prices)

    trades = []
    freed_cash = 0.0

    # ── Exits ──────────────────────────────────────────────────────────────
    for h in to_exit:
        exit_price = prices.get(h["ticker"])
        if exit_price is None:
            # Skip the exit entirely rather than fabricating an exit price
            log.warning("[%s] Skipping exit for %s due to missing live price", pid, h["ticker"])
            continue
        
        trades.append({
            "holding_id": h["id"],
            "ticker": h["ticker"],
            "quantity": h["quantity"],
            "price": exit_price,
            "type": "SELL"
        })
        freed_cash += float(h["quantity"]) * exit_price
        log.info("[%s] prepared exit for %s @ %.2f (rebalance_exit)", pid, h["ticker"], exit_price)

    cash_pool = float(portfolio["remaining_cash"]) + freed_cash

    # ── Entries ────────────────────────────────────────────────────────────
    entries = [t for t in to_enter_tickers if t in prices]
    skipped = to_enter_tickers - set(entries)
    if skipped:
        log.warning("[%s] skipping entries with no price this run: %s", pid, skipped)

    if entries:
        alloc_per_entry = cash_pool / len(entries)
        for ticker in entries:
            price = prices[ticker]
            if price <= 0:
                log.error("[%s] Zero/negative price for %s (%.2f). Skipping.", pid, ticker, price)
                continue
            qty = int(alloc_per_entry // price)
            if qty <= 0:
                continue
            
            trades.append({
                "ticker": ticker,
                "quantity": qty,
                "price": price,
                "type": "BUY"
            })
            log.info("[%s] prepared entry for %s x%d @ %.2f", pid, ticker, qty, price)

    # ── Execute Database Mutations in Single Atomic Call ───────────────────
    log.info("[%s] Executing atomic rebalance RPC with %d trades...", pid, len(trades))
    
    # We always call execute_rebalance even if trades is empty, so it can
    # bump last_rebalanced_at and write the 'No changes made' log entry.
    res = sb.rpc("execute_rebalance", {
        "p_portfolio_id": pid,
        "p_trades": trades
    }).execute()

    log.info(
        "[%s] rebalance complete: %d exited, %d entered",
        pid, len(to_exit), len(entries),
    )


def rebalance_due_portfolios(force: bool = False) -> dict:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set.")

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    today = date.today()

    portfolios = (
        sb.table("portfolios")
        .select("id, remaining_cash, target_size, rebalance_frequency, last_rebalanced_at, strategy_key, rebalance_day_of_month, broker, pending_rebalance_since")
        .in_("status", ACTIVE_STATUSES)
        .execute()
        .data
    )
    if not portfolios:
        log.info("No active portfolios found.")
        return {"succeeded": 0, "failed": 0, "skipped": 0, "total": 0, "errors": []}

    due = [p for p in portfolios if force or _is_due(p, today)]
    log.info("%d/%d active portfolios are due for rebalance.", len(due), len(portfolios))
    
    skipped_count = len(portfolios) - len(due)
    succeeded_count = 0
    failed_count = 0
    errors = []

    if not due:
        return {
            "succeeded": 0,
            "failed": 0,
            "skipped": skipped_count,
            "total": len(portfolios),
            "errors": []
        }

    momentum_client = sb.schema("strategies")

    for portfolio in due:
        pid = portfolio["id"]
        broker = portfolio.get("broker")

        if broker != "VIRTUAL":
            try:
                if not portfolio.get("pending_rebalance_since"):
                    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    sb.table("portfolios").update({
                        "pending_rebalance_since": now_iso
                    }).eq("id", pid).execute()

                    sb.table("portfolio_logs").insert({
                        "portfolio_id": pid,
                        "log_type": "rebalance",
                        "content": "Rebalance is due and awaiting manual execution"
                    }).execute()
                    log.info("Flagged Real portfolio %s as pending rebalance.", pid)
                succeeded_count += 1
            except Exception as exc:
                log.exception("Failed to flag Real portfolio %s", pid)
                failed_count += 1
                errors.append(f"{pid}: {str(exc)}")
            continue

        try:
            _rebalance_one(sb, momentum_client, portfolio, today)
            succeeded_count += 1
        except Exception as exc:
            log.exception("Failed to rebalance portfolio %s", pid)
            failed_count += 1
            errors.append(f"{pid}: {str(exc)}")

    return {
        "succeeded": succeeded_count,
        "failed": failed_count,
        "skipped": skipped_count,
        "total": len(portfolios),
        "errors": errors
    }


def _write_result(succeeded: int, failed: int, skipped: int, total: int, errors: list) -> None:
    pathlib.Path("results").mkdir(exist_ok=True)
    pathlib.Path("results/portfolio_rebalance.json").write_text(json.dumps({
        "script":    "Portfolio Rebalance",
        "succeeded": succeeded,
        "failed":    failed,
        "skipped":   skipped,
        "total":     total,
        "errors":    errors,
    }))


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebalance active portfolios against momentum Top-N")
    parser.add_argument("--force", action="store_true", help="Force rebalance even if cadence is not due")
    args = parser.parse_args()

    try:
        res = rebalance_due_portfolios(force=args.force)
        _write_result(
            succeeded=res["succeeded"],
            failed=res["failed"],
            skipped=res["skipped"],
            total=res["total"],
            errors=res["errors"]
        )
        if res["failed"] > 0:
            return 1
        return 0
    except Exception as exc:
        log.exception("Rebalance run failed.")
        _write_result(succeeded=0, failed=1, skipped=0, total=0, errors=[str(exc)])
        return 1


if __name__ == "__main__":
    sys.exit(main())
