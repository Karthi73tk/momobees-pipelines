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
    if not last:
        return True
    last_date = date.fromisoformat(last)
    cadence_days = CADENCE_DAYS.get(portfolio.get("rebalance_frequency", "weekly"), 7)
    return (today - last_date).days >= cadence_days


def _fetch_top_n_tickers(sb, n: int) -> list[str]:
    resp = (
        sb.table("momentum")
        .select("ticker, rank")
        .lte("rank", n)
        .order("rank")
        .execute()
    )
    return [row["ticker"] for row in (resp.data or [])]


def _rebalance_one(sb, momentum_client, portfolio: dict, today: date) -> None:
    pid = portfolio["id"]
    target_size = portfolio["target_size"]

    holdings = sb.table("holdings").select("*").eq("portfolio_id", pid).eq("status", "open").execute().data
    held_tickers = {h["ticker"] for h in holdings}

    top_n_tickers = set(_fetch_top_n_tickers(momentum_client, target_size))
    if not top_n_tickers:
        log.warning("No momentum picks available — skipping rebalance for portfolio %s", pid)
        raise ValueError("No momentum picks available")

    to_exit = [h for h in holdings if h["ticker"] not in top_n_tickers]
    to_enter_tickers = top_n_tickers - held_tickers

    all_needed_prices = {h["ticker"] for h in to_exit} | to_enter_tickers
    prices = fetch_ticker_prices(all_needed_prices)

    # ── Exits ──────────────────────────────────────────────────────────────
    freed_cash = 0.0
    for h in to_exit:
        exit_price = prices.get(h["ticker"])
        if exit_price is None:
            # Fall back to entry price rather than failing completely
            exit_price = float(h["entry_price"])
            log.warning("Using entry_price fallback for exit %s", h["ticker"])
        
        freed_cash += float(h["quantity"]) * exit_price

        sb.table("holdings").update({
            "status": "closed",
            "exit_price": exit_price,
            "exit_date": today.isoformat(),
            "exit_reason": "rebalance_exit",
        }).eq("id", h["id"]).execute()

        sb.table("transactions").insert({
            "portfolio_id": pid,
            "ticker": h["ticker"],
            "quantity": h["quantity"],
            "price": exit_price,
            "type": "SELL",
        }).execute()
        log.info("[%s] closed %s @ %.2f (rebalance_exit)", pid, h["ticker"], exit_price)

    cash_pool = float(portfolio["remaining_cash"]) + freed_cash

    # ── Entries ────────────────────────────────────────────────────────────
    entries = [t for t in to_enter_tickers if t in prices]
    skipped = to_enter_tickers - set(entries)
    if skipped:
        log.warning("[%s] skipping entries with no price this run: %s", pid, skipped)

    cash_spent = 0.0
    if entries:
        alloc_per_entry = cash_pool / len(entries)
        for ticker in entries:
            price = prices[ticker]
            qty = int(alloc_per_entry // price)
            if qty <= 0:
                continue
            allocation = qty * price
            cash_spent += allocation

            sb.table("holdings").insert({
                "portfolio_id": pid,
                "ticker": ticker,
                "quantity": qty,
                "entry_price": price,
                "current_price": price,
                "status": "open",
            }).execute()

            sb.table("transactions").insert({
                "portfolio_id": pid,
                "ticker": ticker,
                "quantity": qty,
                "price": price,
                "type": "BUY",
            }).execute()
            log.info("[%s] entered %s x%d @ %.2f", pid, ticker, qty, price)

    remaining_cash = cash_pool - cash_spent
    sb.table("portfolios").update({
        "remaining_cash": remaining_cash,
        "last_rebalanced_at": today.isoformat(),
    }).eq("id", pid).execute()

    log.info(
        "[%s] rebalance complete: %d exited, %d entered, cash now %.2f",
        pid, len(to_exit), len(entries), remaining_cash,
    )


def rebalance_due_portfolios(force: bool = False) -> dict:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set.")

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    today = date.today()

    portfolios = (
        sb.table("portfolios")
        .select("id, remaining_cash, target_size, rebalance_frequency, last_rebalanced_at")
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
