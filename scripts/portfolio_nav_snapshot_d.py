#!/usr/bin/env python3
"""
Daily NAV snapshot: for every active (virtual/real) portfolio, revalue open
holdings at today's TVDatafeed close, write current_price back onto each
holding, and record one nav_history point for today (updating it in place if
this job has already run today, so retries/backfills never create duplicate
points on the same day).
"""
import os
import sys
import pathlib
import json
import logging
import argparse
from datetime import datetime, timezone, timedelta, date
from typing import Optional

# Ensure scripts directory is in python search path so we can import momentum_scanner
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from supabase import create_client, Client
from dotenv import load_dotenv

# Try to load local env file for local testing
try:
    load_dotenv(".env.local")
except ImportError:
    pass

from typing import Tuple
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
log = logging.getLogger("portfolio_nav")

# --- Configuration ---
SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

BENCHMARK_TICKER = os.environ.get("BENCHMARK_TICKER", "NIFTY")
BENCHMARK_EXCHANGE = os.environ.get("BENCHMARK_EXCHANGE", "NSE")
STOCK_EXCHANGE = os.environ.get("STOCK_EXCHANGE", "NSE")

ACTIVE_STATUSES = ["virtual", "real"]


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


def fetch_benchmark_price() -> Optional[float]:
    try:
        df = fetch_history(BENCHMARK_TICKER, BENCHMARK_EXCHANGE, n_bars=5)
        if df is not None and not df.empty:
            return float(df["close"].iloc[-1])
    except Exception as exc:
        log.error("Failed to fetch benchmark %s:%s: %s", BENCHMARK_EXCHANGE, BENCHMARK_TICKER, exc)
    return None


def _today_bounds_utc():
    now = datetime.now(timezone.utc)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, start + timedelta(days=1)


def _existing_nav_row_today(sb, portfolio_id: str):
    start, end = _today_bounds_utc()
    resp = (
        sb.table("nav_history")
        .select("id")
        .eq("portfolio_id", portfolio_id)
        .gte("recorded_at", start.isoformat())
        .lt("recorded_at", end.isoformat())
        .limit(1)
        .execute()
    )
    return resp.data[0]["id"] if resp.data else None


def _last_known_benchmark(sb):
    """benchmark_nav is NOT NULL in the schema, so if today's live fetch fails we
    fall back to the most recently recorded value (still real market data, just
    stale by a day or two) rather than a hardcoded number."""
    resp = (
        sb.table("nav_history")
        .select("benchmark_nav")
        .order("recorded_at", desc=True)
        .limit(1)
        .execute()
    )
    return float(resp.data[0]["benchmark_nav"]) if resp.data else None


def snapshot_all_portfolios() -> dict:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set.")

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    portfolios = (
        sb.table("portfolios")
        .select("id, remaining_cash")
        .in_("status", ACTIVE_STATUSES)
        .execute()
        .data
    )
    if not portfolios:
        log.info("No active portfolios to snapshot.")
        return {"succeeded": 0, "failed": 0, "skipped": 0, "total": 0, "errors": []}

    portfolio_ids = [p["id"] for p in portfolios]
    holdings = (
        sb.table("holdings")
        .select("id, portfolio_id, ticker, quantity, entry_price")
        .in_("portfolio_id", portfolio_ids)
        .eq("status", "open")
        .execute()
        .data
    )

    all_tickers = {h["ticker"] for h in holdings}
    prices = fetch_ticker_prices(all_tickers)
    
    nifty_close = fetch_benchmark_price()
    if nifty_close is None:
        nifty_close = _last_known_benchmark(sb)
        log.error("Live NIFTY fetch failed — falling back to last recorded benchmark_nav (%s).", nifty_close)
    if nifty_close is None:
        raise RuntimeError(
            "No live NIFTY price and no prior nav_history row to fall back to. "
            "Refusing to write nav_history rows with a fabricated benchmark value."
        )

    holdings_by_portfolio = {}
    for h in holdings:
        holdings_by_portfolio.setdefault(h["portfolio_id"], []).append(h)

    succeeded_count = 0
    failed_count = 0
    errors = []

    for portfolio in portfolios:
        pid = portfolio["id"]
        try:
            p_holdings = holdings_by_portfolio.get(pid, [])

            holdings_value = 0.0
            for h in p_holdings:
                price = prices.get(h["ticker"])
                if price is None:
                    # Fall back to entry price rather than dropping the position from the valuation entirely
                    price = float(h["entry_price"])
                    log.warning("Using entry_price fallback for %s in portfolio %s", h["ticker"], pid)
                else:
                    sb.table("holdings").update({"current_price": price}).eq("id", h["id"]).execute()
                holdings_value += float(h["quantity"]) * price

            total_nav = holdings_value + float(portfolio["remaining_cash"])

            payload = {
                "portfolio_id": pid,
                "nav": total_nav,
                "benchmark_nav": nifty_close,
            }

            existing_id = _existing_nav_row_today(sb, pid)
            if existing_id:
                sb.table("nav_history").update(payload).eq("id", existing_id).execute()
                log.info("Updated today's NAV for portfolio %s: %.2f", pid, total_nav)
            else:
                sb.table("nav_history").insert(payload).execute()
                log.info("Recorded NAV for portfolio %s: %.2f", pid, total_nav)

            succeeded_count += 1
        except Exception as exc:
            log.exception("Failed to snapshot NAV for portfolio %s", pid)
            failed_count += 1
            errors.append(f"{pid}: {str(exc)}")

    return {
        "succeeded": succeeded_count,
        "failed": failed_count,
        "skipped": 0,
        "total": len(portfolios),
        "errors": errors
    }


def _write_result(succeeded: int, failed: int, skipped: int, total: int, errors: list) -> None:
    pathlib.Path("results").mkdir(exist_ok=True)
    pathlib.Path("results/portfolio_nav.json").write_text(json.dumps({
        "script":    "Portfolio NAV Snapshot",
        "succeeded": succeeded,
        "failed":    failed,
        "skipped":   skipped,
        "total":     total,
        "errors":    errors,
    }))


def main() -> int:
    parser = argparse.ArgumentParser(description="Take daily NAV snapshot for active portfolios")
    args = parser.parse_args()

    try:
        res = snapshot_all_portfolios()
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
        log.exception("NAV snapshot run failed.")
        _write_result(succeeded=0, failed=1, skipped=0, total=0, errors=[str(exc)])
        return 1


if __name__ == "__main__":
    sys.exit(main())
