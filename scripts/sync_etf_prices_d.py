"""
ETF Price Sync — universe.etfs.price / price_date
--------------------------------------------------
Stores today's close for every active ETF listed in universe.etfs.

Why this exists: universe.nse_universe is "all NSE common stocks" by design
(data_sync_engine_nse_all_d.py filters type=stock), so ETFs such as GOLDBEES
never get a price there. The web app's ST2 GO_CASH plan (100% GOLDBEES) needs
one, and reads it from universe.etfs. ETFs must NOT be written into
nse_universe — it would leak into sector breadth, movers, stage and RRG.

Prices come from TradingView daily bars via tvDatafeed (momentum_scanner's
fetch_history, the same helper portfolio_rebalance_d.py prices GOLDBEES with).
tradingview-screener can't be used: its "india" market has no fund rows at all,
not even for an explicit NSE:GOLDBEES ticker. price_date is the bar's own date,
so a weekend, holiday or late run never stamps a stale close with today.

Only the price, price_date and updated_at columns are written; ticker,
company_name and is_active are never touched, and no rows are inserted.

Usage:
    python scripts/sync_etf_prices_d.py                # fetch + update (default)
    python scripts/sync_etf_prices_d.py --preview-only # fetch + preview, no DB writes

Requirements:
    pip install supabase tvdatafeed pandas python-dotenv
"""

import os
import sys
import json
import logging
import argparse
import pathlib
import traceback
from datetime import datetime, timezone
from typing import Optional

from supabase import create_client
from dotenv import load_dotenv

load_dotenv(".env.local")

# Reuse the scanner's TvDatafeed helper for its rate limiting and retries.
try:
    from momentum_scanner import fetch_history
except ImportError as e:
    raise SystemExit(
        "Could not import from momentum_scanner. "
        "Ensure this script is run from the repo root or scripts/ directory."
    ) from e

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
SCHEMA       = "universe"
TABLE        = "etfs"
STOCK_TABLE  = "nse_universe"
GOLD_SYMBOL  = "GOLDBEES"
RESULT_FILE  = "results/etf_prices.json"
EXCHANGE     = "NSE"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────
def _safe_price(value) -> Optional[float]:
    """Return a positive rounded float, or None for NaN/None/<=0."""
    try:
        if value is None or value != value:   # NaN check
            return None
        v = round(float(value), 2)
        return v if v > 0 else None
    except Exception:
        return None


def _write_result(succeeded: int, failed: int, skipped: int, total: int, errors: list) -> None:
    """Write JSON result file consumed by the GitHub Actions summary step."""
    pathlib.Path("results").mkdir(exist_ok=True)
    pathlib.Path(RESULT_FILE).write_text(json.dumps({
        "script":    "ETF Price Sync",
        "succeeded": succeeded,
        "failed":    failed,
        "skipped":   skipped,
        "total":     total,
        "errors":    errors[:10],
    }))


# ── Step 1: Which ETFs ────────────────────────────────────────────────────────
def load_etf_tickers(sb) -> list:
    resp = (
        sb.schema(SCHEMA).table(TABLE)
        .select("ticker")
        .eq("is_active", True)
        .execute()
    )
    return sorted({r["ticker"] for r in (resp.data or []) if r.get("ticker")})


# ── Step 2: Fetch from TradingView ────────────────────────────────────────────
def fetch_etf_closes(tickers: list, errors: list) -> dict:
    """{ticker: (close, bar_date ISO)} from each ETF's latest NSE daily bar."""
    out = {}
    for t in tickers:
        try:
            df = fetch_history(t, EXCHANGE, n_bars=5)
        except Exception as exc:
            log.error("X TradingView fetch failed for %s: %s: %s", t, type(exc).__name__, exc)
            log.debug(traceback.format_exc())
            errors.append(f"{t}: TradingView fetch failed ({type(exc).__name__})")
            continue
        price = _safe_price(df["close"].iloc[-1])
        if price is not None:
            # tvDatafeed stamps bars in the machine's zone; on a UTC runner the
            # 09:15 IST open is 03:45 UTC, still the same calendar date.
            out[t] = (price, df.index[-1].date().isoformat())
    return out


# ── Step 3: Guard — no ETF rows may exist in the stock universe ──────────────
def etfs_in_stock_universe(sb, tickers: list) -> list:
    resp = (
        sb.schema(SCHEMA).table(STOCK_TABLE)
        .select("ticker")
        .in_("ticker", tickers)
        .execute()
    )
    return sorted(r["ticker"] for r in (resp.data or []))


# ── Orchestrator ──────────────────────────────────────────────────────────────
def run(do_update: bool) -> None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("X Missing env vars. Need NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY.")
        _write_result(0, 1, 0, 0, ["Missing env vars"])
        sys.exit(1)

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    errors = []

    tickers = load_etf_tickers(sb)
    if not tickers:
        log.error("X universe.etfs has no active tickers.")
        _write_result(0, 1, 0, 0, ["universe.etfs has no active tickers"])
        sys.exit(1)
    log.info("Active ETFs in universe.etfs: %d (%s)", len(tickers), ", ".join(tickers))

    closes = fetch_etf_closes(tickers, errors)
    run_date = datetime.now(timezone.utc).date().isoformat()
    now = datetime.now(timezone.utc).isoformat()
    missing = [t for t in tickers if t not in closes]

    print("\n" + "=" * 60)
    print("  ETF closes (latest NSE daily bar)" + ("" if do_update else "  (PREVIEW — no writes)"))
    print("=" * 60)
    for t in tickers:
        if t in closes:
            print(f"  {t:<15s} {closes[t][0]:>10.2f}   {closes[t][1]}")
        else:
            print(f"  {t:<15s} {'MISSING':>10s}")
    print("=" * 60 + "\n")

    for t in missing:
        if t == GOLD_SYMBOL:
            msg = (f"URGENT_ALERT: {GOLD_SYMBOL} missing price on {run_date}. "
                   "The ST2 GO_CASH plan cannot be built in the web app until this is fixed.")
            log.error(msg)
            errors.append(msg)
        elif not any(e.startswith(f"{t}:") for e in errors):
            errors.append(f"{t}: no TradingView close")

    succeeded = 0
    failed = len(missing)

    if do_update:
        for t, (price, price_date) in closes.items():
            try:
                (
                    sb.schema(SCHEMA).table(TABLE)
                    .update({"price": price, "price_date": price_date, "updated_at": now})
                    .eq("ticker", t)
                    .execute()
                )
                succeeded += 1
            except Exception as exc:
                failed += 1
                errors.append(f"{t}: update failed ({type(exc).__name__})")
                log.error("X Update failed for %s: %s: %s", t, type(exc).__name__, exc)
                if t == GOLD_SYMBOL:
                    errors.append(f"URGENT_ALERT: {GOLD_SYMBOL} price could not be written on {price_date}.")
        log.info("Updated %d / %d ETF prices.", succeeded, len(tickers))

    leaked = etfs_in_stock_universe(sb, tickers)
    if leaked:
        failed += 1
        errors.append(f"ETF rows found in {STOCK_TABLE}: {', '.join(leaked)}")
        log.error("X ETF rows found in %s.%s: %s", SCHEMA, STOCK_TABLE, leaked)
    else:
        log.info("Guard OK -- no ETF rows in %s.%s.", SCHEMA, STOCK_TABLE)

    if not do_update:
        return

    # URGENT_ALERT first so it survives the summary's error truncation.
    errors.sort(key=lambda e: "URGENT_ALERT" not in e)
    _write_result(succeeded, failed, 0, len(tickers), errors)
    if failed > 0:
        sys.exit(1)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync ETF closes into universe.etfs.")
    parser.add_argument("--preview-only", action="store_true",
                        help="Fetch and preview prices without writing to Supabase.")
    args = parser.parse_args()
    run(do_update=not args.preview_only)
