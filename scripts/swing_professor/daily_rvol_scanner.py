"""
daily_rvol_scanner.py
======================
Component H -- Daily RVOL confirmation scanner. Runs DAILY (not weekly),
scoped only to tickers already ACTIVE on swing_professor.watchlist (both the
checklist-passed Stage 2 bucket and the still-pending young-breakout bucket)
-- not the full NSE universe. Confirms which watchlist tickers are showing
same-day tradable volume/price action, per:

    Price floor        >= Rs.30
    Day change         >= +3%
    30-day avg volume  >= 200,000 shares (trailing, excluding today)
    RVOL (today's volume / 30-day avg volume) > 3.0

Qualifying hits are appended to swing_professor.daily_confirmations (see
sql/0041_swing_professor_daily_confirmations.sql) -- a daily log, not a
single latest-value column, so multiple hits across a base's life aren't
overwritten.

Usage:
    python daily_rvol_scanner.py                # full run (fetch + upsert)
    python daily_rvol_scanner.py --preview-only  # fetch + print, no DB writes

Environment variables (.env.local):
    NEXT_PUBLIC_SUPABASE_URL=https://xxxx.supabase.co
    SUPABASE_SERVICE_ROLE_KEY=eyJ...
"""

import argparse
import json
import logging
import pathlib
from datetime import date
from typing import List, Optional

from dotenv import load_dotenv

from momentum_scanner_tvdatafeed import get_daily_history_with_retry
from stage_analysis_pipeline_w import get_supabase_client
from supabase_watchlist_store import SCHEMA as WATCHLIST_SCHEMA, TABLE as WATCHLIST_TABLE

# momentum_scanner_tvdatafeed calls logging.basicConfig() at import time --
# force=True here wins regardless of import order (same pattern already used
# by weekly_watchlist_pipeline.py to avoid the 5-way collision documented in
# HANDOFF.md Sec.6 item 5).
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%H:%M:%S",
    force=True,
)
log = logging.getLogger("daily_rvol_scanner")

# ── Config constants (untuned starting-point defaults, per the user's spec) ──
PRICE_FLOOR: float = 30.0
DAY_CHANGE_MIN_PCT: float = 3.0
AVG_VOLUME_30D_MIN: int = 200_000
RVOL_MIN: float = 3.0
AVG_VOLUME_WINDOW_DAYS: int = 30

CONFIRMATIONS_SCHEMA = "swing_professor"
CONFIRMATIONS_TABLE = "daily_confirmations"


def fetch_active_watchlist_tickers(supabase) -> List[str]:
    """Every ticker currently ACTIVE on swing_professor.watchlist -- both the
    checklist-passed Stage 2 bucket and the still-pending young bucket. Not
    the full NSE universe, per the confirmed scope for this scanner."""
    resp = (
        supabase.schema(WATCHLIST_SCHEMA).table(WATCHLIST_TABLE)
        .select("ticker")
        .eq("status", "ACTIVE")
        .execute()
    )
    return [r["ticker"] for r in (resp.data or [])]


def evaluate_ticker_rvol(ticker: str) -> Optional[dict]:
    """Fetch daily bars and check the RVOL/day-change/volume/price criteria.
    Returns a confirmation dict if all pass, None if any fails or data is
    unavailable/insufficient."""
    df, error = get_daily_history_with_retry(ticker, n_bars=AVG_VOLUME_WINDOW_DAYS + 10)
    if df is None or df.empty:
        return None

    df = df.set_index("datetime").sort_index()
    if len(df) < AVG_VOLUME_WINDOW_DAYS + 1:
        return None

    latest = df.iloc[-1]
    prior = df.iloc[-2]

    price = float(latest["close"])
    if price < PRICE_FLOOR:
        return None

    prior_close = float(prior["close"])
    if prior_close == 0:
        return None
    day_change_pct = (price - prior_close) / prior_close * 100.0
    if day_change_pct < DAY_CHANGE_MIN_PCT:
        return None

    # Trailing 30-day average volume, excluding today -- avoids today's own
    # volume spike inflating the baseline it's being compared against.
    trailing_window = df["volume"].iloc[-1 - AVG_VOLUME_WINDOW_DAYS : -1]
    avg_volume_30d = float(trailing_window.mean())
    if avg_volume_30d < AVG_VOLUME_30D_MIN:
        return None

    volume = float(latest["volume"])
    rvol = volume / avg_volume_30d
    if rvol <= RVOL_MIN:
        return None

    return {
        "ticker": ticker,
        "price": round(price, 2),
        "day_change_pct": round(day_change_pct, 2),
        "volume": int(volume),
        "avg_volume_30d": round(avg_volume_30d, 2),
        "rvol": round(rvol, 3),
    }


def run_scanner(do_upsert: bool = True) -> List[dict]:
    load_dotenv(".env.local")
    supabase = get_supabase_client()
    today_str = date.today().isoformat()

    tickers = fetch_active_watchlist_tickers(supabase)
    log.info("Scanning %d ACTIVE watchlist ticker(s) for RVOL confirmation ...", len(tickers))

    hits: List[dict] = []
    errors: List[str] = []
    for ticker in tickers:
        try:
            hit = evaluate_ticker_rvol(ticker)
        except Exception as exc:
            log.error("  X %s: %s: %s", ticker, type(exc).__name__, exc)
            errors.append(ticker)
            continue
        if hit:
            hits.append(hit)
            log.info(
                "  MATCH %-12s price=%-9.2f day_chg=%+.2f%% rvol=%.2fx avg_vol_30d=%.0f",
                hit["ticker"], hit["price"], hit["day_change_pct"], hit["rvol"], hit["avg_volume_30d"],
            )

    log.info("Scan complete: %d/%d ticker(s) confirmed.", len(hits), len(tickers))

    if not do_upsert:
        log.info("Preview-only mode -- no DB writes.")
    elif hits:
        rows = [{**hit, "confirmation_date": today_str} for hit in hits]
        supabase.schema(CONFIRMATIONS_SCHEMA).table(CONFIRMATIONS_TABLE).upsert(
            rows, on_conflict="ticker,confirmation_date"
        ).execute()
        log.info(
            "Wrote %d confirmation row(s) to %s.%s for %s.",
            len(rows), CONFIRMATIONS_SCHEMA, CONFIRMATIONS_TABLE, today_str,
        )

    # Result file for the consolidated daily Telegram summary (same shape as
    # every other daily_sync step). Without it this step had no summary line,
    # so a silent no-op, a crash and a clean run all looked identical — and
    # per-ticker exceptions were only ever visible in the raw job log.
    pathlib.Path("results").mkdir(exist_ok=True)
    pathlib.Path("results/rvol.json").write_text(json.dumps({
        "script": "Swing Professor RVOL Scan",
        "succeeded": len(tickers) - len(errors),
        "failed": len(errors),
        "skipped": 0,
        "total": len(tickers),
        "errors": errors,
        "confirmed": len(hits),
    }))

    return hits


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily RVOL Confirmation Scanner (Swing Professor watchlist only)")
    parser.add_argument("--preview-only", action="store_true", help="Fetch + print, no DB writes")
    args = parser.parse_args()

    run_scanner(do_upsert=not args.preview_only)
