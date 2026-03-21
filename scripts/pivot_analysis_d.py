"""
pivot_analysis.py
=================
Uses tvDatafeed to fetch OHLC data and calculates:
  - Traditional Pivot Points & CPR  (Daily / Weekly / Monthly)
  - Pivot Trend  (BULLISH-MOMENTUM / BEARISH-MOMENTUM / BULLISH / BEARISH / NEUTRAL)
  - 2-Day CPR Relationship (Daily timeframe only)

Workflow:
  1. Fetch all tickers from Supabase `pivot_trend_analysis` table
  2. Skip any ticker where `last_updated` is already today (IST)
  3. Parallel fetch + compute via ThreadPoolExecutor (MAX_WORKERS=4)
  4. Upsert results in batches of UPSERT_BATCH
  5. Print summary + write results/pivot.json for GitHub Actions

Run context:
  EOD after NSE market close (3:30 PM IST). Script runs at 4:30 PM IST.
  The last bar from tvDatafeed IS today's completed candle — no bar is dropped.
  CPR levels are built from today's H/L/C → correct levels for TOMORROW's session.

Performance:
  Sequential (old): ~750 tickers x 3 fetches x 0.3s sleep = 11+ min
  Parallel (new):   ~750 tickers / 4 workers x 3 fetches x 0.3s = 3-4 min

Rate limiting:
  - MAX_WORKERS=4 avoids 429s on free TradingView accounts
  - threading.Semaphore caps simultaneous open WebSocket connections
  - 429 errors trigger exponential backoff (2s -> 4s -> 8s -> 16s)
  - Thread-local TvDatafeed instances (one per worker, no shared state)

Requirements:
    pip install tvDatafeed supabase python-dotenv pytz
"""

from __future__ import annotations

import json
import os
import pathlib
import sys
import time
import logging
import argparse
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, date
from dataclasses import dataclass, field
from typing import Optional, List, Tuple

import pytz
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
from tvDatafeed import TvDatafeed, Interval

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv(".env.local")

SUPABASE_URL: str = (
    os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    or os.environ.get("SUPABASE_URL", "")
)
SUPABASE_KEY: str = (
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    or os.environ.get("SUPABASE_KEY", "")
    or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY", "")
)

EXCHANGE       = "NSE"
IST            = pytz.timezone("Asia/Kolkata")
UPSERT_BATCH   = 50
N_DAILY_BARS   = 5
N_WEEKLY_BARS  = 6
N_MONTHLY_BARS = 6
SLEEP_BETWEEN  = 0.3    # seconds between each fetch within a worker

# ── Parallelism & rate limiting ───────────────────────────────────────────────
# 4 workers is safe for free TradingView accounts.
# Each ticker makes 3 fetches (D/W/M) so total connections stay well managed.
MAX_WORKERS: int    = 4
MAX_RETRIES: int    = 4       # 1 original + 3 retries
BACKOFF_BASE: float = 2.0     # 2s -> 4s -> 8s -> 16s
BACKOFF_MAX: float  = 60.0

# Semaphore caps concurrent open WebSocket connections.
_connection_semaphore: threading.Semaphore = threading.Semaphore(MAX_WORKERS)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------

@dataclass
class OHLCBar:
    high:  float
    low:   float
    close: float


@dataclass
class CPRLevels:
    pivot:     float
    bcpr:      float
    tcpr:      float
    r1:        float
    s1:        float
    max_pivot: float
    min_pivot: float
    max_high:  float
    max_low:   float


@dataclass
class TickerResult:
    ticker:                str
    close:                 float
    monthly_trend:         str = "UNKNOWN"
    weekly_trend:          str = "UNKNOWN"
    daily_trend:           str = "UNKNOWN"
    cpr_2day_relationship: str = "UNKNOWN"
    width_classification:  str = "UNKNOWN"
    pdh:                   Optional[float] = None
    pdl:                   Optional[float] = None
    last_updated:          str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


# ---------------------------------------------------------------------------
# CPR / Pivot calculations  (pure CPU — safe to call from any thread)
# ---------------------------------------------------------------------------

def compute_cpr(current: OHLCBar, previous: OHLCBar) -> CPRLevels:
    h, l, c = current.high, current.low, current.close
    pivot = (h + l + c) / 3
    bcpr  = (h + l) / 2
    tcpr  = (pivot - bcpr) + pivot
    r1    = (2 * pivot) - l
    s1    = (2 * pivot) - h
    return CPRLevels(
        pivot=pivot, bcpr=bcpr, tcpr=tcpr, r1=r1, s1=s1,
        max_pivot = max(pivot, bcpr, tcpr),
        min_pivot = min(pivot, bcpr, tcpr),
        max_high  = max(r1, previous.high),
        max_low   = min(s1, previous.low),
    )


def determine_trend(ltp: float, lvl: CPRLevels) -> str:
    if ltp > lvl.max_high:  return "BULLISH-MOMENTUM"
    if ltp < lvl.max_low:   return "BEARISH-MOMENTUM"
    if ltp > lvl.max_pivot: return "BULLISH"
    if ltp < lvl.min_pivot: return "BEARISH"
    return "NEUTRAL"


def determine_2day_cpr(today: CPRLevels, prev: CPRLevels) -> str:
    ct, cb = today.max_pivot, today.min_pivot
    pt, pb = prev.max_pivot,  prev.min_pivot
    if   ct > pt and cb > pb:              return "BULLISH_HV"
    elif ct < pt and cb < pb:              return "BEARISH_LV"
    elif ct > pt and cb <= pt and cb > pb: return "BULLISH_OHV"
    elif cb < pb and ct >= pb and ct < pt: return "BEARISH_OLV"
    elif ct > pt and cb < pb:              return "SIDEWAYS_OV"
    elif ct < pt and cb > pb:              return "BREAKOUT_IV"
    return "UNKNOWN"


def cpr_width_pct(lvl: CPRLevels) -> float:
    if lvl.pivot == 0:
        return 0.0
    return ((lvl.max_pivot - lvl.min_pivot) / lvl.pivot) * 100


def classify_cpr_width(width_pct: float) -> str:
    if width_pct <= 0.1:   return "Super Narrow"
    elif width_pct <= 0.25: return "Narrow"
    elif width_pct <= 0.5:  return "Medium"
    elif width_pct <= 1.5:  return "Wide"
    else:                   return "Very Wide"


# ---------------------------------------------------------------------------
# Per-thread TvDatafeed
# ---------------------------------------------------------------------------

_thread_local = threading.local()

def _get_thread_tv() -> TvDatafeed:
    """Return (or lazily create) a per-thread TvDatafeed instance."""
    if not hasattr(_thread_local, "tv"):
        _thread_local.tv = TvDatafeed()
    return _thread_local.tv


def _is_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "429" in msg or "too many requests" in msg


def _row_to_bar(row: pd.Series) -> OHLCBar:
    return OHLCBar(
        high=float(row["high"]),
        low=float(row["low"]),
        close=float(row["close"]),
    )


# ---------------------------------------------------------------------------
# Fetch one timeframe with retry + backoff
# ---------------------------------------------------------------------------

def fetch_bars_with_retry(
    symbol: str,
    interval: Interval,
    n_bars: int,
) -> Optional[pd.DataFrame]:
    """
    Fetch OHLC bars for one timeframe with exponential backoff on 429s.

    EOD context: last bar IS today's completed candle — no bar is dropped.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with _connection_semaphore:
                tv = _get_thread_tv()
                df = tv.get_hist(
                    symbol   = symbol,
                    exchange = EXCHANGE,
                    interval = interval,
                    n_bars   = n_bars,
                )
                time.sleep(SLEEP_BETWEEN)   # courtesy gap inside semaphore

            if df is None or df.empty or len(df) < 3:
                return None
            return df

        except Exception as exc:
            is_429 = _is_rate_limit_error(exc)

            if attempt < MAX_RETRIES:
                wait = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
                if is_429:
                    if hasattr(_thread_local, "tv"):
                        del _thread_local.tv
                    log.warning(
                        "429 on %s %s (attempt %d/%d) -- backing off %.0fs ...",
                        symbol, interval, attempt, MAX_RETRIES, wait,
                    )
                else:
                    log.warning(
                        "Transient error on %s %s (attempt %d/%d): %s -- retrying in %.0fs ...",
                        symbol, interval, attempt, MAX_RETRIES, type(exc).__name__, wait,
                    )
                time.sleep(wait)
            else:
                log.error(
                    "X Giving up on %s %s after %d attempts: %s",
                    symbol, interval, MAX_RETRIES, exc,
                )

    return None


# ---------------------------------------------------------------------------
# Worker function  (runs in thread pool)
# ---------------------------------------------------------------------------

def process_ticker(
    ticker: str,
    now_utc: str,
) -> Tuple[str, Optional[dict], Optional[str]]:
    """
    Fetch Daily + Weekly + Monthly bars, compute all CPR/trend values,
    return the upsert payload dict.

    Returns:
        (ticker, payload, error_message)
        payload is None on failure.

    Bar layout (EOD run, no bars dropped):
        df.iloc[-1]  -> TODAY's completed bar       (D0 — pivot source)
        df.iloc[-2]  -> yesterday's bar             (D-1 — boundary)
        df.iloc[-3]  -> two trading days ago        (D-2 — 2-day comparison)
    """
    # ── Fetch all three timeframes sequentially within this worker ────────────
    df_d = fetch_bars_with_retry(ticker, Interval.in_daily,   N_DAILY_BARS)
    df_w = fetch_bars_with_retry(ticker, Interval.in_weekly,  N_WEEKLY_BARS)
    df_m = fetch_bars_with_retry(ticker, Interval.in_monthly, N_MONTHLY_BARS)

    if df_d is None or len(df_d) < 3:
        return (ticker, None, f"Insufficient daily data ({len(df_d) if df_d is not None else 0} bars)")

    # ── Daily ─────────────────────────────────────────────────────────────────
    # bar_d0 = today's completed bar    → LTP source, pdh/pdl, tomorrow's pivot source
    # bar_d1 = yesterday's completed    → TODAY's pivot source (compare LTP against this)
    # bar_d2 = D-2 completed            → boundary for today's CPR, yesterday's pivot source
    bar_d0 = _row_to_bar(df_d.iloc[-1])
    bar_d1 = _row_to_bar(df_d.iloc[-2])
    bar_d2 = _row_to_bar(df_d.iloc[-3]) if len(df_d) >= 3 else bar_d1

    ltp = bar_d0.close   # today's closing price

    # Tomorrow's CPR — built from today's H/L/C → used for width + 2-day relationship
    lvl_tomorrow = compute_cpr(bar_d0, bar_d1)

    # Today's CPR — built from yesterday's H/L/C → LTP is evaluated against THIS.
    # Pivot Point convention: today's price vs today's PP levels (derived from yesterday).
    # Comparing LTP against a CPR built from the SAME bar always produces NEUTRAL/BEARISH
    # because close is geometrically inside its own H/L/C-derived CPR band.
    lvl_today = compute_cpr(bar_d1, bar_d2)

    daily_trend           = determine_trend(ltp, lvl_today)            # LTP vs today's levels
    width_classification  = classify_cpr_width(cpr_width_pct(lvl_tomorrow))  # tomorrow's width
    cpr_2day_relationship = determine_2day_cpr(lvl_tomorrow, lvl_today)      # tomorrow vs today

    # ── Weekly ────────────────────────────────────────────────────────────────
    # Use the previous completed week as pivot source (iloc[-2]) so LTP can
    # genuinely be above/below that week's levels. The current week (iloc[-1])
    # is still forming — comparing LTP against it causes the same inside-band issue.
    weekly_trend = "UNKNOWN"
    if df_w is not None and len(df_w) >= 3:
        lvl_w = compute_cpr(_row_to_bar(df_w.iloc[-2]), _row_to_bar(df_w.iloc[-3]))
        weekly_trend = determine_trend(ltp, lvl_w)
    elif df_w is not None and len(df_w) >= 2:
        lvl_w = compute_cpr(_row_to_bar(df_w.iloc[-1]), _row_to_bar(df_w.iloc[-2]))
        weekly_trend = determine_trend(ltp, lvl_w)

    # ── Monthly ───────────────────────────────────────────────────────────────
    # Same logic: previous completed month as pivot source.
    monthly_trend = "UNKNOWN"
    if df_m is not None and len(df_m) >= 3:
        lvl_m = compute_cpr(_row_to_bar(df_m.iloc[-2]), _row_to_bar(df_m.iloc[-3]))
        monthly_trend = determine_trend(ltp, lvl_m)
    elif df_m is not None and len(df_m) >= 2:
        lvl_m = compute_cpr(_row_to_bar(df_m.iloc[-1]), _row_to_bar(df_m.iloc[-2]))
        monthly_trend = determine_trend(ltp, lvl_m)

    payload = {
        "ticker":                ticker,
        "close":                 round(ltp, 2),
        "monthly_trend":         monthly_trend,
        "weekly_trend":          weekly_trend,
        "daily_trend":           daily_trend,
        "cpr_2day_relationship": cpr_2day_relationship,
        "width_classification":  width_classification,
        "pdh":                   round(bar_d0.high, 2),   # today's high = tomorrow's PDH
        "pdl":                   round(bar_d0.low,  2),   # today's low  = tomorrow's PDL
        "last_updated":          now_utc,
    }

    return (ticker, payload, None)


# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

def fetch_tickers(client: Client) -> List[dict]:
    """Return all rows from pivot_trend_analysis."""
    response = client.table("pivot_trend_analysis").select("ticker, last_updated").execute()
    return response.data or []


def today_ist() -> date:
    return datetime.now(IST).date()


def already_updated_today(last_updated_str: Optional[str]) -> bool:
    """Return True if last_updated is today in IST."""
    if not last_updated_str:
        return False
    try:
        dt = datetime.fromisoformat(last_updated_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(IST).date() == today_ist()
    except Exception:
        return False


def flush_batch(client: Client, rows: List[dict]) -> None:
    """Upsert a batch of rows in a single DB call."""
    if not rows:
        return
    try:
        client.table("pivot_trend_analysis").upsert(
            rows, on_conflict="ticker"
        ).execute()
        log.info("  ^ Flushed %d records to Supabase.", len(rows))
    except Exception as exc:
        log.error(
            "X Batch upsert failed (%d rows): %s: %s",
            len(rows), type(exc).__name__, exc,
        )
        log.debug(traceback.format_exc())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("=" * 62)
    log.info(
        "  Pivot & CPR Analysis  --  %s  (workers=%d)",
        datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"), MAX_WORKERS,
    )
    log.info("=" * 62)

    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("X Missing env vars. Need NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY.")
        sys.exit(1)

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    log.info("Supabase connected.")

    # ── Fetch + skip check ────────────────────────────────────────────────────
    all_rows = fetch_tickers(supabase)
    log.info("Total tickers in DB : %d", len(all_rows))

    to_process: List[str] = []
    skipped:    List[str] = []
    for row in all_rows:
        ticker = row["ticker"]
        if already_updated_today(row.get("last_updated")):
            skipped.append(ticker)
        else:
            to_process.append(ticker)

    log.info("Already up-to-date  : %d  (skipping)", len(skipped))
    log.info("To process          : %d", len(to_process))

    if not to_process:
        log.info("Nothing to do -- all tickers already updated today.")
        _write_result(0, 0, len(skipped), len(all_rows), [])
        return

    now_utc   = datetime.now(timezone.utc).isoformat()
    total     = len(to_process)
    succeeded: List[str]             = []
    failed:    List[Tuple[str, str]] = []
    pending_records: List[dict]      = []

    # ── Parallel fetch + compute ──────────────────────────────────────────────
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_ticker, ticker, now_utc): ticker
            for ticker in to_process
        }

        completed = 0
        for future in as_completed(futures):
            ticker = futures[future]
            completed += 1

            try:
                result_ticker, payload, error = future.result()

                if error:
                    log.warning(
                        "[%d/%d] X %s -- %s", completed, total, ticker, error
                    )
                    failed.append((ticker, error))
                    continue

                log.info(
                    "[%d/%d] %s  LTP=%-9.2f  M=%-18s  W=%-18s  D=%-18s  CPR2=%-14s  WIDTH=%s",
                    completed, total, ticker,
                    payload["close"],
                    payload["monthly_trend"], payload["weekly_trend"],
                    payload["daily_trend"],   payload["cpr_2day_relationship"],
                    payload["width_classification"],
                )
                succeeded.append(ticker)
                pending_records.append(payload)

                if len(pending_records) >= UPSERT_BATCH:
                    flush_batch(supabase, pending_records)
                    pending_records.clear()

            except Exception as exc:
                log.error(
                    "[%d/%d] X Unhandled error for %s: %s: %s",
                    completed, total, ticker, type(exc).__name__, exc,
                )
                log.debug(traceback.format_exc())
                failed.append((ticker, str(exc)))

    # ── Final flush ───────────────────────────────────────────────────────────
    if pending_records:
        flush_batch(supabase, pending_records)
        pending_records.clear()

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info("")
    log.info("=" * 62)
    log.info("  SUMMARY")
    log.info("=" * 62)
    log.info("  Total in DB         : %d", len(all_rows))
    log.info("  Skipped (up-to-date): %d", len(skipped))
    log.info("  Processed           : %d", total)
    log.info("  Succeeded           : %d", len(succeeded))
    log.info("  Failed              : %d", len(failed))
    log.info("  Workers             : %d", MAX_WORKERS)
    if failed:
        log.info("")
        log.info("  Failed tickers:")
        for ticker, reason in failed[:20]:
            log.info("    x  %-15s  %s", ticker, reason)
        if len(failed) > 20:
            log.info("    ... and %d more.", len(failed) - 20)
    log.info("=" * 62)

    # ── Write result JSON for GitHub Actions summary ──────────────────────────
    _write_result(len(succeeded), len(failed), len(skipped), len(all_rows),
                  [t for t, _ in failed[:10]])


def _write_result(succeeded: int, failed: int, skipped: int,
                  total: int, errors: List[str]) -> None:
    pathlib.Path("results").mkdir(exist_ok=True)
    pathlib.Path("results/pivot.json").write_text(json.dumps({
        "script":    "Pivot Analysis",
        "succeeded": succeeded,
        "failed":    failed,
        "skipped":   skipped,
        "total":     total,
        "errors":    errors,
    }))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pivot & CPR Analysis -- Daily Pipeline."
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS,
        help=f"Number of parallel fetch workers (default: {MAX_WORKERS}). "
             f"Lower if seeing 429 errors.",
    )
    args = parser.parse_args()

    MAX_WORKERS           = args.workers
    _connection_semaphore = threading.Semaphore(MAX_WORKERS)

    main()
