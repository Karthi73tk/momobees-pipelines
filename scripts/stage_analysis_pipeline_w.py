"""
stage_analysis_pipeline.py
==========================
Stan Weinstein Stage Analysis -- Weekly Data Pipeline
Fetches OHLCV data via tvdatafeed, computes stages, upserts to Supabase.

Usage:
    python stage_analysis_pipeline.py                # full run (fetch + upsert)
    python stage_analysis_pipeline.py --preview-only # fetch + print, no DB writes
    python stage_analysis_pipeline.py --workers 3    # override worker count

Environment variables (.env.local):
    NEXT_PUBLIC_SUPABASE_URL=https://xxxx.supabase.co
    SUPABASE_SERVICE_ROLE_KEY=eyJ...

Performance:
    Parallel fetch+compute via ThreadPoolExecutor (MAX_WORKERS=4).
    Each worker fetches one ticker's OHLCV and computes the stage independently.
    Results are collected via as_completed() and flushed to Supabase in batches.

    Sequential (old):  ~1,800 tickers x (0.5s delay + 0.8s fetch) = 23+ min
    Parallel (new):    ~1,800 tickers / 4 workers x 0.8s            =  6-8 min

Rate limiting:
    - MAX_WORKERS=4 avoids 429s on free TradingView accounts
    - threading.Semaphore caps simultaneous open WebSocket connections
    - 429 errors trigger exponential backoff (2s -> 4s -> 8s -> 16s)
    - Each worker's TvDatafeed instance is recreated after a 429

Weekly skip logic:
    A ticker is skipped if its row in weekly_stock_stages already has an
    updated_at timestamp from TODAY (UTC). Re-running the same calendar day
    produces identical results -- safe to skip.

Requirements:
    pip install tvdatafeed supabase pandas numpy python-dotenv
"""

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
from datetime import date, datetime, timezone
from dataclasses import dataclass, field
from typing import Optional, List, Set, Tuple

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
from tvDatafeed import TvDatafeed, Interval

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv(".env.local")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_URL: str = (
    os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    or os.environ.get("SUPABASE_URL", "")
)
SUPABASE_KEY: str = (
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    or os.environ.get("SUPABASE_KEY", "")
    or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY", "")
)

UNIVERSE_TABLE: str = "stock_universe_nse_all"
STAGES_TABLE: str   = "weekly_stock_stages"

# ── Exchange / market ─────────────────────────────────────────────────────────
DEFAULT_EXCHANGE: str = os.getenv("STOCK_EXCHANGE", "NSE")

# ── Stage parameters ──────────────────────────────────────────────────────────
SMA_PERIOD: int       = 30      # Weinstein's 30-week SMA
SLOPE_LOOKBACK: int   = 4       # Weeks back for SMA slope calculation
HIGH_LOW_PERIOD: int  = 52      # 52-week high / low
FLAT_THRESHOLD: float = 0.015   # 1.5% -- SMA slope considered "flat"
N_BARS: int           = HIGH_LOW_PERIOD + SMA_PERIOD + SLOPE_LOOKBACK + 20  # ~106

# ── Parallelism & rate limiting ───────────────────────────────────────────────
# 4 workers is safe for free TradingView accounts.
# Raise to 6-8 only if you have a Pro/Pro+ account.
MAX_WORKERS: int     = 4
REQUEST_DELAY: float = 0.5    # Base courtesy gap per worker request (seconds)

# Retry config for 429 / transient errors
MAX_RETRIES: int    = 4       # Total attempts per ticker (1 original + 3 retries)
BACKOFF_BASE: float = 2.0     # Exponential backoff: 2s, 4s, 8s, 16s
BACKOFF_MAX: float  = 60.0

# ── Batch upsert ──────────────────────────────────────────────────────────────
# Stage produces 1 row per ticker, so this is tickers-per-flush.
UPSERT_BATCH_SIZE: int = 50

# Semaphore caps concurrent open WebSocket connections.
_connection_semaphore: threading.Semaphore = threading.Semaphore(MAX_WORKERS)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class StageRecord:
    ticker: str
    analysis_date: str
    close: float
    volume: int
    sma_30: float
    sma_slope: float
    week_52_high: float
    week_52_low: float
    week_52_midpoint: float
    stage: int
    stage_label: str


@dataclass
class PipelineSummary:
    total: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    workers: int = 0
    elapsed: float = 0.0
    failed_tickers: list = field(default_factory=list)
    skipped_tickers: list = field(default_factory=list)

    def report(self) -> str:
        lines = [
            "",
            "=" * 62,
            "  STAGE ANALYSIS PIPELINE -- RUN SUMMARY",
            "=" * 62,
            f"  Total tickers        : {self.total}",
            f"  Succeeded            : {self.succeeded}",
            f"  Skipped (up-to-date) : {self.skipped}",
            f"  Failed               : {self.failed}",
            f"  Workers              : {self.workers}",
            f"  Max retries/ticker   : {MAX_RETRIES}",
            f"  Elapsed time         : {self.elapsed:.1f}s",
            "=" * 62,
        ]
        if self.failed_tickers:
            lines.append(f"  FAILED tickers ({len(self.failed_tickers)}):")
            for t in self.failed_tickers[:20]:
                lines.append(f"    x {t}")
            if len(self.failed_tickers) > 20:
                lines.append(f"    ... and {len(self.failed_tickers) - 20} more.")
        if self.skipped_tickers:
            lines.append("  SKIPPED tickers (first 10 shown):")
            for t in self.skipped_tickers[:10]:
                lines.append(f"    - {t}")
            if len(self.skipped_tickers) > 10:
                lines.append(f"    ... and {len(self.skipped_tickers) - 10} more.")
        lines.append("=" * 62)
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Stage computation  (pure CPU -- no I/O, safe to call from any thread)
# ---------------------------------------------------------------------------

STAGE_LABELS = {
    1: "Stage 1 - Basing",
    2: "Stage 2 - Advancing",
    3: "Stage 3 - Topping",
    4: "Stage 4 - Declining",
}


def compute_stage(df: pd.DataFrame) -> Optional[StageRecord]:
    """
    Given a DataFrame of weekly OHLCV bars (oldest to newest),
    compute the Weinstein stage for the most recent completed week.
    Returns a StageRecord, or None if there is insufficient data.
    """
    min_rows = SMA_PERIOD + SLOPE_LOOKBACK
    if len(df) < min_rows:
        return None

    df = df.copy().sort_index()
    df["sma_30"]    = df["close"].rolling(window=SMA_PERIOD, min_periods=SMA_PERIOD).mean()
    df["sma_slope"] = (
        (df["sma_30"] - df["sma_30"].shift(SLOPE_LOOKBACK))
        / df["sma_30"].shift(SLOPE_LOOKBACK)
    )
    df["high_52w"] = df["high"].rolling(window=HIGH_LOW_PERIOD, min_periods=HIGH_LOW_PERIOD).max()
    df["low_52w"]  = df["low"].rolling(window=HIGH_LOW_PERIOD, min_periods=HIGH_LOW_PERIOD).min()
    df["mid_52w"]  = (df["high_52w"] + df["low_52w"]) / 2

    latest = df.dropna(subset=["sma_30", "sma_slope", "high_52w", "low_52w"]).iloc[-1]

    close    = float(latest["close"])
    sma_30   = float(latest["sma_30"])
    slope    = float(latest["sma_slope"])
    high_52w = float(latest["high_52w"])
    low_52w  = float(latest["low_52w"])
    mid_52w  = float(latest["mid_52w"])
    volume   = int(latest["volume"]) if not np.isnan(latest["volume"]) else 0

    bar_date = latest.name
    analysis_date = (
        bar_date.date().isoformat()
        if isinstance(bar_date, pd.Timestamp)
        else str(bar_date)[:10]
    )

    if slope > FLAT_THRESHOLD and close > sma_30:
        stage = 2
    elif slope < -FLAT_THRESHOLD and close < sma_30:
        stage = 4
    elif abs(slope) <= FLAT_THRESHOLD and close < mid_52w:
        stage = 1
    elif abs(slope) <= FLAT_THRESHOLD and close >= mid_52w:
        stage = 3
    else:
        stage = 1   # edge case fallback

    return StageRecord(
        ticker           = "",
        analysis_date    = analysis_date,
        close            = round(close,    4),
        volume           = volume,
        sma_30           = round(sma_30,   4),
        sma_slope        = round(slope,    6),
        week_52_high     = round(high_52w, 4),
        week_52_low      = round(low_52w,  4),
        week_52_midpoint = round(mid_52w,  4),
        stage            = stage,
        stage_label      = STAGE_LABELS[stage],
    )


# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

def get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error(
            "X Missing env vars. Need NEXT_PUBLIC_SUPABASE_URL "
            "and SUPABASE_SERVICE_ROLE_KEY in .env.local"
        )
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_all_tickers(supabase: Client) -> List[dict]:
    """Returns all active rows from stock_universe_nse_all, paginated."""
    all_rows, page, page_size = [], 0, 1000
    while True:
        resp = (
            supabase.table(UNIVERSE_TABLE)
            .select("ticker, sector, industry, company_name")
            .eq("is_active", True)
            .range(page * page_size, (page + 1) * page_size - 1)
            .execute()
        )
        rows = resp.data or []
        all_rows.extend(rows)
        if len(rows) < page_size:
            break
        page += 1
    log.info("Fetched %d active tickers from %s.", len(all_rows), UNIVERSE_TABLE)
    return all_rows


def get_already_processed_today(supabase: Client, today_str: str) -> Set[str]:
    """
    Returns tickers whose updated_at falls on today (UTC) in weekly_stock_stages.
    Uses updated_at (not analysis_date) -- analysis_date only changes weekly,
    so it can't tell us if we already ran today.
    """
    today_start = f"{today_str}T00:00:00+00:00"
    today_end   = f"{today_str}T23:59:59+00:00"

    processed: Set[str] = set()
    page, page_size = 0, 1000

    while True:
        resp = (
            supabase.table(STAGES_TABLE)
            .select("ticker")
            .gte("updated_at", today_start)
            .lte("updated_at", today_end)
            .range(page * page_size, (page + 1) * page_size - 1)
            .execute()
        )
        rows = resp.data or []
        for row in rows:
            processed.add(row["ticker"])
        if len(rows) < page_size:
            break
        page += 1

    if processed:
        log.info(
            "Already processed today (%s): %d tickers will be skipped.",
            today_str, len(processed),
        )
    else:
        log.info("No tickers processed today (%s) -- full run will proceed.", today_str)
    return processed


def flush_batch(supabase: Client, records: List[dict]) -> None:
    """Upsert a batch of stage records in a single DB call."""
    if not records:
        return
    try:
        supabase.table(STAGES_TABLE).upsert(
            records, on_conflict="ticker,analysis_date"
        ).execute()
        log.info("  ^ Flushed %d records to Supabase.", len(records))
    except Exception as exc:
        log.error(
            "X Upsert batch failed (%d records): %s: %s",
            len(records), type(exc).__name__, exc,
        )
        log.debug(traceback.format_exc())


# ---------------------------------------------------------------------------
# Per-thread TvDatafeed  (each worker gets its own instance)
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


# ---------------------------------------------------------------------------
# Worker function  (runs in thread pool)
# ---------------------------------------------------------------------------

def process_ticker(
    ticker: str,
    now_utc: str,
) -> Tuple[str, Optional[dict], Optional[str]]:
    """
    Fetch OHLCV, compute stage, build payload dict for one ticker.

    Returns:
        (ticker, payload, error_tag)
        - payload is None on failure
        - error_tag is None on success, "no_data" or "compute_error" on failure
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with _connection_semaphore:          # cap concurrent WebSocket connections
                time.sleep(REQUEST_DELAY)
                tv     = _get_thread_tv()
                raw_df = tv.get_hist(
                    symbol   = ticker,
                    exchange = DEFAULT_EXCHANGE,
                    interval = Interval.in_weekly,
                    n_bars   = N_BARS,
                )

            if raw_df is None or raw_df.empty:
                return (ticker, None, "no_data")

            raw_df.columns = [c.lower() for c in raw_df.columns]
            break   # fetch succeeded, exit retry loop

        except Exception as exc:
            is_429 = _is_rate_limit_error(exc)

            if attempt < MAX_RETRIES:
                wait = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
                if is_429:
                    # Discard poisoned connection, force new instance on retry
                    if hasattr(_thread_local, "tv"):
                        del _thread_local.tv
                    log.warning(
                        "429 on %s (attempt %d/%d) -- backing off %.0fs ...",
                        ticker, attempt, MAX_RETRIES, wait,
                    )
                else:
                    log.warning(
                        "Transient error on %s (attempt %d/%d): %s -- retrying in %.0fs ...",
                        ticker, attempt, MAX_RETRIES, type(exc).__name__, wait,
                    )
                time.sleep(wait)
            else:
                log.error(
                    "X Giving up on %s after %d attempts. Last error: %s: %s",
                    ticker, MAX_RETRIES, type(exc).__name__, exc,
                )
                return (ticker, None, "no_data")
    else:
        return (ticker, None, "no_data")

    # ── Compute stage (pure CPU, no I/O) ─────────────────────────────────────
    try:
        record = compute_stage(raw_df)
        if record is None:
            return (ticker, None, "compute_error")
        record.ticker = ticker
    except Exception as exc:
        log.error(
            "X Stage computation failed for %s: %s: %s",
            ticker, type(exc).__name__, exc,
        )
        return (ticker, None, "compute_error")

    payload = {
        "ticker"           : record.ticker,
        "analysis_date"    : record.analysis_date,
        "close"            : record.close,
        "volume"           : record.volume,
        "sma_30"           : record.sma_30,
        "sma_slope"        : record.sma_slope,
        "week_52_high"     : record.week_52_high,
        "week_52_low"      : record.week_52_low,
        "week_52_midpoint" : record.week_52_midpoint,
        "stage"            : record.stage,
        "stage_label"      : record.stage_label,
        "updated_at"       : now_utc,
    }

    return (ticker, payload, None)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(do_upsert: bool) -> PipelineSummary:
    summary    = PipelineSummary(workers=MAX_WORKERS)
    today_str  = date.today().isoformat()
    now_utc    = datetime.now(timezone.utc).isoformat()
    start_time = time.time()

    log.info(
        "=== Stage Analysis Pipeline starting (mode: %s, workers: %d, max_retries: %d) ===",
        "UPSERT" if do_upsert else "PREVIEW ONLY", MAX_WORKERS, MAX_RETRIES,
    )

    supabase = get_supabase_client()
    universe = fetch_all_tickers(supabase)
    summary.total = len(universe)

    # ── Skip check ────────────────────────────────────────────────────────────
    already_done: Set[str] = set()
    if do_upsert:
        already_done = get_already_processed_today(supabase, today_str)

    pending    = [r for r in universe if r["ticker"] not in already_done]
    skipped    = [r for r in universe if r["ticker"] in already_done]
    summary.skipped          = len(skipped)
    summary.skipped_tickers  = [r["ticker"] for r in skipped]
    total                    = len(pending)

    log.info(
        "Processing %d tickers (%d skipped, %d workers) ...",
        total, summary.skipped, MAX_WORKERS,
    )

    pending_records: List[dict] = []

    # ── Parallel fetch + compute ──────────────────────────────────────────────
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_ticker, row["ticker"], now_utc): row["ticker"]
            for row in pending
        }

        completed = 0
        for future in as_completed(futures):
            ticker = futures[future]
            completed += 1

            try:
                result_ticker, payload, error = future.result()

                if error == "no_data":
                    log.warning(
                        "[%d/%d] %s -- no data (exhausted retries).",
                        completed, total, ticker,
                    )
                    summary.failed += 1
                    summary.failed_tickers.append(ticker)
                    continue

                if error == "compute_error":
                    log.warning(
                        "[%d/%d] %s -- stage computation failed.",
                        completed, total, ticker,
                    )
                    summary.failed += 1
                    summary.failed_tickers.append(ticker)
                    continue

                log.info(
                    "[%d/%d] %s -> %s | Close: %.2f | SMA30: %.2f | Slope: %.4f",
                    completed, total, ticker,
                    payload["stage_label"], payload["close"],
                    payload["sma_30"], payload["sma_slope"],
                )
                summary.succeeded += 1

                if do_upsert:
                    pending_records.append(payload)
                    if len(pending_records) >= UPSERT_BATCH_SIZE:
                        flush_batch(supabase, pending_records)
                        pending_records.clear()

            except Exception as exc:
                log.error(
                    "[%d/%d] X Unhandled error for %s: %s: %s",
                    completed, total, ticker, type(exc).__name__, exc,
                )
                log.debug(traceback.format_exc())
                summary.failed += 1
                summary.failed_tickers.append(ticker)

    # ── Final flush ───────────────────────────────────────────────────────────
    if do_upsert and pending_records:
        flush_batch(supabase, pending_records)
        pending_records.clear()

    summary.elapsed = time.time() - start_time

    # ── Write result JSON for GitHub Actions summary ──────────────────────────
    pathlib.Path("results").mkdir(exist_ok=True)
    pathlib.Path("results/stage.json").write_text(json.dumps({
        "script":    "Stage Analysis (Weekly)",
        "succeeded": summary.succeeded,
        "failed":    summary.failed,
        "skipped":   summary.skipped,
        "total":     summary.total,
        "errors":    summary.failed_tickers[:10],
    }))

    return summary


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Stan Weinstein Stage Analysis -- Weekly Pipeline."
    )
    parser.add_argument(
        "--preview-only",
        action="store_true",
        help="Fetch and compute stages but do NOT write to Supabase.",
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

    summary = run_pipeline(do_upsert=not args.preview_only)
    print(summary.report())
