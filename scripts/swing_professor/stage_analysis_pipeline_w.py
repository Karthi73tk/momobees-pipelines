"""
stage_analysis_pipeline.py
==========================
Stan Weinstein Stage Analysis -- Weekly Data Pipeline
Fetches OHLCV data via tvdatafeed, computes stages, upserts to Supabase.

Usage:
    python stage_analysis_pipeline.py                # full run (fetch + upsert)
    python stage_analysis_pipeline.py --preview-only # fetch + print, no DB writes
    python stage_analysis_pipeline.py --workers 3    # override worker count

    # Stage 1 -> Stage 2 "young breakout" screener (adds a daily-volume fetch
    # per ticker on top of the normal weekly fetch, and does NOT change what
    # gets upserted to weekly_stock_stages):
    python stage_analysis_pipeline.py --screen --preview-only
    python stage_analysis_pipeline.py --screen --young-weeks 4 \
        --close-min 100 --close-max 5000 --market-cap-max 30000 \
        --volume-ratio-min 1.5

    Screener filters (all must pass):
      - Stage just transitioned from Stage 1 -> Stage 2, and has been in
        Stage 2 for <= --young-weeks weeks ("young" stage 2, i.e. not an
        old/extended advance).
      - latest close > --close-min AND < --close-max
      - market_cap < --market-cap-max  (nse_universe.market_cap)
      - latest DAILY volume >= --volume-ratio-min * SMA(daily volume, 20)

    Results are written to results/stage1_to_stage2_screen.csv and printed.

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

UNIVERSE_SCHEMA: str = "universe"
UNIVERSE_TABLE: str = "nse_universe"
STAGES_SCHEMA: str   = "stage"
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

# ── Stage 1 -> Stage 2 "young breakout" screener ──────────────────────────────
UNIVERSE_MARKET_CAP_COLUMN: str = "market_cap"   # column in nse_universe

DAILY_VOLUME_SMA_PERIOD: int = 20
DAILY_N_BARS: int            = DAILY_VOLUME_SMA_PERIOD + 15   # buffer for holidays/gaps

YOUNG_STAGE2_MAX_WEEKS: int  = 4       # "young" = in Stage 2 for at most this many weeks
# How many weeks before the start of the current Stage-2 run to look back for
# evidence of Stage 1 (the SMA-slope condition lags the close/midpoint
# crossover, so a breakout often shows a few transitional Stage-3 bars right
# before flipping to Stage 2 -- this window lets that pass as long as no
# Stage 4 shows up in between, which would signal a real prior downtrend/top).
STAGE1_LOOKBACK_WEEKS: int   = 8
SCREEN_CLOSE_MIN: float      = 100.0
SCREEN_CLOSE_MAX: float      = 5000.0
SCREEN_MARKET_CAP_MAX: float = 30000.0
SCREEN_VOLUME_RATIO_MIN: float = 1.5

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
    # -- transition info (weekly, used by the Stage1->Stage2 screener) --
    stage_prev: Optional[int] = None      # stage held immediately before the current Stage-2 run
    weeks_in_stage: int = 0               # consecutive weeks in the current stage
    is_fresh_stage2: bool = False         # current stage == 2 AND stage_prev == 1
    # -- daily volume info (only populated when --screen is used) --
    market_cap: Optional[float] = None
    latest_volume_daily: Optional[int] = None
    sma_volume_20_daily: Optional[float] = None
    volume_ratio: Optional[float] = None
    passes_screen: bool = False


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


def detect_stage2_freshness(stages, lookback=STAGE1_LOOKBACK_WEEKS):
    """
    Given a list/array of stage integers (oldest to newest), determine:
      - weeks_in_stage: consecutive weeks in the current (latest) stage
      - stage_prev: the stage held immediately before the current run (or None)
      - is_fresh_stage2: True if current stage is 2, preceded by Stage 1
        (within `lookback` weeks, skipping transitional Stage 3), and no
        Stage 4 in that window.

    This is the exact algorithm from compute_stage() lines 296-322, extracted
    as a standalone callable so stage1_to_stage2_screen_v3.py (and other
    dependents) can import it directly.
    """
    if not len(stages):
        return 0, None, False

    stage = int(stages[-1])
    weeks_in_stage = 1
    i = len(stages) - 1
    while i > 0 and stages[i - 1] == stage:
        weeks_in_stage += 1
        i -= 1
    stage_prev = int(stages[i - 1]) if i > 0 else None

    j = i
    while j > 0 and stages[j - 1] == 3:
        j -= 1
    window_start = max(0, j - lookback)
    context_stages = stages[window_start:i]
    is_fresh_stage2 = (
        stage == 2
        and 1 in context_stages
        and 4 not in context_stages
    )

    return weeks_in_stage, stage_prev, is_fresh_stage2


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

    valid = df.dropna(subset=["sma_30", "sma_slope", "high_52w", "low_52w"]).copy()
    if valid.empty:
        return None

    # ── Vectorized stage classification for every valid row ────────────────
    # (needed so we can look back and see what stage a ticker was in before
    # the current one, to detect a Stage 1 -> Stage 2 transition)
    slope_s = valid["sma_slope"]
    close_s = valid["close"]
    sma_s   = valid["sma_30"]
    mid_s   = valid["mid_52w"]

    conditions = [
        (slope_s > FLAT_THRESHOLD) & (close_s > sma_s),                    # Stage 2
        (slope_s < -FLAT_THRESHOLD) & (close_s < sma_s),                   # Stage 4
        (slope_s.abs() <= FLAT_THRESHOLD) & (close_s < mid_s),             # Stage 1
        (slope_s.abs() <= FLAT_THRESHOLD) & (close_s >= mid_s),            # Stage 3
    ]
    choices = [2, 4, 1, 3]
    valid["stage"] = np.select(conditions, choices, default=1)   # edge case fallback -> 1

    latest   = valid.iloc[-1]
    stages   = valid["stage"].to_numpy()

    close    = float(latest["close"])
    sma_30   = float(latest["sma_30"])
    slope    = float(latest["sma_slope"])
    high_52w = float(latest["high_52w"])
    low_52w  = float(latest["low_52w"])
    mid_52w  = float(latest["mid_52w"])
    volume   = int(latest["volume"]) if not np.isnan(latest["volume"]) else 0
    stage    = int(latest["stage"])

    bar_date = latest.name
    analysis_date = (
        bar_date.date().isoformat()
        if isinstance(bar_date, pd.Timestamp)
        else str(bar_date)[:10]
    )

    # ── Transition freshness (extracted to detect_stage2_freshness) ────────
    weeks_in_stage, stage_prev, is_fresh_stage2 = detect_stage2_freshness(
        stages, lookback=STAGE1_LOOKBACK_WEEKS
    )

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
        stage_prev       = stage_prev,
        weeks_in_stage   = weeks_in_stage,
        is_fresh_stage2  = is_fresh_stage2,
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
            supabase.schema(UNIVERSE_SCHEMA).table(UNIVERSE_TABLE)
            .select(f"ticker, sector, industry, company_name, {UNIVERSE_MARKET_CAP_COLUMN}")
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
            supabase.schema(STAGES_SCHEMA).table(STAGES_TABLE)
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
        supabase.schema(STAGES_SCHEMA).table(STAGES_TABLE).upsert(
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


def _fetch_hist_with_retries(
    ticker: str,
    interval: Interval,
    n_bars: int,
) -> Optional[pd.DataFrame]:
    """
    Shared fetch-with-backoff logic (used for both the weekly bars needed for
    stage calculation and, when --screen is on, the daily bars needed for the
    volume-spike check). Returns None if the ticker could not be fetched.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with _connection_semaphore:          # cap concurrent WebSocket connections
                time.sleep(REQUEST_DELAY)
                tv     = _get_thread_tv()
                raw_df = tv.get_hist(
                    symbol   = ticker,
                    exchange = DEFAULT_EXCHANGE,
                    interval = interval,
                    n_bars   = n_bars,
                )

            if raw_df is None or raw_df.empty:
                return None

            raw_df.columns = [c.lower() for c in raw_df.columns]
            return raw_df

        except Exception as exc:
            is_429 = _is_rate_limit_error(exc)

            if attempt < MAX_RETRIES:
                wait = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
                if is_429:
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
                return None
    return None


def fetch_daily_volume_stats(ticker: str) -> Tuple[Optional[int], Optional[float]]:
    """
    Fetch daily bars and return (latest_volume, sma_volume_20).
    Either value may be None if data was unavailable/insufficient.
    """
    raw_df = _fetch_hist_with_retries(ticker, Interval.in_daily, DAILY_N_BARS)
    if raw_df is None or raw_df.empty:
        return None, None

    raw_df = raw_df.sort_index()
    if len(raw_df) < DAILY_VOLUME_SMA_PERIOD:
        return None, None

    sma20 = raw_df["volume"].rolling(window=DAILY_VOLUME_SMA_PERIOD, min_periods=DAILY_VOLUME_SMA_PERIOD).mean()
    latest_volume = int(raw_df["volume"].iloc[-1])
    latest_sma20  = float(sma20.iloc[-1]) if not np.isnan(sma20.iloc[-1]) else None
    return latest_volume, latest_sma20


# ---------------------------------------------------------------------------
# Worker function  (runs in thread pool)
# ---------------------------------------------------------------------------

def process_ticker(
    ticker: str,
    now_utc: str,
    market_cap: Optional[float] = None,
    screen: bool = False,
) -> Tuple[str, Optional[dict], Optional[str], Optional[StageRecord]]:
    """
    Fetch OHLCV, compute stage, build payload dict for one ticker.
    If screen=True, also fetches daily bars for the volume-spike check and
    evaluates the Stage1->Stage2 "young breakout" screen.

    Returns:
        (ticker, payload, error_tag, record)
        - payload is None on failure (this is what gets upserted to Supabase,
          unchanged from before -- the screener never alters this)
        - error_tag is None on success, "no_data" or "compute_error" on failure
        - record is the full StageRecord (incl. screen fields), or None on failure
    """
    raw_df = _fetch_hist_with_retries(ticker, Interval.in_weekly, N_BARS)
    if raw_df is None:
        return (ticker, None, "no_data", None)

    # ── Compute stage (pure CPU, no I/O) ─────────────────────────────────────
    try:
        record = compute_stage(raw_df)
        if record is None:
            return (ticker, None, "compute_error", None)
        record.ticker = ticker
    except Exception as exc:
        log.error(
            "X Stage computation failed for %s: %s: %s",
            ticker, type(exc).__name__, exc,
        )
        return (ticker, None, "compute_error", None)

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

    # ── Screener (only runs the extra daily fetch when explicitly asked) ────
    if screen:
        record.market_cap = market_cap

        # Cheap checks first -- skip the daily fetch entirely if this ticker
        # can't possibly qualify (saves an API call per non-candidate).
        close_ok = SCREEN_CLOSE_MIN < record.close < SCREEN_CLOSE_MAX
        cap_ok   = market_cap is not None and market_cap < SCREEN_MARKET_CAP_MAX
        fresh_ok = record.is_fresh_stage2 and record.weeks_in_stage <= YOUNG_STAGE2_MAX_WEEKS

        if close_ok and cap_ok and fresh_ok:
            latest_vol, sma20 = fetch_daily_volume_stats(ticker)
            record.latest_volume_daily  = latest_vol
            record.sma_volume_20_daily  = sma20
            if latest_vol is not None and sma20 not in (None, 0):
                record.volume_ratio = latest_vol / sma20
                record.passes_screen = record.volume_ratio >= SCREEN_VOLUME_RATIO_MIN

    return (ticker, payload, None, record)


def load_failed_tickers(path: str) -> Set[str]:
    """
    Read a prior run's results/stage.json and return the set of failed tickers
    to retry. Prefers the full 'failed_tickers' list; falls back to the
    truncated 'errors' preview (older runs / files that only kept the first
    10) with a warning, since that will silently under-retry.
    """
    p = pathlib.Path(path)
    if not p.exists():
        log.error("X --retry-failed file not found: %s", path)
        sys.exit(1)

    data = json.loads(p.read_text())
    failed = data.get("failed_tickers")
    if failed:
        log.info("Retry mode: loaded %d failed tickers from %s.", len(failed), path)
        return set(failed)

    errors = data.get("errors", [])
    reported_failed = data.get("failed")
    if reported_failed and len(errors) < reported_failed:
        log.warning(
            "X %s only has a truncated 'errors' list (%d of %d actually failed). "
            "This file predates the full 'failed_tickers' field -- only %d "
            "tickers will be retried, not the full %d. Re-run without "
            "--retry-failed once to regenerate a complete stage.json.",
            path, len(errors), reported_failed, len(errors), reported_failed,
        )
    return set(errors)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(do_upsert: bool, screen: bool = False, retry_failed_path: Optional[str] = None) -> PipelineSummary:
    summary    = PipelineSummary(workers=MAX_WORKERS)
    today_str  = date.today().isoformat()
    now_utc    = datetime.now(timezone.utc).isoformat()
    start_time = time.time()

    log.info(
        "=== Stage Analysis Pipeline starting (mode: %s, workers: %d, max_retries: %d, screen: %s, retry_failed: %s) ===",
        "UPSERT" if do_upsert else "PREVIEW ONLY", MAX_WORKERS, MAX_RETRIES, screen, retry_failed_path,
    )

    supabase = get_supabase_client()
    universe = fetch_all_tickers(supabase)

    if retry_failed_path:
        retry_set = load_failed_tickers(retry_failed_path)
        before    = len(universe)
        universe  = [r for r in universe if r["ticker"] in retry_set]
        missing   = retry_set - {r["ticker"] for r in universe}
        log.info(
            "Retry mode: narrowed universe from %d to %d tickers.",
            before, len(universe),
        )
        if missing:
            log.warning(
                "%d ticker(s) from the retry list are no longer in the active "
                "universe (renamed/delisted?) and will be skipped: %s",
                len(missing), ", ".join(sorted(missing)[:10]),
            )

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

    market_caps = {r["ticker"]: r.get(UNIVERSE_MARKET_CAP_COLUMN) for r in universe}
    screen_matches: List[StageRecord] = []

    log.info(
        "Processing %d tickers (%d skipped, %d workers) ...",
        total, summary.skipped, MAX_WORKERS,
    )

    pending_records: List[dict] = []

    # ── Parallel fetch + compute ──────────────────────────────────────────────
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                process_ticker, row["ticker"], now_utc,
                market_caps.get(row["ticker"]), screen,
            ): row["ticker"]
            for row in pending
        }

        completed = 0
        for future in as_completed(futures):
            ticker = futures[future]
            completed += 1

            try:
                result_ticker, payload, error, record = future.result()

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

                if screen and record is not None and record.passes_screen:
                    log.info(
                        "  MATCH %s | Close: %.2f | MCap: %s | Vol/SMA20: %.2fx | "
                        "Weeks in Stage 2: %d",
                        ticker, record.close, record.market_cap,
                        record.volume_ratio, record.weeks_in_stage,
                    )
                    screen_matches.append(record)

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
        "script":         "Stage Analysis (Weekly)",
        "succeeded":      summary.succeeded,
        "failed":         summary.failed,
        "skipped":        summary.skipped,
        "total":          summary.total,
        "errors":         summary.failed_tickers[:10],   # short preview for GH Actions summary
        "failed_tickers": summary.failed_tickers,         # full list, used by --retry-failed
    }))

    if screen:
        write_screen_report(screen_matches)

    return summary


def write_screen_report(matches: List[StageRecord]) -> None:
    """Write Stage1->Stage2 'young breakout' matches to CSV and print a table."""
    out_path = pathlib.Path("results/stage1_to_stage2_screen.csv")
    matches  = sorted(matches, key=lambda r: r.volume_ratio or 0, reverse=True)

    header = [
        "ticker", "close", "market_cap", "weeks_in_stage2",
        "latest_volume_daily", "sma_volume_20_daily", "volume_ratio",
        "sma_30", "sma_slope", "week_52_high", "week_52_low", "analysis_date",
    ]
    lines = [",".join(header)]
    for r in matches:
        lines.append(",".join(str(v) for v in [
            r.ticker, r.close, r.market_cap, r.weeks_in_stage,
            r.latest_volume_daily, r.sma_volume_20_daily,
            round(r.volume_ratio, 3) if r.volume_ratio else "",
            r.sma_30, r.sma_slope, r.week_52_high, r.week_52_low, r.analysis_date,
        ]))
    out_path.write_text("\n".join(lines))

    log.info("")
    log.info("=" * 62)
    log.info("  STAGE 1 -> STAGE 2 YOUNG BREAKOUT SCREEN -- %d match(es)", len(matches))
    log.info("  Filters: close in (%.0f, %.0f) | market_cap < %.0f | "
              "vol >= %.1fx SMA(20) | weeks_in_stage2 <= %d",
              SCREEN_CLOSE_MIN, SCREEN_CLOSE_MAX, SCREEN_MARKET_CAP_MAX,
              SCREEN_VOLUME_RATIO_MIN, YOUNG_STAGE2_MAX_WEEKS)
    log.info("=" * 62)
    for r in matches:
        log.info(
            "  %-12s close=%-9.2f mcap=%-10s weeks_in_st2=%-3d vol_ratio=%.2fx",
            r.ticker, r.close, r.market_cap, r.weeks_in_stage, r.volume_ratio,
        )
    log.info("  Saved to %s", out_path)
    log.info("=" * 62)


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
    parser.add_argument(
        "--screen",
        action="store_true",
        help="Also screen for young Stage1->Stage2 transitions meeting the "
             "close/market-cap/volume filters (fetches extra daily bars). "
             "Writes results/stage1_to_stage2_screen.csv.",
    )
    parser.add_argument("--young-weeks", type=int, default=YOUNG_STAGE2_MAX_WEEKS,
        help=f"Max weeks in Stage 2 to still count as 'young' (default: {YOUNG_STAGE2_MAX_WEEKS}).")
    parser.add_argument("--close-min", type=float, default=SCREEN_CLOSE_MIN,
        help=f"Minimum latest close (default: {SCREEN_CLOSE_MIN}).")
    parser.add_argument("--close-max", type=float, default=SCREEN_CLOSE_MAX,
        help=f"Maximum latest close (default: {SCREEN_CLOSE_MAX}).")
    parser.add_argument("--market-cap-max", type=float, default=SCREEN_MARKET_CAP_MAX,
        help=f"Maximum market cap (default: {SCREEN_MARKET_CAP_MAX}).")
    parser.add_argument("--volume-ratio-min", type=float, default=SCREEN_VOLUME_RATIO_MIN,
        help=f"Minimum latest_volume / SMA(volume,20) (default: {SCREEN_VOLUME_RATIO_MIN}).")
    parser.add_argument(
        "--retry-failed",
        type=str,
        default=None,
        metavar="PATH",
        help="Only reprocess tickers listed as failed in a prior run's "
             "stage.json (e.g. results/stage.json), instead of the full "
             "universe. Useful for mopping up 429/transient failures "
             "without a full re-run. Combine with --workers to lower "
             "concurrency for the retry pass.",
    )
    args = parser.parse_args()

    MAX_WORKERS           = args.workers
    _connection_semaphore = threading.Semaphore(MAX_WORKERS)

    YOUNG_STAGE2_MAX_WEEKS  = args.young_weeks
    SCREEN_CLOSE_MIN        = args.close_min
    SCREEN_CLOSE_MAX        = args.close_max
    SCREEN_MARKET_CAP_MAX   = args.market_cap_max
    SCREEN_VOLUME_RATIO_MIN = args.volume_ratio_min

    summary = run_pipeline(
        do_upsert=not args.preview_only,
        screen=args.screen,
        retry_failed_path=args.retry_failed,
    )
    print(summary.report())
