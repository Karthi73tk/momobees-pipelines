"""
rrg_pipeline.py
===============
RRG (Relative Rotation Graph) Data Pipeline
Calculates JdK RS-Ratio and JdK RS-Momentum for NSE stocks
and upserts the results into a Supabase PostgreSQL database.

Usage:
    python rrg_pipeline.py                # full run (fetch + upsert)
    python rrg_pipeline.py --preview-only # fetch + compute, no DB writes
    python rrg_pipeline.py --workers 3    # override worker count

Environment variables (.env.local):
    NEXT_PUBLIC_SUPABASE_URL=https://xxxx.supabase.co
    SUPABASE_SERVICE_ROLE_KEY=eyJ...

    # Optional overrides (defaults shown):
    BENCHMARK_TICKER=NIFTY
    BENCHMARK_EXCHANGE=NSE
    STOCK_EXCHANGE=NSE
    LOOKBACK_PERIODS=150
    TAIL_PERIODS=12

Rate limiting strategy:
    - MAX_WORKERS=4 (conservative default — avoids 429s on free TV accounts)
    - threading.Semaphore caps simultaneous open WebSocket connections
    - 429 errors trigger exponential backoff (2s → 4s → 8s → 16s, up to MAX_RETRIES)
    - Per-worker REQUEST_DELAY=0.5s as a base courtesy gap between calls

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
from datetime import datetime, timezone
from typing import List, Optional, Set, Dict, Tuple

import pandas as pd
import numpy as np
from dotenv import load_dotenv
from supabase import create_client, Client
from tvDatafeed import TvDatafeed, Interval

# ---------------------------------------------------------------------------
# Configuration & Logging
# ---------------------------------------------------------------------------

load_dotenv(".env.local")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

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
RRG_TABLE: str      = "rrg_data"

# ── Exchange / benchmark ──────────────────────────────────────────────────────
BENCHMARK_TICKER: str   = os.getenv("BENCHMARK_TICKER",   "NIFTY")
BENCHMARK_EXCHANGE: str = os.getenv("BENCHMARK_EXCHANGE", "NSE")
STOCK_EXCHANGE: str     = os.getenv("STOCK_EXCHANGE",     "NSE")
LOOKBACK_PERIODS: int   = int(os.getenv("LOOKBACK_PERIODS", "150"))
TAIL_PERIODS: int       = int(os.getenv("TAIL_PERIODS",     "12"))
TIMEFRAME: str          = "weekly"

# ── Parallelism & rate limiting ───────────────────────────────────────────────
# 4 workers is safe for free TradingView accounts.
# Raise to 6-8 only if you have a Pro/Pro+ account.
MAX_WORKERS: int     = 4
REQUEST_DELAY: float = 0.5    # Base gap between each worker's requests (seconds)

# Retry config for 429 / transient errors
MAX_RETRIES: int        = 4   # Total attempts per ticker (1 original + 3 retries)
BACKOFF_BASE: float     = 2.0 # Exponential backoff base (2s, 4s, 8s, 16s)
BACKOFF_MAX: float      = 60.0

# ── Batch upsert ──────────────────────────────────────────────────────────────
UPSERT_BATCH_TICKERS: int = 50   # 50 tickers × 12 rows = ~600 rows per DB call

# ── RRG parameters (standard JdK settings) ────────────────────────────────────
RS_RATIO_PERIOD: int    = 14
RS_RATIO_MA_PERIOD: int = 65
RS_MOMENTUM_PERIOD: int = 11
ROC_PERIOD: int         = 1
ROC_BASELINE_MIN: float = 0.01

# Semaphore limits concurrent open WebSocket connections regardless of worker count.
# Keep this <= MAX_WORKERS. Lowering it further reduces 429 risk.
_connection_semaphore: threading.Semaphore = threading.Semaphore(MAX_WORKERS)


# ---------------------------------------------------------------------------
# Supabase client
# ---------------------------------------------------------------------------

def get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error(
            "X Missing env vars. Need NEXT_PUBLIC_SUPABASE_URL "
            "and SUPABASE_SERVICE_ROLE_KEY in .env.local"
        )
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ---------------------------------------------------------------------------
# Ticker fetching
# ---------------------------------------------------------------------------

def fetch_tickers(supabase: Client) -> List[str]:
    logger.info("Fetching ticker list from %s ...", UNIVERSE_TABLE)
    all_tickers: List[str] = []
    page_size = 1000
    offset = 0

    while True:
        response = (
            supabase.table(UNIVERSE_TABLE)
            .select("ticker")
            .eq("is_active", True)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = [row["ticker"] for row in (response.data or []) if row.get("ticker")]
        all_tickers.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    if not all_tickers:
        raise ValueError("No tickers returned from %s." % UNIVERSE_TABLE)

    logger.info("Fetched %d active tickers.", len(all_tickers))
    return all_tickers


# ---------------------------------------------------------------------------
# Already-updated check
# ---------------------------------------------------------------------------

def fetch_already_updated_tickers(supabase: Client, today_str: str) -> Set[str]:
    """
    Return tickers whose most-recent updated_at in rrg_data is today (UTC).
    Pages through DESC-ordered results, keeping the first (most recent)
    updated_at per ticker, marks as done if date portion == today_str.
    """
    latest_updated_at: Dict[str, str] = {}
    page_size = 1000
    offset = 0

    while True:
        response = (
            supabase.table(RRG_TABLE)
            .select("ticker, updated_at")
            .eq("timeframe", TIMEFRAME)
            .eq("benchmark_ticker", BENCHMARK_TICKER)
            .order("updated_at", desc=True)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        raw_batch = response.data or []

        for row in raw_batch:
            t  = row.get("ticker")
            ua = row.get("updated_at")
            if t and ua and t not in latest_updated_at:
                latest_updated_at[t] = ua

        if len(raw_batch) < page_size:
            break
        offset += page_size

    already_done: Set[str] = {
        ticker for ticker, ua in latest_updated_at.items()
        if ua[:10] == today_str
    }

    if already_done:
        logger.info(
            "Already updated today (%s): %d tickers will be skipped.",
            today_str, len(already_done),
        )
    else:
        logger.info("No tickers updated today (%s) -- full run will proceed.", today_str)
    return already_done


# ---------------------------------------------------------------------------
# Price data fetching with retry + backoff
# ---------------------------------------------------------------------------

_thread_local = threading.local()

def _get_thread_tv() -> TvDatafeed:
    """Return (or create) a per-thread TvDatafeed instance."""
    if not hasattr(_thread_local, "tv"):
        _thread_local.tv = TvDatafeed()
    return _thread_local.tv


def _is_rate_limit_error(exc: Exception) -> bool:
    """Return True if exception looks like a TradingView 429."""
    msg = str(exc).lower()
    return "429" in msg or "too many requests" in msg


def fetch_weekly_closes_with_retry(
    symbol: str,
    exchange: str,
    n_bars: int = LOOKBACK_PERIODS,
) -> Optional[pd.Series]:
    """
    Fetch weekly closes with exponential backoff on 429 / transient errors.
    Uses the semaphore to cap concurrent open WebSocket connections.

    Backoff schedule (BACKOFF_BASE=2, MAX_RETRIES=4):
      Attempt 1 — immediate
      Attempt 2 — wait 2s  after failure
      Attempt 3 — wait 4s  after failure
      Attempt 4 — wait 8s  after failure
    """
    last_exc = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with _connection_semaphore:           # cap concurrent connections
                time.sleep(REQUEST_DELAY)         # courtesy gap per attempt
                tv = _get_thread_tv()
                df = tv.get_hist(
                    symbol   = symbol,
                    exchange = exchange,
                    interval = Interval.in_weekly,
                    n_bars   = n_bars,
                )

            if df is None or df.empty:
                return None

            df.index = pd.to_datetime(df.index).normalize()
            closes = df["close"].dropna()

            min_bars = RS_RATIO_MA_PERIOD + RS_RATIO_PERIOD + ROC_PERIOD + 5
            if len(closes) < min_bars:
                return None

            return closes   # success

        except Exception as exc:
            last_exc = exc
            is_429   = _is_rate_limit_error(exc)

            if attempt < MAX_RETRIES:
                wait = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
                if is_429:
                    # On 429, also recreate the thread-local TV instance —
                    # the existing WebSocket connection may be poisoned.
                    if hasattr(_thread_local, "tv"):
                        del _thread_local.tv
                    logger.warning(
                        "429 on %s (attempt %d/%d) — backing off %.0fs ...",
                        symbol, attempt, MAX_RETRIES, wait,
                    )
                else:
                    logger.warning(
                        "Transient error on %s (attempt %d/%d): %s — retrying in %.0fs ...",
                        symbol, attempt, MAX_RETRIES, type(exc).__name__, wait,
                    )
                time.sleep(wait)
            else:
                logger.error(
                    "X Giving up on %s after %d attempts. Last error: %s: %s",
                    symbol, MAX_RETRIES, type(exc).__name__, exc,
                )

    return None


# ---------------------------------------------------------------------------
# RRG Calculations
# ---------------------------------------------------------------------------

def calculate_rrg(
    ticker_closes: pd.Series,
    benchmark_closes: pd.Series,
) -> Optional[pd.DataFrame]:
    """
    Calculate JdK RS-Ratio and RS-Momentum for a single ticker.

    RS-Ratio:    (EMA(RS, 14) / SMA(RS, 65)) * 100   -> centred at 100
    RS-Momentum: z-score of ROC normalised by rolling std x 10 + 100
                 -> avoids near-zero blowups from mean-based normalisation
    """
    try:
        combined = pd.DataFrame({
            "ticker":    ticker_closes,
            "benchmark": benchmark_closes,
        }).dropna()

        min_bars = RS_RATIO_MA_PERIOD + RS_RATIO_PERIOD + ROC_PERIOD + 5
        if len(combined) < min_bars:
            return None

        combined["rs"]     = combined["ticker"] / combined["benchmark"]
        combined["rs_ema"] = combined["rs"].ewm(span=RS_RATIO_PERIOD, adjust=False).mean()
        combined["rs_ma"]  = combined["rs"].rolling(
            window=RS_RATIO_MA_PERIOD, min_periods=RS_RATIO_MA_PERIOD
        ).mean()
        combined["rs_ratio"] = (combined["rs_ema"] / combined["rs_ma"]) * 100

        combined["roc"] = combined["rs_ratio"].pct_change(periods=ROC_PERIOD) * 100

        roc_window = RS_MOMENTUM_PERIOD * 3
        combined["roc_ema"] = combined["roc"].ewm(span=RS_MOMENTUM_PERIOD, adjust=False).mean()
        combined["roc_std"] = combined["roc"].rolling(
            window=roc_window, min_periods=roc_window
        ).std()
        safe_std = combined["roc_std"].clip(lower=ROC_BASELINE_MIN)
        combined["rs_momentum"] = (combined["roc_ema"] / safe_std) * 10 + 100
        combined["rs_momentum"] = combined["rs_momentum"].fillna(100)

        result = combined[["rs_ratio", "rs_momentum", "ticker"]].copy()
        result.rename(columns={"ticker": "close_price"}, inplace=True)
        result = result.dropna(subset=["rs_ratio", "rs_momentum"])

        return result if not result.empty else None

    except Exception as exc:
        logger.error("RRG calculation error: %s: %s", type(exc).__name__, exc)
        return None


# ---------------------------------------------------------------------------
# Worker function
# ---------------------------------------------------------------------------

def process_ticker(
    ticker: str,
    benchmark_closes: pd.Series,
    now_utc: str,
) -> Tuple[str, Optional[List[dict]], Optional[str]]:
    """
    Fetch + compute + build records for one ticker.
    Returns (ticker, records, error_tag) where error_tag is None on success.
    """
    ticker_closes = fetch_weekly_closes_with_retry(ticker, STOCK_EXCHANGE)
    if ticker_closes is None:
        return (ticker, None, "no_data")

    rrg_df = calculate_rrg(ticker_closes, benchmark_closes)
    if rrg_df is None or rrg_df.empty:
        return (ticker, None, "no_rrg")

    recent  = rrg_df.tail(TAIL_PERIODS).copy()
    records = [
        {
            "ticker":           ticker,
            "date":             date_idx.strftime("%Y-%m-%d"),
            "timeframe":        TIMEFRAME,
            "rs_ratio":         round(float(row["rs_ratio"]),    6),
            "rs_momentum":      round(float(row["rs_momentum"]), 6),
            "benchmark_ticker": BENCHMARK_TICKER,
            "close_price":      round(float(row["close_price"]), 4),
            "updated_at":       now_utc,
        }
        for date_idx, row in recent.iterrows()
    ]

    return (ticker, records, None)


# ---------------------------------------------------------------------------
# Batch flusher
# ---------------------------------------------------------------------------

def flush_batch(supabase: Client, records: List[dict]) -> int:
    """Upsert accumulated records in a single DB call. Returns rows written."""
    if not records:
        return 0
    try:
        supabase.table(RRG_TABLE).upsert(
            records,
            on_conflict="ticker,date,timeframe,benchmark_ticker",
        ).execute()
        tickers_in_batch = len({r["ticker"] for r in records})
        logger.info(
            "  ^ Flushed %d records (%d tickers) to Supabase.",
            len(records), tickers_in_batch,
        )
        return len(records)
    except Exception as exc:
        logger.error(
            "X Batch upsert failed (%d rows): %s: %s",
            len(records), type(exc).__name__, exc,
        )
        logger.debug(traceback.format_exc())
        return 0


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(do_upsert: bool) -> None:
    logger.info(
        "=== RRG Pipeline starting (mode: %s, workers: %d, max_retries: %d) ===",
        "UPSERT" if do_upsert else "PREVIEW ONLY", MAX_WORKERS, MAX_RETRIES,
    )
    start_time = time.time()

    supabase = get_supabase_client()
    tickers  = fetch_tickers(supabase)

    # ── Fetch benchmark (single-threaded, done once) ──────────────────────────
    logger.info("Fetching benchmark: %s (%s) ...", BENCHMARK_TICKER, BENCHMARK_EXCHANGE)
    benchmark_tv     = TvDatafeed()
    benchmark_closes = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            df = benchmark_tv.get_hist(
                symbol   = BENCHMARK_TICKER,
                exchange = BENCHMARK_EXCHANGE,
                interval = Interval.in_weekly,
                n_bars   = LOOKBACK_PERIODS,
            )
            if df is not None and not df.empty:
                df.index        = pd.to_datetime(df.index).normalize()
                benchmark_closes = df["close"].dropna()
                break
        except Exception as exc:
            wait = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
            logger.warning(
                "Benchmark fetch attempt %d/%d failed: %s -- retrying in %.0fs ...",
                attempt, MAX_RETRIES, exc, wait,
            )
            time.sleep(wait)

    if benchmark_closes is None:
        logger.error("X Failed to fetch benchmark %s after %d attempts. Aborting.",
                     BENCHMARK_TICKER, MAX_RETRIES)
        sys.exit(1)

    logger.info(
        "Benchmark loaded: %d weekly bars (%s -> %s).",
        len(benchmark_closes),
        benchmark_closes.index.min().date(),
        benchmark_closes.index.max().date(),
    )

    # ── Already-done check ────────────────────────────────────────────────────
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now_utc   = datetime.now(timezone.utc).isoformat()

    already_done: Set[str] = set()
    if do_upsert:
        logger.info("Checking tickers already updated today (%s UTC) ...", today_str)
        already_done = fetch_already_updated_tickers(supabase, today_str)

    pending_tickers = [t for t in tickers if t not in already_done]
    skipped_today   = len(tickers) - len(pending_tickers)
    total           = len(pending_tickers)

    logger.info(
        "Processing %d tickers (%d skipped, %d workers) ...",
        total, skipped_today, MAX_WORKERS,
    )

    # ── Counters ──────────────────────────────────────────────────────────────
    success_count = 0
    no_data_count = 0
    no_rrg_count  = 0
    error_count   = 0

    failed_tickers: List[str] = []
    error_tickers:  List[str] = []

    pending_records:  List[dict] = []
    tickers_in_batch: int        = 0

    # ── Parallel fetch + compute ──────────────────────────────────────────────
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_ticker, ticker, benchmark_closes, now_utc): ticker
            for ticker in pending_tickers
        }

        completed = 0
        for future in as_completed(futures):
            ticker = futures[future]
            completed += 1

            try:
                result_ticker, records, error = future.result()

                if error == "no_data":
                    logger.warning(
                        "[%d/%d] %s -- no price data (exhausted retries).",
                        completed, total, ticker,
                    )
                    no_data_count += 1
                    failed_tickers.append(ticker)
                    continue

                if error == "no_rrg":
                    logger.warning(
                        "[%d/%d] %s -- RRG calc returned no data.",
                        completed, total, ticker,
                    )
                    no_rrg_count += 1
                    failed_tickers.append(ticker)
                    continue

                last_rs  = records[-1]["rs_ratio"]   if records else 0.0
                last_mom = records[-1]["rs_momentum"] if records else 0.0
                logger.info(
                    "[%d/%d] %s -> RS-Ratio: %.4f | RS-Mom: %.4f",
                    completed, total, ticker, last_rs, last_mom,
                )
                success_count += 1

                if do_upsert and records:
                    pending_records.extend(records)
                    tickers_in_batch += 1

                    if tickers_in_batch >= UPSERT_BATCH_TICKERS:
                        flush_batch(supabase, pending_records)
                        pending_records.clear()
                        tickers_in_batch = 0

            except Exception as exc:
                logger.error(
                    "[%d/%d] X Unhandled error for %s: %s: %s",
                    completed, total, ticker, type(exc).__name__, exc,
                )
                logger.debug(traceback.format_exc())
                error_count += 1
                error_tickers.append(ticker)

    # ── Final flush ───────────────────────────────────────────────────────────
    if do_upsert and pending_records:
        flush_batch(supabase, pending_records)
        pending_records.clear()

    elapsed   = time.time() - start_time
    processed = success_count + no_data_count + no_rrg_count + error_count
    unvisited = max(0, total - processed)

    # ── Summary ───────────────────────────────────────────────────────────────
    mode_label = "UPSERT COMPLETE" if do_upsert else "PREVIEW COMPLETE (no DB writes)"
    summary_lines = [
        "",
        "=" * 62,
        f"  RRG PIPELINE -- {mode_label}",
        "=" * 62,
        f"  Total tickers          : {len(tickers)}",
        f"  Succeeded              : {success_count}",
        f"  Skipped (up-to-date)   : {skipped_today}",
        f"  No price data          : {no_data_count}",
        f"  RRG calc failed        : {no_rrg_count}",
        f"  Errors (exceptions)    : {error_count}",
        f"  Unvisited              : {unvisited}",
        f"  Workers                : {MAX_WORKERS}",
        f"  Max retries / ticker   : {MAX_RETRIES}",
        f"  Elapsed time           : {elapsed:.1f}s",
        "=" * 62,
    ]

    if failed_tickers:
        summary_lines.append(
            "  Failed tickers (%d): %s" % (
                len(failed_tickers), ", ".join(failed_tickers[:20])
            )
        )
        if len(failed_tickers) > 20:
            summary_lines.append("    ... and %d more." % (len(failed_tickers) - 20))

    if error_tickers:
        summary_lines.append(
            "  Error tickers  (%d): %s" % (
                len(error_tickers), ", ".join(error_tickers[:20])
            )
        )
        if len(error_tickers) > 20:
            summary_lines.append("    ... and %d more." % (len(error_tickers) - 20))

    summary_lines.append("=" * 62)
    logger.info("\n".join(summary_lines))

    # ── Write result JSON for GitHub Actions summary ──────────────────────────
    total_failed = len(failed_tickers) + len(error_tickers)
    pathlib.Path("results").mkdir(exist_ok=True)
    pathlib.Path("results/rrg.json").write_text(json.dumps({
        "script":    "RRG Pipeline (Weekly)",
        "succeeded": success_count,
        "failed":    total_failed,
        "skipped":   skipped_today,
        "total":     len(tickers),
        "errors":    (failed_tickers + error_tickers)[:10],
    }))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="RRG (Relative Rotation Graph) Data Pipeline."
    )
    parser.add_argument(
        "--preview-only",
        action="store_true",
        help="Fetch and compute RRG values but do NOT write to Supabase.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS,
        help=f"Number of parallel fetch workers (default: {MAX_WORKERS}). "
             f"Lower if seeing 429 errors.",
    )
    args = parser.parse_args()
    MAX_WORKERS            = args.workers
    _connection_semaphore  = threading.Semaphore(MAX_WORKERS)

    run_pipeline(do_upsert=not args.preview_only)
