"""
Market Indices — Data Sync Engine
------------------------------------------
Fetches daily OHLCV for a small starter list of NSE market indices via
tvDatafeed (same fetch_ohlcv_with_retry pattern as data_sync_engine_n750_d.py:
per-thread TvDatafeed instances, symbol/exchange passed separately,
semaphore-gated, exponential backoff on 429s) and computes:
    latest close, previous close, 1-day % change, EMA200, above_ema200

Writes        : universe.indices, screener.indices_metrics
Result file   : results/indices.json  (for GitHub Actions summary)

*** SYMBOL LIST NOT VERIFIED ***
This session has no live network/package access to tvDatafeed/TradingView,
so INDEX_UNIVERSE below has NOT been confirmed to resolve against a real
tv.get_hist() call. Run this script manually (e.g. `python
scripts/data_sync_engine_indices.py --dry-run`) and check the console output
/ results/indices.json error list before relying on it in the scheduled
workflow.

Usage:
    python data_sync_engine_indices.py             # full run
    python data_sync_engine_indices.py --workers 3 # override worker count
    python data_sync_engine_indices.py --dry-run   # fetch + print, no DB writes

Requirements:
    pip install supabase tvDatafeed pandas numpy tqdm python-dotenv
    (all already in requirements.txt — no new dependencies)

Environment variables (.env.local):
    NEXT_PUBLIC_SUPABASE_URL
    NEXT_PUBLIC_SUPABASE_ANON_KEY   (or SUPABASE_SERVICE_ROLE_KEY)
"""

import os
import sys
import time
import random
import logging
import argparse
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, date
from typing import Optional, List, Dict, Tuple

import pandas as pd
from tqdm import tqdm
from supabase import create_client, Client
from tvDatafeed import TvDatafeed, Interval
from dotenv import load_dotenv
import json
import pathlib

load_dotenv(".env.local")

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = (
    os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    or os.environ.get("SUPABASE_URL", "")
)
SUPABASE_KEY = (
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY", "")
)

UNIVERSE_SCHEMA  = "universe"
UNIVERSE_TABLE   = "indices"
METRICS_SCHEMA   = "screener"
METRICS_TABLE    = "indices_metrics"
DEFAULT_EXCHANGE = "NSE"

N_BARS      = 260    # same buffer as data_sync_engine_n750_d.py: 200-EMA + headroom
EMA_PERIOD  = 200

# ── Starter index universe ──────────────────────────────────────────────────
# UNVERIFIED — see module docstring. symbol/exchange are passed separately to
# fetch_ohlcv_with_retry(), matching tv.get_hist(symbol=..., exchange=...)
# usage in data_sync_engine_n750_d.py — NOT a combined "NSE:NIFTY" string.
INDEX_UNIVERSE: List[dict] = [
    {"ticker": "NIFTY",             "name": "Nifty 50",              "exchange": "NSE"},
    {"ticker": "BANKNIFTY",         "name": "Nifty Bank",            "exchange": "NSE"},
    {"ticker": "NIFTY_FIN_SERVICE", "name": "Nifty Financial Services", "exchange": "NSE"},
    {"ticker": "NIFTYMIDCAP150",    "name": "Nifty Midcap 150",      "exchange": "NSE"},
    {"ticker": "NIFTYSMLCAP250",    "name": "Nifty Smallcap 250",    "exchange": "NSE"},
]

# ── Rate limiting ─────────────────────────────────────────────────────────────
# Same defaults as data_sync_engine_n750_d.py. 4 workers is safe for free
# TradingView accounts; raise only if you have a Pro/Pro+ account.
MAX_WORKERS       = 4
REQUEST_DELAY_SEC = 0.5
REQUEST_JITTER    = 0.3
MAX_RETRIES       = 4        # 1 original + 3 retries
BACKOFF_BASE      = 2.0      # 2s → 4s → 8s → 16s
BACKOFF_MAX       = 60.0

UPSERT_BATCH_SIZE = 50

# Semaphore caps concurrent open WebSocket connections regardless of worker count.
_connection_semaphore: threading.Semaphore = threading.Semaphore(MAX_WORKERS)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# =============================================================================
# Supabase helpers
# =============================================================================

def get_sb() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error(
            "X Missing env vars. Need NEXT_PUBLIC_SUPABASE_URL "
            "and SUPABASE_SERVICE_ROLE_KEY (or NEXT_PUBLIC_SUPABASE_ANON_KEY)."
        )
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def flush_batch(sb: Client, rows: List[dict]) -> None:
    """Upsert a batch of indices_metrics records in a single DB call."""
    if not rows:
        return
    try:
        sb.schema(METRICS_SCHEMA).table(METRICS_TABLE).upsert(rows, on_conflict="ticker").execute()
        log.info("  ^ Flushed %d records to Supabase.", len(rows))
    except Exception as exc:
        log.error(
            "X Batch upsert failed (%d rows): %s: %s",
            len(rows), type(exc).__name__, exc,
        )
        log.debug(traceback.format_exc())


# =============================================================================
# Per-thread TvDatafeed + fetch helper
# (mirrors data_sync_engine_n750_d.py::fetch_ohlcv_with_retry exactly —
#  duplicated here rather than imported, since scripts in this repo are
#  self-contained and data_sync_engine_n750_d.py is not to be modified.)
# =============================================================================

_thread_local = threading.local()

def _get_thread_tv() -> TvDatafeed:
    """Return (or lazily create) a per-thread TvDatafeed instance."""
    if not hasattr(_thread_local, "tv"):
        _thread_local.tv = TvDatafeed()
    return _thread_local.tv


def _is_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "429" in msg or "too many requests" in msg


def _sleep(base: float = REQUEST_DELAY_SEC) -> None:
    time.sleep(base + random.uniform(0, REQUEST_JITTER))


def fetch_ohlcv_with_retry(
    symbol: str,
    exchange: str,
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Fetch daily OHLCV with exponential backoff on 429 / transient errors.
    Uses the semaphore to cap concurrent open WebSocket connections.

    On a 429, the thread-local TvDatafeed instance is discarded and
    recreated on the next attempt to drop the poisoned connection.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with _connection_semaphore:
                tv = _get_thread_tv()
                df = tv.get_hist(
                    symbol   = symbol,
                    exchange = exchange,
                    interval = Interval.in_daily,
                    n_bars   = N_BARS,
                )
                _sleep()   # courtesy gap inside the semaphore

            if df is None or df.empty:
                return None, "empty response"

            df = df.copy()
            df.index = pd.to_datetime(df.index)
            df.sort_index(inplace=True)
            df.columns = [c.lower() for c in df.columns]
            df = df[~df["close"].isna()]
            return df, None

        except Exception as exc:
            is_429 = _is_rate_limit_error(exc)

            if attempt < MAX_RETRIES:
                wait = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
                if is_429:
                    if hasattr(_thread_local, "tv"):
                        del _thread_local.tv
                    log.warning(
                        "429 on %s (attempt %d/%d) -- backing off %.0fs ...",
                        symbol, attempt, MAX_RETRIES, wait,
                    )
                else:
                    log.warning(
                        "Transient error on %s (attempt %d/%d): %s -- retrying in %.0fs ...",
                        symbol, attempt, MAX_RETRIES, type(exc).__name__, wait,
                    )
                time.sleep(wait)
            else:
                log.error(
                    "X Giving up on %s after %d attempts. Last: %s: %s",
                    symbol, MAX_RETRIES, type(exc).__name__, exc,
                )

    return None, f"All {MAX_RETRIES} attempts failed"


def _f(v, d: int = 2) -> Optional[float]:
    try:
        return round(float(v), d) if v is not None and v == v else None
    except Exception:
        return None


def calc_index_metrics(df: pd.DataFrame) -> Dict[str, Optional[object]]:
    """
    latest close, previous close, 1-day % change, EMA200, above_ema200 —
    same style of fields data_sync_engine_n750_d.py computes for stocks.
    """
    empty: Dict[str, Optional[object]] = {
        "price": None, "prev_close": None, "pct_change_1d": None,
        "ema_200": None, "above_ema200": None,
    }
    try:
        closes = df["close"]
        if len(closes) < 2:
            return empty

        latest = float(closes.iloc[-1])
        prev   = float(closes.iloc[-2])
        pct_change_1d = ((latest - prev) / prev * 100) if prev else None

        ema200 = None
        above  = None
        if len(closes) >= EMA_PERIOD:
            ema_series = closes.ewm(span=EMA_PERIOD, adjust=False).mean()
            ema200 = float(ema_series.iloc[-1])
            above  = latest > ema200
        else:
            log.warning(
                "  Only %d bars available (< %d) -- ema_200/above_ema200 will be NULL.",
                len(closes), EMA_PERIOD,
            )

        return {
            "price":         _f(latest),
            "prev_close":    _f(prev),
            "pct_change_1d": _f(pct_change_1d, 4),
            "ema_200":       _f(ema200),
            "above_ema200":  above,
        }
    except Exception as exc:
        log.warning("Index metrics calc error: %s", exc)
        return empty


# =============================================================================
# Worker (runs in thread pool)
# =============================================================================

def process_index(
    ticker: str,
    exchange: str,
    now_utc: str,
) -> Tuple[str, Optional[dict], Optional[str]]:
    """
    Fetch OHLCV, compute metrics, build upsert payload for one index.

    Returns:
        (ticker, payload, error_message)
        payload is None on failure.
    """
    df, err = fetch_ohlcv_with_retry(ticker, exchange)
    if df is None:
        return (ticker, None, err)

    metrics = calc_index_metrics(df)

    payload = {
        "ticker":         ticker,
        "price":          metrics["price"],
        "prev_close":     metrics["prev_close"],
        "pct_change_1d":  metrics["pct_change_1d"],
        "ema_200":        metrics["ema_200"],
        "above_ema200":   metrics["above_ema200"],
        "tv_synced_at":   now_utc,
        "updated_at":     now_utc,
    }

    return (ticker, payload, None)


# =============================================================================
# Main
# =============================================================================

def run_sync(dry_run: bool = False) -> None:
    start_time = time.time()
    sb = None if dry_run else get_sb()

    tickers         = [r["ticker"] for r in INDEX_UNIVERSE]
    ticker_exchange = {r["ticker"]: r.get("exchange", DEFAULT_EXCHANGE) for r in INDEX_UNIVERSE}
    now_utc         = datetime.now(timezone.utc).isoformat()

    log.info("Index universe: %d tickers (UNVERIFIED list -- see module docstring).", len(tickers))

    # ── Upsert universe.indices rows first (static metadata) ──────────────────
    universe_rows = [
        {
            "ticker":     r["ticker"],
            "name":       r["name"],
            "exchange":   r.get("exchange", DEFAULT_EXCHANGE),
            "is_active":  True,
            "updated_at": now_utc,
        }
        for r in INDEX_UNIVERSE
    ]
    if dry_run:
        log.info("[dry-run] Would upsert %d rows to %s.%s:", len(universe_rows), UNIVERSE_SCHEMA, UNIVERSE_TABLE)
        for row in universe_rows:
            log.info("  %s", row)
    else:
        sb.schema(UNIVERSE_SCHEMA).table(UNIVERSE_TABLE).upsert(
            universe_rows, on_conflict="ticker"
        ).execute()
        log.info("Upserted %d rows to %s.", len(universe_rows), UNIVERSE_TABLE)

    # ── Fetch + compute metrics (parallel, same pattern as Phase 2 in n750) ───
    errors:  List[dict] = []
    success = 0
    total   = len(tickers)

    pending_records: List[dict] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                process_index,
                ticker,
                ticker_exchange.get(ticker, DEFAULT_EXCHANGE),
                now_utc,
            ): ticker
            for ticker in tickers
        }

        with tqdm(total=total, desc="Indices sync", unit="index") as pbar:
            for future in as_completed(futures):
                ticker = futures[future]
                pbar.update(1)

                try:
                    result_ticker, payload, err = future.result()

                    if payload is None:
                        errors.append({"ticker": ticker, "reason": err or "unknown"})
                        pbar.set_postfix_str(f"FAIL: {ticker}")
                        continue

                    success += 1
                    pbar.set_postfix_str(
                        f"px={payload['price']}" if payload.get("price") is not None else ticker
                    )

                    if dry_run:
                        log.info("[dry-run] %s", payload)
                    else:
                        pending_records.append(payload)
                        if len(pending_records) >= UPSERT_BATCH_SIZE:
                            flush_batch(sb, pending_records)
                            pending_records.clear()

                except Exception as exc:
                    log.error(
                        "X Unhandled error for %s: %s: %s",
                        ticker, type(exc).__name__, exc,
                    )
                    log.debug(traceback.format_exc())
                    errors.append({"ticker": ticker, "reason": str(exc)})

    # Final flush
    if pending_records and not dry_run:
        flush_batch(sb, pending_records)
        pending_records.clear()

    elapsed = time.time() - start_time

    # ── Report ────────────────────────────────────────────────────────────────
    div = "-" * 62
    print(f"\n{div}")
    print(f"  INDICES SYNC REPORT -- {date.today()}")
    print(div)
    print(f"  Dry run                : {dry_run}")
    print(f"  Succeeded               : {success}/{total}")
    print(f"  Errors                  : {len(errors)}")
    print(f"  Workers                 : {MAX_WORKERS}")
    print(f"  Max retries / index     : {MAX_RETRIES}")
    print(f"  Elapsed time            : {elapsed:.1f}s")
    if errors:
        print(f"\n  Error detail:")
        for e in errors:
            print(f"    {e['ticker']:<20} {str(e['reason'])[:50]}")
    print(f"{div}\n")

    # ── Write result JSON for GitHub Actions summary ──────────────────────────
    pathlib.Path("results").mkdir(exist_ok=True)
    pathlib.Path("results/indices.json").write_text(json.dumps({
        "script":    "Indices Data Sync",
        "succeeded": success,
        "failed":    len(errors),
        "skipped":   0,
        "total":     total,
        "errors":    [e["ticker"] for e in errors[:10]],
    }))


# =============================================================================
# Entry point
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Market Indices -- Data Sync Engine."
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS,
        help=f"Number of parallel workers (default: {MAX_WORKERS}). "
             f"Lower if seeing 429 errors.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print computed rows without writing to Supabase. "
             "Use this first to verify INDEX_UNIVERSE actually resolves.",
    )
    args = parser.parse_args()

    MAX_WORKERS           = args.workers
    _connection_semaphore = threading.Semaphore(MAX_WORKERS)

    run_sync(dry_run=args.dry_run)
