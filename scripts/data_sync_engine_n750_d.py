"""
N750 Momentum Screener — Data Sync Engine
------------------------------------------
Phase 1 — TradingView Screener  (single bulk call, ~10 sec for all 750 stocks)
    Fields fetched : close, EMA200, High.All, first_bar_time, market_cap_basic
    Calculates     : pct_from_ath
    Writes         : price, ema_200, all_time_high, pct_from_ath, market_cap

Phase 2 — tvDatafeed  (per-ticker, parallel, for RS Ratio calculation)
    Fetches        : daily OHLCV for stock + NIFTYTOTALMARKET benchmark
    Calculates     : rs_ratio, rs_ratio_ema200, rs_above_ema200, bars_count
    Resume-safe    : skips tickers whose rs_synced_at is already today
    Parallel       : ThreadPoolExecutor (MAX_WORKERS=4, semaphore-gated)
    Rate limiting  : exponential backoff on 429, per-thread TvDatafeed instances

Usage:
    python data_sync_engine_n750.py             # full run
    python data_sync_engine_n750.py --workers 3 # override worker count

Requirements:
    pip install supabase tvDatafeed tradingview-screener pandas numpy tqdm python-dotenv

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

import numpy as np
import pandas as pd
from tqdm import tqdm
from supabase import create_client, Client
from tvDatafeed import TvDatafeed, Interval
from tradingview_screener import Query, col
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

UNIVERSE_TABLE   = "Total_Market_N750"
METRICS_TABLE    = "stock_metrics"
DEFAULT_EXCHANGE = "NSE"
BENCHMARK_SYMBOL = "NIFTY_TOTAL_MKT"

N_BARS        = 260    # enough for 200-EMA + buffer
RS_EMA_PERIOD = 200

# ── Rate limiting ─────────────────────────────────────────────────────────────
# 4 workers is safe for free TradingView accounts.
# Raise to 6-8 only if you have a Pro/Pro+ account.
MAX_WORKERS       = 4
REQUEST_DELAY_SEC = 0.5
REQUEST_JITTER    = 0.3
MAX_RETRIES       = 4        # 1 original + 3 retries
BACKOFF_BASE      = 2.0      # 2s → 4s → 8s → 16s
BACKOFF_MAX       = 60.0

# ── Batch upsert ──────────────────────────────────────────────────────────────
# Phase 2 accumulates results and flushes every N tickers (1 row per ticker).
UPSERT_BATCH_SIZE = 50

# Semaphore caps concurrent open WebSocket connections regardless of worker count.
_connection_semaphore: threading.Semaphore = threading.Semaphore(MAX_WORKERS)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── TV Screener columns ───────────────────────────────────────────────────────
TV_COLUMNS = [
    "name",             # ticker
    "description",      # company name
    "sector",
    "industry",
    "close",
    "EMA200",
    "High.All",
    "first_bar_time",   # Unix ts of first available daily bar
    "market_cap_basic",
]


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


def fetch_universe(sb: Client) -> List[dict]:
    resp = (
        sb.table(UNIVERSE_TABLE)
        .select("ticker, exchange")
        .eq("is_active", True)
        .execute()
    )
    return resp.data or []


def fetch_rs_sync_dates(sb: Client) -> Dict[str, Optional[str]]:
    """Return {ticker: rs_synced_at} from stock_metrics for resume logic."""
    resp = sb.table(METRICS_TABLE).select("ticker, rs_synced_at").execute()
    return {r["ticker"]: r.get("rs_synced_at") for r in (resp.data or [])}


def flush_phase2_batch(sb: Client, rows: List[dict]) -> None:
    """Upsert a batch of Phase 2 RS records in a single DB call."""
    if not rows:
        return
    try:
        sb.table(METRICS_TABLE).upsert(rows, on_conflict="ticker").execute()
        log.info("  ^ Flushed %d Phase 2 records to Supabase.", len(rows))
    except Exception as exc:
        log.error(
            "X Phase 2 batch upsert failed (%d rows): %s: %s",
            len(rows), type(exc).__name__, exc,
        )
        log.debug(traceback.format_exc())


# =============================================================================
# Resume helper
# =============================================================================

def _synced_today(ts: Optional[str]) -> bool:
    if not ts:
        return False
    try:
        dt = datetime.fromisoformat(ts)
        d  = dt.astimezone(timezone.utc).date() if dt.tzinfo else dt.date()
        return d == date.today()
    except Exception:
        return False


def _sleep(base: float = REQUEST_DELAY_SEC) -> None:
    time.sleep(base + random.uniform(0, REQUEST_JITTER))


# =============================================================================
# PHASE 1 — TV Screener (bulk, ~10 sec for all 750)
# =============================================================================

def phase1_screener(universe_tickers: List[str]) -> Tuple[List[dict], List[dict]]:
    """
    Paginated TV Screener fetch.
    Uses 'name' column for matching (confirmed; 'ticker' column = 0 matches).
    Remaps special chars: TV 'M&M' → Supabase 'M_M'.
    """
    log.info("Phase 1: TV Screener paginated fetch ...")
    all_frames = []
    PAGE_SIZE  = 2000

    for offset in [0, 2000]:
        try:
            _count, df_page = (
                Query()
                .set_markets("india")
                .select(*TV_COLUMNS)
                .where(
                    col("exchange") == "NSE",
                    col("is_primary") == True,
                )
                .limit(PAGE_SIZE)
                .offset(offset)
                .get_scanner_data()
            )
            if df_page.empty:
                break
            all_frames.append(df_page)
            log.info("  Page offset=%d: %d rows.", offset, len(df_page))
        except Exception as exc:
            log.warning("  Page offset=%d failed: %s", offset, exc)
            break

    if not all_frames:
        log.error("TV Screener returned no data.")
        return [], []

    df = pd.concat(all_frames, ignore_index=True).drop_duplicates(subset="name")
    log.info("TV Screener total: %d unique rows.", len(df))

    df["ticker"] = (
        df["name"]
        .str.replace("&", "_", regex=False)
        .str.replace(".", "_", regex=False)
    )

    universe_set = set(universe_tickers)
    df = df[df["ticker"].isin(universe_set)].copy()
    log.info("TV Screener: %d rows matched N750 universe.", len(df))

    payloads_universe: List[dict] = []
    payloads_metrics:  List[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    def _f(v, d: int = 2) -> Optional[float]:
        try:
            return round(float(v), d) if v is not None and v == v else None
        except Exception:
            return None

    for _, row in df.iterrows():
        price = row.get("close")
        ath   = row.get("High.All")
        ticker = row["ticker"]

        pct_from_ath = None
        if price and ath and ath > 0:
            pct_from_ath = round(float(price) / float(ath) - 1, 6)

        mcap_raw = row.get("market_cap_basic")
        market_cap = None
        if mcap_raw is not None and mcap_raw == mcap_raw:
            try:
                market_cap = round(float(mcap_raw) / 1_00_00_000, 2)
            except Exception:
                pass

        payloads_universe.append({
            "ticker":       ticker,
            "company_name": row.get("description"),
            "sector":       row.get("sector"),
            "industry":     row.get("industry"),
            "updated_at":   now,
        })

        # bars_count intentionally excluded — Phase 2 sets it via len(stock_df)
        payloads_metrics.append({
            "ticker":        ticker,
            "price":         _f(price),
            "ema_200":       _f(row.get("EMA200")),
            "all_time_high": _f(ath),
            "pct_from_ath":  pct_from_ath,
            "market_cap":    market_cap,
            "tv_synced_at":  now,
            "updated_at":    now,
        })

    return payloads_universe, payloads_metrics


# =============================================================================
# PHASE 2 — Per-thread TvDatafeed + fetch helpers
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


def calc_rs_ratio_ema(
    stock_df: pd.DataFrame,
    bench_df: pd.DataFrame,
) -> Dict[str, Optional[object]]:
    """
    Ratio Line = stock_close / benchmark_close (daily).
    rs_above_ema200 = True if latest ratio > 200-EMA of ratio.
    """
    empty: Dict[str, Optional[object]] = {
        "rs_ratio": None, "rs_ratio_ema200": None, "rs_above_ema200": None
    }
    try:
        merged = pd.merge(
            stock_df["close"].rename("stock"),
            bench_df["close"].rename("bench"),
            left_index=True, right_index=True, how="inner",
        )
        if len(merged) < RS_EMA_PERIOD:
            return empty

        merged["ratio"]     = merged["stock"] / merged["bench"]
        merged["ratio_ema"] = merged["ratio"].ewm(span=RS_EMA_PERIOD, adjust=False).mean()

        r   = float(merged["ratio"].iloc[-1])
        ema = float(merged["ratio_ema"].iloc[-1])

        return {
            "rs_ratio":        round(r, 8),
            "rs_ratio_ema200": round(ema, 8),
            "rs_above_ema200": r > ema,
        }
    except Exception as exc:
        log.warning("RS calc error for ticker -- %s", exc)
        return empty


# =============================================================================
# Phase 2 worker  (runs in thread pool)
# =============================================================================

def process_ticker_phase2(
    ticker: str,
    exchange: str,
    bench_df: Optional[pd.DataFrame],
    now_utc: str,
) -> Tuple[str, Optional[dict], Optional[str]]:
    """
    Fetch OHLCV, compute RS ratio, build upsert payload for one ticker.

    Returns:
        (ticker, payload, error_message)
        payload is None on failure.
    """
    stock_df, err = fetch_ohlcv_with_retry(ticker, exchange)
    if stock_df is None:
        return (ticker, None, err)

    rs = (
        calc_rs_ratio_ema(stock_df, bench_df)
        if bench_df is not None
        else {"rs_ratio": None, "rs_ratio_ema200": None, "rs_above_ema200": None}
    )

    payload = {
        "ticker":          ticker,
        "bars_count":      len(stock_df),
        "rs_ratio":        rs["rs_ratio"],
        "rs_ratio_ema200": rs["rs_ratio_ema200"],
        "rs_above_ema200": rs["rs_above_ema200"],
        "rs_synced_at":    now_utc,
        "updated_at":      now_utc,
    }

    return (ticker, payload, None)


# =============================================================================
# Main
# =============================================================================

def run_sync() -> None:
    start_time = time.time()
    sb = get_sb()

    log.info("Fetching N750 universe ...")
    universe = fetch_universe(sb)
    if not universe:
        log.error("Universe is empty. Aborting.")
        return

    tickers         = [r["ticker"] for r in universe]
    ticker_exchange = {r["ticker"]: r.get("exchange", DEFAULT_EXCHANGE) for r in universe}
    log.info("Universe: %d active tickers.", len(tickers))

    # ── Phase 1: TV Screener (bulk, no parallelism needed) ────────────────────
    try:
        p1_universe, p1_metrics = phase1_screener(tickers)
    except Exception as exc:
        log.error("Phase 1 failed: %s", exc)
        p1_universe, p1_metrics = [], []

    BATCH = 100

    if p1_universe:
        for i in range(0, len(p1_universe), BATCH):
            sb.table(UNIVERSE_TABLE).upsert(
                p1_universe[i : i + BATCH], on_conflict="ticker"
            ).execute()
        log.info("Phase 1: upserted %d rows to %s.", len(p1_universe), UNIVERSE_TABLE)

    if p1_metrics:
        for i in range(0, len(p1_metrics), BATCH):
            sb.table(METRICS_TABLE).upsert(
                p1_metrics[i : i + BATCH], on_conflict="ticker"
            ).execute()
        log.info("Phase 1: upserted %d rows to %s.", len(p1_metrics), METRICS_TABLE)
    else:
        log.warning("Phase 1: no rows to upsert.")

    # ── Phase 2: tvDatafeed RS Ratio (parallel) ───────────────────────────────
    log.info("Phase 2: RS Ratio calculation (workers=%d) ...", MAX_WORKERS)

    rs_dates   = fetch_rs_sync_dates(sb)
    p2_tickers = [t for t in tickers if not _synced_today(rs_dates.get(t))]
    skipped    = len(tickers) - len(p2_tickers)
    log.info(
        "Phase 2: %d to process, %d skipped (already synced today).",
        len(p2_tickers), skipped,
    )

    # Fetch benchmark once in the main thread before spawning workers.
    # bench_df is read-only after this point — safe to share across threads.
    log.info("Fetching benchmark: %s ...", BENCHMARK_SYMBOL)
    bench_df, bench_err = fetch_ohlcv_with_retry(BENCHMARK_SYMBOL, DEFAULT_EXCHANGE)
    if bench_df is None:
        log.error("Benchmark fetch failed: %s. rs_above_ema200 will be NULL.", bench_err)

    now_utc = datetime.now(timezone.utc).isoformat()

    errors:  List[dict] = []
    success = 0
    total   = len(p2_tickers)

    pending_records: List[dict] = []

    # tqdm wraps the as_completed iterator for a live progress bar.
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                process_ticker_phase2,
                ticker,
                ticker_exchange.get(ticker, DEFAULT_EXCHANGE),
                bench_df,
                now_utc,
            ): ticker
            for ticker in p2_tickers
        }

        with tqdm(total=total, desc="Phase 2 RS Ratio", unit="stock") as pbar:
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
                        f"rs={payload['rs_ratio']:.4f}" if payload.get("rs_ratio") else ticker
                    )

                    pending_records.append(payload)
                    if len(pending_records) >= UPSERT_BATCH_SIZE:
                        flush_phase2_batch(sb, pending_records)
                        pending_records.clear()

                except Exception as exc:
                    log.error(
                        "X Unhandled error for %s: %s: %s",
                        ticker, type(exc).__name__, exc,
                    )
                    log.debug(traceback.format_exc())
                    errors.append({"ticker": ticker, "reason": str(exc)})

    # Final flush
    if pending_records:
        flush_phase2_batch(sb, pending_records)
        pending_records.clear()

    elapsed = time.time() - start_time

    # ── Report ────────────────────────────────────────────────────────────────
    div = "-" * 62
    print(f"\n{div}")
    print(f"  SYNC REPORT -- {date.today()}")
    print(div)
    print(f"  Phase 1 (TV Screener)  : {len(p1_metrics)} rows upserted")
    print(f"  Phase 2 (RS Ratio)     : {success}/{total} succeeded")
    print(f"  Skipped (today)        : {skipped}")
    print(f"  Errors                 : {len(errors)}")
    print(f"  Workers                : {MAX_WORKERS}")
    print(f"  Max retries / ticker   : {MAX_RETRIES}")
    print(f"  Elapsed time           : {elapsed:.1f}s")
    if errors:
        print(f"\n  Error detail:")
        for e in errors[:20]:
            print(f"    {e['ticker']:<16} {str(e['reason'])[:50]}")
        if len(errors) > 20:
            print(f"    ... and {len(errors) - 20} more.")
    print(f"{div}\n")

    # ── Write result JSON for GitHub Actions summary ──────────────────────────
    pathlib.Path("results").mkdir(exist_ok=True)
    pathlib.Path("results/n750.json").write_text(json.dumps({
        "script":    "N750 Data Sync",
        "succeeded": success,
        "failed":    len(errors),
        "skipped":   skipped,
        "total":     len(tickers),
        "errors":    [e["ticker"] for e in errors[:10]],
    }))


# =============================================================================
# Entry point
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="N750 Momentum Screener -- Data Sync Engine."
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS,
        help=f"Number of parallel Phase 2 workers (default: {MAX_WORKERS}). "
             f"Lower if seeing 429 errors.",
    )
    args = parser.parse_args()

    MAX_WORKERS           = args.workers
    _connection_semaphore = threading.Semaphore(MAX_WORKERS)

    run_sync()
