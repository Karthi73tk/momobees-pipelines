"""
stage2_data_lib.py
==================
Self-contained, side-effect-free data fetching helpers for Stage 2 expansion
and weekly watchlist screens (Supabase universe, stage history, and tvDatafeed weekly bars).

Importing this module causes ZERO side effects (no logging.basicConfig, no load_dotenv).
"""

import logging
import os
import sys
import threading
import time
from collections import defaultdict
from typing import Dict, List, Optional

import pandas as pd

log = logging.getLogger(__name__)

# ── Supabase config ───────────────────────────────────────────────────────────
UNIVERSE_SCHEMA = "universe"
UNIVERSE_TABLE = "nse_universe"
UNIVERSE_MARKET_CAP_COLUMN = "market_cap"
STAGES_SCHEMA = "stage"
STAGES_TABLE = "weekly_stock_stages"

# ── Universe & history constants ──────────────────────────────────────────────
EPOCH_HISTORY_WEEKS = 104     # Weeks of weekly_stock_stages to pull per ticker
WEEKLY_SMA_WARMUP_BARS = 15   # Extra weekly bars fetched before epoch start

# ── Parallelism & rate limiting ───────────────────────────────────────────────
MAX_WORKERS = 4
REQUEST_DELAY = 0.5
MAX_RETRIES = 4
BACKOFF_BASE = 2.0
BACKOFF_MAX = 60.0

_thread_local = threading.local()
_connection_semaphore = threading.Semaphore(MAX_WORKERS)


def get_supabase_client():
    """Create and return a Supabase client using environment variables."""
    from supabase import create_client

    url = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    if not url or not key:
        raise RuntimeError("Missing NEXT_PUBLIC_SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY in .env.local")
    return create_client(url, key)


def fetch_universe(supabase) -> Dict[str, dict]:
    """Active tickers with price/market_cap, keyed by ticker."""
    out: Dict[str, dict] = {}
    page, page_size = 0, 1000
    while True:
        resp = (
            supabase.schema(UNIVERSE_SCHEMA).table(UNIVERSE_TABLE)
            .select(f"ticker, price, {UNIVERSE_MARKET_CAP_COLUMN}, is_active")
            .eq("is_active", True)
            .range(page * page_size, (page + 1) * page_size - 1)
            .execute()
        )
        rows = resp.data or []
        for r in rows:
            out[r["ticker"]] = r
        if len(rows) < page_size:
            break
        page += 1
    log.info("Fetched %d active tickers from %s.%s.", len(out), UNIVERSE_SCHEMA, UNIVERSE_TABLE)
    return out


def get_target_date(supabase, lookback_weeks: int) -> str:
    """Latest analysis_date in weekly_stock_stages, walked back N snapshots."""
    seen: List[str] = []
    page, page_size = 0, 2000
    while len(seen) <= max(lookback_weeks, 1):
        resp = (
            supabase.schema(STAGES_SCHEMA).table(STAGES_TABLE)
            .select("analysis_date")
            .order("analysis_date", desc=True)
            .range(page * page_size, (page + 1) * page_size - 1)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            break
        for r in rows:
            d = r["analysis_date"]
            if d not in seen:
                seen.append(d)
        if len(rows) < page_size:
            break
        page += 1

    if not seen:
        raise RuntimeError(f"No rows found in {STAGES_TABLE}. Run the weekly stage pipeline first.")
    if lookback_weeks >= len(seen):
        raise ValueError(f"--lookback-weeks {lookback_weeks} goes further back than the "
                         f"{len(seen)} distinct analysis_date(s) available.")
    return seen[lookback_weeks]


def fetch_stage_history(supabase, cutoff_date: str, target_date: str) -> Dict[str, List[dict]]:
    """weekly_stock_stages rows for every ticker in [cutoff_date, target_date],
    oldest -> newest. Nothing after target_date is fetched (honest replay)."""
    history: Dict[str, List[dict]] = defaultdict(list)
    page, page_size = 0, 1000
    while True:
        resp = (
            supabase.schema(STAGES_SCHEMA).table(STAGES_TABLE)
            .select("ticker, analysis_date, close, volume, stage")
            .gte("analysis_date", cutoff_date)
            .lte("analysis_date", target_date)
            .order("ticker")
            .order("analysis_date")
            .range(page * page_size, (page + 1) * page_size - 1)
            .execute()
        )
        rows = resp.data or []
        for r in rows:
            history[r["ticker"]].append(r)
        if len(rows) < page_size:
            break
        page += 1

    for ticker in history:
        history[ticker].sort(key=lambda r: r["analysis_date"])
    return history


# ── tvDatafeed WEEKLY fetch ───────────────────────────────────────────────────
def _get_thread_tv():
    from tvDatafeed import TvDatafeed
    if not hasattr(_thread_local, "tv"):
        username = os.environ.get("TV_USERNAME")
        password = os.environ.get("TV_PASSWORD")
        _thread_local.tv = TvDatafeed(username=username, password=password) if username and password else TvDatafeed()
    return _thread_local.tv


def _is_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "429" in msg or "too many requests" in msg


def fetch_weekly_bars_with_retry(ticker: str, n_bars: int) -> Optional[pd.DataFrame]:
    """Fetch weekly OHLCV bars for ticker from tvDatafeed with retry/backoff."""
    from tvDatafeed import Interval

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with _connection_semaphore:
                time.sleep(REQUEST_DELAY)
                tv = _get_thread_tv()
                df = tv.get_hist(symbol=ticker, exchange="NSE", interval=Interval.in_weekly, n_bars=n_bars)
            if df is not None and not df.empty:
                df.columns = [c.lower() for c in df.columns]
                return df.sort_index()
            if attempt < MAX_RETRIES:
                wait = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
                log.warning("    %s: empty response (attempt %d/%d) -- retrying in %.0fs", ticker, attempt, MAX_RETRIES, wait)
                time.sleep(wait)
        except Exception as exc:
            is_429 = _is_rate_limit_error(exc)
            if attempt < MAX_RETRIES:
                wait = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
                if is_429 and hasattr(_thread_local, "tv"):
                    del _thread_local.tv
                log.warning("    %s: %s (attempt %d/%d) -- retrying in %.0fs", ticker, type(exc).__name__, attempt, MAX_RETRIES, wait)
                time.sleep(wait)
            else:
                log.error("    %s: giving up after %d attempts (%s: %s)", ticker, MAX_RETRIES, type(exc).__name__, exc)
    return None
