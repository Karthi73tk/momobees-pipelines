#!/usr/bin/env python3
"""
Index Monthly Returns Sync
--------------------------
Fetches daily closes for NIFTY and CNX500 via tvDatafeed and computes
calendar month returns. Upserts the current in-progress month (month-to-date)
daily, and ensures closed months are stored.

Writes:
  public.nifty_monthly_returns
  public.cnx500_monthly_returns

Usage:
  python sync_index_monthly_returns.py
  python sync_index_monthly_returns.py --dry-run
"""

import os
import sys
import time
import random
import logging
import argparse
import traceback
import threading
import json
import pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
from supabase import create_client, Client
from tvDatafeed import TvDatafeed, Interval
from dotenv import load_dotenv

# Try loading env vars
env_path = "/Users/karthik/Documents/Vibe Code 2026/agy/MoMoBees_js-main/MoMoBees_js/.env.local"
load_dotenv(env_path)
load_dotenv()

SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL") or os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY", "")

MAX_WORKERS       = 2
REQUEST_DELAY_SEC = 0.5
REQUEST_JITTER    = 0.3
MAX_RETRIES       = 4
BACKOFF_BASE      = 2.0
BACKOFF_MAX       = 60.0
N_BARS            = 6000  # Pull as much history as possible (tvdatafeed max is ~5000-6000)

_connection_semaphore = threading.Semaphore(MAX_WORKERS)
_thread_local = threading.local()

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

INDICES = [
    {"ticker": "NIFTY", "exchange": "NSE", "table": "nifty_monthly_returns"},
    {"ticker": "CNX500", "exchange": "NSE", "table": "cnx500_monthly_returns"},
]

def _get_thread_tv() -> TvDatafeed:
    if not hasattr(_thread_local, "tv"):
        _thread_local.tv = TvDatafeed()
    return _thread_local.tv

def _is_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "429" in msg or "too many requests" in msg

def _sleep(base: float = REQUEST_DELAY_SEC) -> None:
    time.sleep(base + random.uniform(0, REQUEST_JITTER))

def fetch_ohlcv_with_retry(symbol: str, exchange: str) -> pd.DataFrame:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with _connection_semaphore:
                tv = _get_thread_tv()
                df = tv.get_hist(
                    symbol=symbol,
                    exchange=exchange,
                    interval=Interval.in_daily,
                    n_bars=N_BARS,
                )
                _sleep()

            if df is None or df.empty:
                return None

            df = df.copy()
            df.index = pd.to_datetime(df.index)
            df.sort_index(inplace=True)
            df.columns = [c.lower() for c in df.columns]
            df = df[~df["close"].isna()]
            return df
        except Exception as exc:
            is_429 = _is_rate_limit_error(exc)
            if attempt < MAX_RETRIES:
                wait = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
                if is_429:
                    if hasattr(_thread_local, "tv"):
                        del _thread_local.tv
                    log.warning("429 on %s (attempt %d/%d) -- backing off %.0fs", symbol, attempt, MAX_RETRIES, wait)
                else:
                    log.warning("Transient error on %s (attempt %d/%d): %s -- retrying in %.0fs", symbol, attempt, MAX_RETRIES, exc, wait)
                time.sleep(wait)
            else:
                log.error("Giving up on %s after %d attempts. Last error: %s", symbol, MAX_RETRIES, exc)
    return None

def compute_monthly_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Groups daily data by calendar month and computes return_pct:
    (last trading day's close / prior month's last trading day's close) - 1.
    """
    df_resampled = df['close'].resample('ME').last()
    returns = df_resampled.pct_change()
    
    res = []
    for date, ret in returns.items():
        if pd.isna(ret):
            continue
        res.append({
            "month": date.strftime("%Y-%m"),
            "return_pct": round(float(ret), 6)
        })
    return pd.DataFrame(res)

def get_sb() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("Missing env vars for Supabase")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def sync_index(index_info: dict, dry_run: bool = False):
    ticker = index_info["ticker"]
    exchange = index_info["exchange"]
    table = index_info["table"]
    
    log.info("Fetching history for %s...", ticker)
    df = fetch_ohlcv_with_retry(ticker, exchange)
    if df is None:
        log.error("Failed to fetch data for %s", ticker)
        return
        
    monthly_df = compute_monthly_returns(df)
    if monthly_df.empty:
        log.warning("No monthly returns computed for %s", ticker)
        return
        
    log.info("Computed %d months for %s (from %s to %s)", 
             len(monthly_df), ticker, monthly_df["month"].min(), monthly_df["month"].max())
    
    rows = monthly_df.to_dict(orient="records")
    
    if dry_run:
        log.info("[dry-run] Would upsert %d rows to %s", len(rows), table)
        return rows
        
    sb = get_sb()
    # Batch upsert
    batch_size = 500
    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            sb.table(table).upsert(batch, on_conflict="month").execute()
        log.info("Successfully upserted %d rows to %s", len(rows), table)
    except Exception as exc:
        log.error("Failed to upsert to %s: %s", table, exc)
    
    return rows

def sanity_check_nifty():
    log.info("\n--- STEP 4: Sanity check NIFTY ---")
    sb = get_sb()
    df = fetch_ohlcv_with_retry("NIFTY", "NSE")
    if df is None:
        log.error("Failed to fetch NIFTY")
        return
    
    recomputed = compute_monthly_returns(df)
    
    # Get last 6 fully closed months (excluding current month)
    # The current month is the last one in the resampled dataframe
    current_month_str = datetime.now().strftime("%Y-%m")
    closed_months = recomputed[recomputed["month"] < current_month_str].tail(6)
    
    log.info("Recomputed last 6 closed months:")
    
    discrepancies = []
    
    for _, row in closed_months.iterrows():
        month = row["month"]
        ret_recomp = row["return_pct"]
        
        # Fetch existing
        res = sb.table("nifty_monthly_returns").select("return_pct").eq("month", month).execute()
        if not res.data:
            log.warning("Month %s not found in Supabase nifty_monthly_returns!", month)
            continue
            
        ret_db = res.data[0]["return_pct"]
        diff = abs(ret_recomp - ret_db) * 100 # in percentage points
        
        log.info("Month %s: Recomputed = %.6f, DB = %.6f, diff = %.4f pp", month, ret_recomp, ret_db, diff)
        
        if diff > 0.1:
            discrepancies.append((month, ret_recomp, ret_db, diff))
            
    if discrepancies:
        log.warning("Found discrepancies > 0.1 pp:")
        for d in discrepancies:
            log.warning("  %s: recomputed %f, db %f, diff %.4f pp", *d)
    else:
        log.info("All last 6 closed months match closely (diff <= 0.1 pp)!")

def report_cnx500():
    log.info("\n--- STEP 5: Report CNX500 ---")
    df = fetch_ohlcv_with_retry("CNX500", "NSE")
    if df is None:
        log.error("Failed to fetch CNX500")
        return
        
    monthly_df = compute_monthly_returns(df)
    log.info("row count: %d", len(monthly_df))
    log.info("min month: %s", monthly_df["month"].min())
    log.info("max month: %s", monthly_df["month"].max())
    
    log.info("\n2015-01 through 2015-12 values:")
    y2015 = monthly_df[monthly_df["month"].str.startswith("2015-")]
    for _, row in y2015.iterrows():
        log.info("  %s: %.6f", row["month"], row["return_pct"])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args()
    
    if args.report:
        sanity_check_nifty()
        report_cnx500()
    else:
        success = 0
        failed = 0
        for idx in INDICES:
            try:
                res = sync_index(idx, dry_run=args.dry_run)
                if res is not None:
                    success += 1
                else:
                    failed += 1
            except Exception as exc:
                log.error("Error syncing %s: %s", idx['ticker'], exc)
                failed += 1
                
        if not args.dry_run:
            pathlib.Path("results").mkdir(exist_ok=True)
            pathlib.Path("results/index_monthly_returns.json").write_text(json.dumps({
                "script":    "Index Monthly Returns Sync",
                "succeeded": success,
                "failed":    failed,
                "skipped":   0,
                "total":     len(INDICES),
                "errors":    [],
            }))
