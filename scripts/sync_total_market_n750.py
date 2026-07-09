"""
Total_Market_N750 — Universe Sync Pipeline
--------------------------------------------
Rebuilds the `universe.n750` table from the official Nifty Total Market
index constituent list, enriched with sector/industry from TradingView, and
validated against tvDatafeed (only tickers that actually return live data
get upserted).

Flow:
    1. Download constituent CSV from niftyindices.com
         -> ticker, company_name, exchange
    2. Fetch ALL NSE primary symbols from `tradingview_screener`
         -> ticker (remapped), sector, industry
    3. Join (1) and (2) on the remapped ticker
    4. Validate every ticker against `tvDatafeed` (parallel, retry w/ backoff)
         -> fetch latest close only; ticker is only kept if this succeeds
    5. Upsert PASSING tickers only into Supabase `universe.n750`
         (optionally delete rows for tickers no longer in the index)

NOTE ON THE CSV: niftyindices.com columns have shifted over the years between
runs of "Company Name,Industry,Symbol,Series,ISIN Code" and similar variants.
This script auto-detects the Symbol / Company Name columns by fuzzy header
matching so it doesn't silently break if a column is renamed -- but you
should still eyeball the printed column list on first run.

Usage:
    python sync_total_market_n750.py                 # full run: fetch + validate + upsert
    python sync_total_market_n750.py --dry-run        # fetch + validate + print/save, no DB writes
    python sync_total_market_n750.py --save out.csv   # also save the final result to CSV
    python sync_total_market_n750.py --skip-validation
        # skip the tvDatafeed check entirely (upsert everything that has a TV sector match)
    python sync_total_market_n750.py --workers 6
        # override tvDatafeed parallel worker count (default: 4)
    python sync_total_market_n750.py --remove-missing
        # additionally delete rows for tickers currently in the DB but no
        # longer present in the fresh constituent list

Requirements:
    pip install supabase tradingview-screener tvDatafeed pandas requests tqdm python-dotenv
"""

import argparse
import io
import json
import os
import pathlib
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv(".env.local")

# ── Config ────────────────────────────────────────────────────────────────────
NIFTY_CSV_URL = "https://www.niftyindices.com/IndexConstituent/ind_niftytotalmarket_list.csv"
DEFAULT_EXCHANGE = "NSE"
UNIVERSE_SCHEMA = "universe"
UNIVERSE_TABLE = "n750"
BATCH_SIZE = 100

SUPABASE_URL = (
    os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    or os.environ.get("SUPABASE_URL", "")
)
SUPABASE_KEY = (
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY", "")
)

# niftyindices.com blocks requests without a browser-like User-Agent.
CSV_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/csv,*/*",
}

# ── tvDatafeed validation config ──────────────────────────────────────────────
# Same shape as Phase 2 of data_sync_engine_n750_d.py: per-thread TvDatafeed
# instances, semaphore-gated concurrency, exponential backoff on 429s.
MAX_WORKERS = 4
REQUEST_DELAY_SEC = 0.5
REQUEST_JITTER = 0.3
MAX_RETRIES = 4          # 1 original + 3 retries
BACKOFF_BASE = 2.0        # 2s -> 4s -> 8s -> 16s
BACKOFF_MAX = 60.0

_connection_semaphore = threading.Semaphore(MAX_WORKERS)
_thread_local = threading.local()


# =============================================================================
# Step 1 — Download Nifty Total Market constituents
# =============================================================================

def clean_symbol(raw: str) -> str:
    """
    TradingView symbols don't support '-' or '&'.
    Replace both (and '.', for tickers like 'M.M.FIN') with '_' so the
    constituent list and TradingView's symbol set use the same key.
    """
    if raw is None:
        return raw
    return (
        str(raw).strip()
        .replace("&", "_")
        .replace("-", "_")
        .replace(".", "_")
    )


def fetch_nifty_constituents() -> pd.DataFrame:
    print(f"Downloading Nifty Total Market constituents from {NIFTY_CSV_URL} ...")
    resp = requests.get(NIFTY_CSV_URL, headers=CSV_HEADERS, timeout=30)
    resp.raise_for_status()

    df = pd.read_csv(io.StringIO(resp.text))
    df.columns = [c.strip() for c in df.columns]
    print(f"  Downloaded {len(df)} rows. Columns: {list(df.columns)}")

    # Auto-detect the relevant columns (header names have varied over time).
    def _find_col(candidates: List[str]) -> Optional[str]:
        lower_map = {c.lower(): c for c in df.columns}
        for cand in candidates:
            if cand.lower() in lower_map:
                return lower_map[cand.lower()]
        return None

    symbol_col = _find_col(["Symbol", "Ticker"])
    name_col = _find_col(["Company Name", "CompanyName", "Company"])

    if not symbol_col or not name_col:
        print("X Could not auto-detect Symbol/Company Name columns.")
        print(f"  Found columns: {list(df.columns)}")
        sys.exit(1)

    out = pd.DataFrame({
        "raw_symbol": df[symbol_col].astype(str).str.strip(),
        "company_name": df[name_col].astype(str).str.strip(),
    })
    out["ticker"] = out["raw_symbol"].apply(clean_symbol)
    out["exchange"] = DEFAULT_EXCHANGE
    out = out.drop_duplicates(subset="ticker").reset_index(drop=True)

    print(f"  Parsed {len(out)} unique constituents after ticker cleanup.")
    return out[["ticker", "raw_symbol", "company_name", "exchange"]]


# =============================================================================
# Step 2 — Fetch sector/industry for ALL NSE symbols from TradingView
# =============================================================================

def fetch_tv_sector_industry() -> pd.DataFrame:
    from tradingview_screener import Query, col

    print("Fetching sector/industry for all NSE symbols from TradingView Screener ...")
    all_frames = []
    PAGE_SIZE = 2000

    for offset in [0, 2000, 4000]:
        try:
            _count, df_page = (
                Query()
                .set_markets("india")
                .select("name", "sector", "industry")
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
            print(f"  Page offset={offset}: {len(df_page)} rows.")
        except Exception as exc:
            print(f"  Page offset={offset} failed: {exc}")
            break

    if not all_frames:
        print("X TradingView Screener returned no data.")
        return pd.DataFrame(columns=["ticker", "sector", "industry"])

    tv_df = pd.concat(all_frames, ignore_index=True).drop_duplicates(subset="name")
    tv_df["ticker"] = tv_df["name"].apply(clean_symbol)
    print(f"  TradingView total: {len(tv_df)} unique NSE symbols.")

    return tv_df[["ticker", "sector", "industry"]]


# =============================================================================
# Step 3 — Validate each ticker against tvDatafeed (parallel, with retry)
# =============================================================================

def _get_thread_tv():
    from tvDatafeed import TvDatafeed
    if not hasattr(_thread_local, "tv"):
        _thread_local.tv = TvDatafeed()
    return _thread_local.tv


def _is_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "429" in msg or "too many requests" in msg


def _sleep(base: float = REQUEST_DELAY_SEC) -> None:
    time.sleep(base + random.uniform(0, REQUEST_JITTER))


def fetch_latest_close_with_retry(ticker: str, exchange: str) -> Tuple[Optional[float], Optional[str]]:
    """
    Fetch just the latest daily close for one ticker via tvDatafeed, with
    exponential backoff on 429s. Returns (latest_close, error_message).
    A ticker "passes" validation only if this returns a non-None close.
    """
    from tvDatafeed import Interval

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with _connection_semaphore:
                tv = _get_thread_tv()
                df = tv.get_hist(
                    symbol=ticker,
                    exchange=exchange,
                    interval=Interval.in_daily,
                    n_bars=2,   # just need the latest close; 2 bars avoids edge cases
                )
                _sleep()

            if df is None or df.empty:
                return None, "empty response"

            df = df.copy()
            df.columns = [c.lower() for c in df.columns]
            df = df[~df["close"].isna()]
            if df.empty:
                return None, "no valid close price"

            latest_close = float(df["close"].iloc[-1])
            return latest_close, None

        except Exception as exc:
            is_429 = _is_rate_limit_error(exc)

            # Discard the thread-local connection on ANY failure, not just
            # 429s. tvDatafeed's underlying websocket also dies from plain
            # "Connection to remote host was lost" / "Connection timed out"
            # errors, and reusing a dead socket on retry just fails again --
            # this was the actual cause of "valid ticker, still failed".
            if hasattr(_thread_local, "tv"):
                del _thread_local.tv

            if attempt < MAX_RETRIES:
                wait = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
                time.sleep(wait)
            else:
                return None, f"{type(exc).__name__}: {exc}"

    return None, f"All {MAX_RETRIES} attempts failed"


def validate_tickers(merged: pd.DataFrame, workers: int, retry_sweeps: int = 2) -> Tuple[pd.DataFrame, List[dict]]:
    """
    Runs fetch_latest_close_with_retry() for every ticker in parallel.
    Returns (passing_df, failures) where passing_df is the subset of `merged`
    whose tvDatafeed fetch succeeded, annotated with a `latest_close` column.

    After the main parallel pass, any failures get `retry_sweeps` additional
    passes at reduced concurrency (max(1, workers // 2)) and a longer courtesy
    delay. Most "empty response" failures are transient connection drops
    (common under an anonymous/nologin tvDatafeed session, especially at
    higher worker counts) rather than genuinely bad symbols -- a slower,
    lower-concurrency retry clears the vast majority of them.
    """
    global _connection_semaphore

    def _run_pass(tickers_df: pd.DataFrame, pass_workers: int, extra_delay: float = 0.0) -> Dict[str, Tuple[Optional[float], Optional[str]]]:
        _connection_semaphore = threading.Semaphore(pass_workers)
        results: Dict[str, Tuple[Optional[float], Optional[str]]] = {}

        with ThreadPoolExecutor(max_workers=pass_workers) as executor:
            futures = {
                executor.submit(fetch_latest_close_with_retry, row["ticker"], row["exchange"]): row["ticker"]
                for _, row in tickers_df.iterrows()
            }
            with tqdm(total=len(futures), desc=f"Validating (workers={pass_workers})", unit="stock") as pbar:
                for future in as_completed(futures):
                    ticker = futures[future]
                    pbar.update(1)
                    try:
                        close, err = future.result()
                    except Exception as exc:
                        close, err = None, f"Unhandled: {exc}"
                    results[ticker] = (close, err)
                    pbar.set_postfix_str(ticker if close is None else f"OK {ticker}")
                    if extra_delay:
                        time.sleep(extra_delay)

        return results

    print(f"\nValidating {len(merged)} tickers against tvDatafeed (workers={workers}) ...")
    results = _run_pass(merged, workers)

    remaining = merged[merged["ticker"].map(lambda t: results[t][0] is None)]
    for sweep in range(1, retry_sweeps + 1):
        if remaining.empty:
            break
        sweep_workers = max(1, workers // 2)
        print(f"\nRetry sweep {sweep}/{retry_sweeps}: re-checking {len(remaining)} failed tickers "
              f"(workers={sweep_workers}, likely transient connection drops) ...")
        sweep_results = _run_pass(remaining, sweep_workers, extra_delay=0.5)
        results.update(sweep_results)
        remaining = merged[merged["ticker"].map(lambda t: results[t][0] is None)]

    merged = merged.copy()
    merged["latest_close"] = merged["ticker"].map(lambda t: results[t][0])
    merged["_validation_error"] = merged["ticker"].map(lambda t: results[t][1])

    passing = merged[merged["latest_close"].notna()].drop(columns=["_validation_error"]).reset_index(drop=True)
    failing = merged[merged["latest_close"].isna()]

    failures = [
        {"ticker": row["ticker"], "reason": row["_validation_error"]}
        for _, row in failing.iterrows()
    ]

    print(f"\n  Final result -- Passed: {len(passing)}/{len(merged)}   Failed: {len(failures)}/{len(merged)}")
    if failures:
        print("  Failed tickers after all retry sweeps (first 30) -- worth checking manually:")
        for f in failures[:30]:
            print(f"    {f['ticker']:<20} {str(f['reason'])[:60]}")
        if len(failures) > 30:
            print(f"    ... and {len(failures) - 30} more.")

    return passing, failures


# =============================================================================
# Step 4 — Merge
# =============================================================================

def build_universe(nifty_df: pd.DataFrame, tv_df: pd.DataFrame) -> pd.DataFrame:
    merged = nifty_df.merge(tv_df, on="ticker", how="left")

    unmatched = merged[merged["sector"].isna()]
    if not unmatched.empty:
        print(f"\nWarning: {len(unmatched)} constituents had no TradingView sector/industry match:")
        preview_cols = ["ticker", "raw_symbol", "company_name"]
        with pd.option_context("display.max_rows", 30, "display.width", 150):
            print(unmatched[preview_cols].head(30).to_string(index=False))
        if len(unmatched) > 30:
            print(f"  ... and {len(unmatched) - 30} more.")

    return merged


# =============================================================================
# Step 5 — Upsert into Supabase (validated tickers only)
# =============================================================================

def get_sb():
    from supabase import create_client
    if not SUPABASE_URL or not SUPABASE_KEY:
        print(
            "X Missing env vars. Need NEXT_PUBLIC_SUPABASE_URL and "
            "SUPABASE_SERVICE_ROLE_KEY (or NEXT_PUBLIC_SUPABASE_ANON_KEY)."
        )
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def upsert_universe(sb, passing: pd.DataFrame) -> None:
    """Upserts ONLY the rows that passed tvDatafeed validation (or all rows,
    if validation was skipped via --skip-validation)."""
    now = datetime.now(timezone.utc).isoformat()

    payload = [
        {
            "ticker": row["ticker"],
            "company_name": row["company_name"],
            "exchange": row["exchange"],
            "sector": None if pd.isna(row["sector"]) else row["sector"],
            "industry": None if pd.isna(row["industry"]) else row["industry"],
            "is_active": True,
            "updated_at": now,
        }
        for _, row in passing.iterrows()
    ]

    print(f"\nUpserting {len(payload)} validated rows into {UNIVERSE_SCHEMA}.{UNIVERSE_TABLE} ...")
    for i in range(0, len(payload), BATCH_SIZE):
        batch = payload[i : i + BATCH_SIZE]
        sb.schema(UNIVERSE_SCHEMA).table(UNIVERSE_TABLE).upsert(batch, on_conflict="ticker").execute()
    print(f"  Done. Upserted {len(payload)} rows.")


def remove_missing(sb, index_df: pd.DataFrame) -> int:
    """
    Delete rows from the DB for tickers that are no longer present in the
    fresh Nifty Total Market constituent list (i.e. hard-remove index
    rebalance dropouts, rather than flagging them with is_active=False).

    IMPORTANT: `index_df` should be the full CSV-matched universe (`merged`),
    NOT the tvDatafeed-validated subset (`passing`). A ticker that's still a
    genuine index constituent but happened to fail tvDatafeed validation this
    run (e.g. a transient connection drop) must NOT be removed -- it's still
    real, it just didn't get its price/sector refreshed this run and will
    very likely succeed next run. Only true index rebalance removals (ticker
    no longer in the CSV at all) should be deleted.

    Returns the number of tickers removed.
    """
    fresh_tickers = set(index_df["ticker"])

    resp = sb.schema(UNIVERSE_SCHEMA).table(UNIVERSE_TABLE).select("ticker").execute()
    current_tickers = {r["ticker"] for r in (resp.data or [])}

    to_remove = current_tickers - fresh_tickers
    if not to_remove:
        print("No tickers to remove -- DB ticker set matches fresh list.")
        return 0

    print(f"Removing {len(to_remove)} tickers no longer in the constituent list ...")
    tickers_list = list(to_remove)
    for i in range(0, len(tickers_list), BATCH_SIZE):
        batch = tickers_list[i : i + BATCH_SIZE]
        sb.schema(UNIVERSE_SCHEMA).table(UNIVERSE_TABLE).delete().in_("ticker", batch).execute()
    print(f"  Done. Removed {len(to_remove)} tickers.")
    print(f"  Tickers: {sorted(to_remove)[:20]}{' ...' if len(to_remove) > 20 else ''}")
    return len(to_remove)


def _write_result(succeeded: int, failed: int, skipped: int, total: int, errors: list) -> None:
    pathlib.Path("results").mkdir(exist_ok=True)
    pathlib.Path("results/universe_sync.json").write_text(json.dumps({
        "script":    "Universe Sync (N750)",
        "succeeded": succeeded,
        "failed":    failed,
        "skipped":   skipped,
        "total":     total,
        "errors":    errors,
    }))


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Sync universe.n750 from Nifty Total Market index.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print/save only -- no DB writes.")
    parser.add_argument("--save", type=str, default=None, help="Optional CSV path to save the merged result.")
    parser.add_argument(
        "--remove-missing",
        action="store_true",
        help="Delete rows for tickers no longer present in the fresh CSV constituent list "
             "(index rebalance removals) -- independent of tvDatafeed validation outcome.",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip the tvDatafeed live-fetch check entirely (upsert everything with a TV sector match).",
    )
    parser.add_argument(
        "--workers", type=int, default=MAX_WORKERS,
        help=f"Parallel tvDatafeed validation workers (default: {MAX_WORKERS}). Lower if seeing 429 errors.",
    )
    parser.add_argument(
        "--retry-sweeps", type=int, default=2,
        help="Extra low-concurrency retry passes for tickers that failed validation (default: 2).",
    )
    args = parser.parse_args()

    nifty_df = fetch_nifty_constituents()
    tv_df = fetch_tv_sector_industry()
    merged = build_universe(nifty_df, tv_df)

    print(f"\nMerged universe size (pre-validation): {len(merged)} rows.")
    with pd.option_context("display.max_columns", None, "display.width", 200):
        print(merged.head(15).to_string(index=False))

    if args.skip_validation:
        print("\n--skip-validation set: skipping tvDatafeed check.")
        passing = merged
        failures: List[dict] = []
    else:
        passing, failures = validate_tickers(merged, workers=args.workers, retry_sweeps=args.retry_sweeps)

    print(f"\nFinal (post-validation) universe size: {len(passing)} rows.")

    if args.save:
        passing.to_csv(args.save, index=False)
        print(f"Saved final result to: {args.save}")
        if failures:
            fail_path = args.save.rsplit(".", 1)[0] + "_failed.csv"
            pd.DataFrame(failures).to_csv(fail_path, index=False)
            print(f"Saved failed-ticker detail to: {fail_path}")

    if args.dry_run:
        print("\n--dry-run set: skipping database writes.")
        return

    sb = get_sb()
    upsert_universe(sb, passing)

    removed_count = 0
    if args.remove_missing:
        removed_count = remove_missing(sb, merged)

    # ── Write result JSON for GitHub Actions summary ──────────────────────────
    errors = [f["ticker"] for f in failures[:10]]
    if removed_count:
        errors = errors + [f"(removed {removed_count} delisted tickers)"]
    _write_result(
        succeeded=len(passing),
        failed=len(failures),
        skipped=0,
        total=len(merged),
        errors=errors[:10],
    )


if __name__ == "__main__":
    main()
