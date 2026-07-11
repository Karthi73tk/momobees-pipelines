"""
Universe Sync Pipeline (v2 -- simplified)
--------------------------------------------
Rebuilds `universe.n750` directly: one row per active stock, with an
`indices` text[] column recording which Nifty tier it belongs to (exactly
one of NIFTY50 / NIFTYNEXT50 / MIDCAP150 / SMALLCAP250 / MICROCAP250) plus
an optional 'FNO' tag. `universe.n500` is a VIEW (n750 minus MICROCAP250) --
nothing to sync there separately.

Only active stocks are stored: a ticker that drops out of every tier list
gets DELETED from n750, not flagged inactive.

Flow:
    1. Download the 5 Nifty tier CSVs -> assign each ticker its (single) tier
    2. Cross-check: union of the 5 tiers vs directly-fetched NIFTY500 UNION
       MICROCAP250 CSVs (informational only)
    3. Download & parse the NSE F&O market-lot CSV -> tag matching tickers 'FNO'
    4. Attach sector/industry: prefer `universe.nse_universe` (DB), fall back
       to a live TradingView Screener pull
    5. (optional, --validate) validate tickers against tvDatafeed
    6. Upsert into `universe.n750`
    7. DELETE rows for tickers no longer in the fresh union (index rebalance
       dropouts) -- default behavior, since only actives are stored

Usage:
    python sync_universe.py                   # full run: fetch + upsert + prune
    python sync_universe.py --dry-run          # fetch + print/save, no DB writes/deletes
    python sync_universe.py --save out.csv     # also save the result to CSV
    python sync_universe.py --validate         # validate tickers via tvDatafeed (slow)
    python sync_universe.py --workers 6        # tvDatafeed parallel workers (default: 4)
    python sync_universe.py --skip-fno         # skip the F&O tagging
    python sync_universe.py --keep-orphans     # don't delete dropped-out tickers
    python sync_universe.py --no-legacy-columns
        # omit exchange/is_active from upserts after those additive-migration
        # compatibility columns have been dropped from universe.n750

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
DEFAULT_EXCHANGE = "NSE"
UNIVERSE_SCHEMA = "universe"
N750_TABLE = "n750"
NSE_UNIVERSE_TABLE = "nse_universe"
BATCH_SIZE = 100
RESULT_PATH = "results/universe_sync.json"

# Tiers are mutually exclusive -- each ticker belongs to exactly one. Order
# matters only for the (rare) duplicate-warning case.
TIER_URLS: Dict[str, str] = {
    "NIFTY50": "https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv",
    "NIFTYNEXT50": "https://www.niftyindices.com/IndexConstituent/ind_niftynext50list.csv",
    "MIDCAP150": "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv",
    "SMALLCAP250": "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv",
    "MICROCAP250": "https://www.niftyindices.com/IndexConstituent/ind_niftymicrocap250_list.csv",
}
NIFTY500_URL = "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"  # cross-check only
BASE500_TIERS = ["NIFTY50", "NIFTYNEXT50", "MIDCAP150", "SMALLCAP250"]

FNO_URL = "https://nsearchives.nseindia.com/content/fo/fo_mktlots.csv"
# NSE occasionally rotates the exact archive path. If this 404s, check
# https://www.nseindia.com/all-reports-derivatives for the current link.

SUPABASE_URL = (
    os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    or os.environ.get("SUPABASE_URL", "")
)
SUPABASE_KEY = (
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY", "")
)

CSV_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/csv,*/*",
}

MAX_WORKERS = 4
REQUEST_DELAY_SEC = 0.5
REQUEST_JITTER = 0.3
MAX_RETRIES = 4
BACKOFF_BASE = 2.0
BACKOFF_MAX = 60.0

_connection_semaphore = threading.Semaphore(MAX_WORKERS)
_thread_local = threading.local()


# =============================================================================
# Shared helpers
# =============================================================================

def clean_symbol(raw: str) -> str:
    if raw is None:
        return raw
    return str(raw).strip().replace("&", "_").replace("-", "_").replace(".", "_")


def _find_col(columns: List[str], candidates: List[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


# =============================================================================
# Step 1 — Download the 5 tier CSVs + assign each ticker its tier
# =============================================================================

def fetch_index_csv(label: str, url: str) -> pd.DataFrame:
    print(f"[{label}] Downloading from {url} ...")
    resp = requests.get(url, headers=CSV_HEADERS, timeout=30)
    resp.raise_for_status()

    df = pd.read_csv(io.StringIO(resp.text))
    df.columns = [c.strip() for c in df.columns]

    symbol_col = _find_col(df.columns, ["Symbol", "Ticker"])
    name_col = _find_col(df.columns, ["Company Name", "CompanyName", "Company"])
    if not symbol_col or not name_col:
        print(f"  X [{label}] Could not auto-detect Symbol/Company Name columns: {list(df.columns)}")
        sys.exit(1)

    out = pd.DataFrame({
        "raw_symbol": df[symbol_col].astype(str).str.strip(),
        "company_name": df[name_col].astype(str).str.strip(),
    })
    out["ticker"] = out["raw_symbol"].apply(clean_symbol)
    out = out.drop_duplicates(subset="ticker").reset_index(drop=True)
    print(f"  [{label}] {len(out)} unique constituents.")
    return out[["ticker", "company_name"]]


def build_tier_map(tier_frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """One row per ticker with its single tier tag. Warns (does not fail) on
    the rare case a ticker appears in more than one tier CSV -- keeps the
    first tier encountered in TIER_URLS order."""
    seen: Dict[str, str] = {}
    rows = []
    duplicates = []

    for tier, df in tier_frames.items():
        for _, row in df.iterrows():
            t = row["ticker"]
            if t in seen:
                duplicates.append((t, seen[t], tier))
                continue
            seen[t] = tier
            rows.append({"ticker": t, "company_name": row["company_name"], "tier": tier})

    if duplicates:
        print(f"\nWarning: {len(duplicates)} tickers appeared in more than one tier list "
              f"(kept first, ignored later): {duplicates[:10]}")

    return pd.DataFrame(rows)


def cross_check_totalmarket(tier_frames: Dict[str, pd.DataFrame]) -> None:
    """Informational only: union(5 tiers) should equal NIFTY500 UNION
    MICROCAP250, i.e. NIFTY500 should equal union(the 4 base tiers)."""
    print(f"Cross-checking against {NIFTY500_URL} ...")
    resp = requests.get(NIFTY500_URL, headers=CSV_HEADERS, timeout=30)
    resp.raise_for_status()
    df500 = pd.read_csv(io.StringIO(resp.text))
    df500.columns = [c.strip() for c in df500.columns]
    symbol_col = _find_col(df500.columns, ["Symbol", "Ticker"])
    if not symbol_col:
        print(f"Cross-check WARNING: could not auto-detect NIFTY500 symbol column: {list(df500.columns)}")
        return
    nifty500_tickers = set(df500[symbol_col].astype(str).str.strip().apply(clean_symbol))

    union_base4 = set()
    for tier in BASE500_TIERS:
        union_base4 |= set(tier_frames[tier]["ticker"])

    only_in_union = union_base4 - nifty500_tickers
    only_in_500 = nifty500_tickers - union_base4
    if not only_in_union and not only_in_500:
        print("Cross-check OK: union(NIFTY50, NEXT50, MIDCAP150, SMALLCAP250) == fetched NIFTY500.")
    else:
        print("Cross-check WARNING: drift detected (informational, does not block the run).")
        if only_in_union:
            print(f"  In union but not fetched NIFTY500 ({len(only_in_union)}): {sorted(only_in_union)[:20]}")
        if only_in_500:
            print(f"  In fetched NIFTY500 but not union ({len(only_in_500)}): {sorted(only_in_500)[:20]}")


# =============================================================================
# Step 2 — NSE F&O market-lot CSV
# =============================================================================

def fetch_fno_tickers(url: str = FNO_URL) -> set:
    print(f"[FNO] Downloading from {url} ...")
    resp = requests.get(url, headers=CSV_HEADERS, timeout=30)
    resp.raise_for_status()

    df = pd.read_csv(io.StringIO(resp.text), header=0)
    df.columns = [c.strip() for c in df.columns]
    underlying_col = df.columns[0]
    symbol_col = df.columns[1]

    marker_mask = df[underlying_col].astype(str).str.strip().str.lower() == \
        "derivatives on individual securities"
    marker_idx = df.index[marker_mask]
    if len(marker_idx) == 0:
        print("  X [FNO] Could not find 'Derivatives on Individual Securities' marker row.")
        sys.exit(1)

    stock_rows = df.loc[marker_idx[0] + 1:].copy()
    stock_rows[symbol_col] = stock_rows[symbol_col].astype(str).str.strip()
    stock_rows = stock_rows[stock_rows[symbol_col].str.lower() != "nan"]

    tickers = set(stock_rows[symbol_col].apply(clean_symbol))
    print(f"  [FNO] {len(tickers)} F&O-eligible securities.")
    return tickers


# =============================================================================
# Step 3 — Sector / industry enrichment
# =============================================================================

def fetch_sector_industry_from_db(sb, tickers: List[str]) -> pd.DataFrame:
    print(f"Looking up sector/industry for {len(tickers)} tickers in {NSE_UNIVERSE_TABLE} ...")
    rows: List[dict] = []
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        resp = (
            sb.schema(UNIVERSE_SCHEMA).table(NSE_UNIVERSE_TABLE)
            .select("ticker, sector, industry")
            .in_("ticker", batch)
            .execute()
        )
        rows.extend(resp.data or [])
    df = pd.DataFrame(rows, columns=["ticker", "sector", "industry"])
    print(f"  Found sector/industry for {len(df)}/{len(tickers)} tickers in the DB.")
    return df


def fetch_tv_sector_industry() -> pd.DataFrame:
    from tradingview_screener import Query, col
    print("Fetching sector/industry for all NSE symbols from TradingView Screener (fallback) ...")
    all_frames = []
    for offset in [0, 2000, 4000]:
        try:
            _count, df_page = (
                Query()
                .set_markets("india")
                .select("name", "sector", "industry")
                .where(col("exchange") == "NSE", col("is_primary") == True)
                .limit(2000)
                .offset(offset)
                .get_scanner_data()
            )
            if df_page.empty:
                break
            all_frames.append(df_page)
        except Exception as exc:
            print(f"  Page offset={offset} failed: {exc}")
            break

    if not all_frames:
        return pd.DataFrame(columns=["ticker", "sector", "industry"])
    tv_df = pd.concat(all_frames, ignore_index=True).drop_duplicates(subset="name")
    tv_df["ticker"] = tv_df["name"].apply(clean_symbol)
    return tv_df[["ticker", "sector", "industry"]]


def attach_sector_industry(master: pd.DataFrame, sb=None) -> pd.DataFrame:
    if sb is not None:
        db_sector = fetch_sector_industry_from_db(sb, master["ticker"].tolist())
        merged = master.merge(db_sector, on="ticker", how="left")
    else:
        print("No Supabase credentials available for sector/industry lookup -- using TradingView Screener.")
        tv_df = fetch_tv_sector_industry()
        merged = master.merge(tv_df, on="ticker", how="left")

    missing = merged[merged["sector"].isna()]
    if not missing.empty:
        if sb is not None:
            print(f"{len(missing)} tickers missing sector/industry in DB -- falling back to TradingView Screener.")
            tv_df = fetch_tv_sector_industry()
            merged = merged.drop(columns=["sector", "industry"]).merge(tv_df, on="ticker", how="left")

        still_missing = merged[merged["sector"].isna()]
        if not still_missing.empty:
            print(f"  Warning: {len(still_missing)} tickers still have no sector/industry match.")
            with pd.option_context("display.max_rows", 20, "display.width", 150):
                print(still_missing[["ticker", "company_name"]].head(20).to_string(index=False))

    return merged


# =============================================================================
# Step 4 — Optional tvDatafeed validation
# =============================================================================

def _get_thread_tv():
    from tvDatafeed import TvDatafeed
    if not hasattr(_thread_local, "tv"):
        _thread_local.tv = TvDatafeed()
    return _thread_local.tv


def fetch_latest_close_with_retry(ticker: str) -> Tuple[Optional[float], Optional[str]]:
    from tvDatafeed import Interval
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with _connection_semaphore:
                tv = _get_thread_tv()
                df = tv.get_hist(symbol=ticker, exchange=DEFAULT_EXCHANGE, interval=Interval.in_daily, n_bars=2)
                time.sleep(REQUEST_DELAY_SEC + random.uniform(0, REQUEST_JITTER))
            if df is None or df.empty:
                return None, "empty response"
            df = df.copy()
            df.columns = [c.lower() for c in df.columns]
            df = df[~df["close"].isna()]
            if df.empty:
                return None, "no valid close price"
            return float(df["close"].iloc[-1]), None
        except Exception as exc:
            if hasattr(_thread_local, "tv"):
                del _thread_local.tv
            if attempt < MAX_RETRIES:
                time.sleep(min(BACKOFF_BASE ** attempt, BACKOFF_MAX))
            else:
                return None, f"{type(exc).__name__}: {exc}"
    return None, f"All {MAX_RETRIES} attempts failed"


def validate_tickers(master: pd.DataFrame, workers: int) -> Tuple[pd.DataFrame, List[dict]]:
    global _connection_semaphore
    _connection_semaphore = threading.Semaphore(workers)

    results: Dict[str, Optional[float]] = {}
    errors: Dict[str, Optional[str]] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_latest_close_with_retry, t): t for t in master["ticker"]}
        with tqdm(total=len(futures), desc=f"Validating (workers={workers})", unit="stock") as pbar:
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    close, err = future.result()
                except Exception as exc:
                    close, err = None, f"Unhandled: {exc}"
                results[ticker] = close
                errors[ticker] = err
                pbar.update(1)

    master = master.copy()
    master["latest_close"] = master["ticker"].map(results)
    passing = master[master["latest_close"].notna()].drop(columns=["latest_close"]).reset_index(drop=True)
    failing = master[master["latest_close"].isna()]
    failures = [
        {"ticker": row["ticker"], "reason": errors.get(row["ticker"])}
        for _, row in failing.iterrows()
    ]
    print(f"Validation: {len(passing)}/{len(master)} tickers passed.")
    if failures:
        print("  Failed validation tickers (first 30):")
        for failure in failures[:30]:
            print(f"    {failure['ticker']:<20} {str(failure['reason'])[:80]}")
    return passing, failures


# =============================================================================
# Step 5 — Upsert + prune universe.n750
# =============================================================================

def get_sb():
    from supabase import create_client
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("X Missing env vars. Need NEXT_PUBLIC_SUPABASE_URL and "
              "SUPABASE_SERVICE_ROLE_KEY (or NEXT_PUBLIC_SUPABASE_ANON_KEY).")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def upsert_n750(sb, master: pd.DataFrame, include_legacy_columns: bool = True) -> None:
    now = datetime.now(timezone.utc).isoformat()
    payload = []
    for _, row in master.iterrows():
        item = {
            "ticker": row["ticker"],
            "company_name": row["company_name"],
            "sector": None if pd.isna(row.get("sector")) else row["sector"],
            "industry": None if pd.isna(row.get("industry")) else row["industry"],
            "indices": row["indices"],
            "updated_at": now,
        }
        if include_legacy_columns:
            # The v2 SQL is additive by default: existing n750 tables may still
            # have not-null compatibility columns used by older app code/jobs.
            item["exchange"] = DEFAULT_EXCHANGE
            item["is_active"] = True
        payload.append(item)

    print(f"\nUpserting {len(payload)} rows into {UNIVERSE_SCHEMA}.{N750_TABLE} ...")
    for i in range(0, len(payload), BATCH_SIZE):
        batch = payload[i:i + BATCH_SIZE]
        sb.schema(UNIVERSE_SCHEMA).table(N750_TABLE).upsert(batch, on_conflict="ticker").execute()
    print(f"  Done. Upserted {len(payload)} rows.")


def prune_dropped_tickers(sb, fresh_tickers: set) -> int:
    """Hard-delete rows for tickers no longer in any tier list --
    only active stocks are stored, so a dropout is a delete, not a flag."""
    resp = sb.schema(UNIVERSE_SCHEMA).table(N750_TABLE).select("ticker").execute()
    current = {r["ticker"] for r in (resp.data or [])}
    dropped = current - fresh_tickers
    if not dropped:
        print("No tickers to prune -- DB ticker set matches the fresh union.")
        return 0

    print(f"Pruning {len(dropped)} tickers no longer in any tier list ...")
    dropped_list = list(dropped)
    for i in range(0, len(dropped_list), BATCH_SIZE):
        batch = dropped_list[i:i + BATCH_SIZE]
        sb.schema(UNIVERSE_SCHEMA).table(N750_TABLE).delete().in_("ticker", batch).execute()
    print(f"  Done. Pruned {len(dropped)} tickers: {sorted(dropped)[:20]}{' ...' if len(dropped) > 20 else ''}")
    return len(dropped)


def _write_result(succeeded: int, failed: int, skipped: int, pruned: int, total: int, errors: list) -> None:
    pathlib.Path("results").mkdir(exist_ok=True)
    pathlib.Path(RESULT_PATH).write_text(json.dumps({
        "script": "Universe Sync (n750, v2)",
        "succeeded": succeeded,
        "failed": failed,
        "skipped": skipped,
        "pruned": pruned,
        "total": total,
        "errors": errors,
    }))


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Sync universe.n750 (tiers + F&O tags) to Supabase.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print/save only -- no DB writes.")
    parser.add_argument("--save", type=str, default=None, help="Optional CSV path to save the result.")
    parser.add_argument("--validate", action="store_true", help="Validate tickers via tvDatafeed before upserting.")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help=f"tvDatafeed workers (default: {MAX_WORKERS}).")
    parser.add_argument("--skip-fno", action="store_true", help="Skip F&O tagging.")
    parser.add_argument("--keep-orphans", action="store_true", help="Don't delete tickers that dropped out of every list.")
    parser.add_argument(
        "--no-legacy-columns",
        action="store_true",
        help="Do not send exchange/is_active in the upsert payload. Use only after those columns are dropped.",
    )
    args = parser.parse_args()

    # 1. Tier CSVs + tier assignment
    tier_frames = {tier: fetch_index_csv(tier, url) for tier, url in TIER_URLS.items()}
    cross_check_totalmarket(tier_frames)
    master = build_tier_map(tier_frames)
    print(f"\nMaster universe (union of all 5 tiers): {len(master)} tickers.")

    # 2. F&O tagging
    if not args.skip_fno:
        fno_tickers = fetch_fno_tickers()
    else:
        print("--skip-fno set: skipping F&O tagging.")
        fno_tickers = set()

    master["indices"] = master.apply(
        lambda r: [r["tier"]] + (["FNO"] if r["ticker"] in fno_tickers else []), axis=1
    )
    master = master.drop(columns=["tier"])

    sb = None
    if SUPABASE_URL and SUPABASE_KEY:
        sb = get_sb()

    # 3. Sector / industry
    master = attach_sector_industry(master, sb)

    # 4. Optional validation
    failures: List[dict] = []
    if args.validate:
        master, failures = validate_tickers(master, workers=args.workers)

    print(f"\nFinal universe size: {len(master)} rows.")
    with pd.option_context("display.max_columns", None, "display.width", 200):
        print(master.head(15).to_string(index=False))

    if args.save:
        master.to_csv(args.save, index=False)
        print(f"Saved result to: {args.save}")

    if args.dry_run:
        print("\n--dry-run set: skipping database writes.")
        return

    # 5. Upsert + prune
    if sb is None:
        print("X Missing env vars. Need NEXT_PUBLIC_SUPABASE_URL and "
              "SUPABASE_SERVICE_ROLE_KEY (or NEXT_PUBLIC_SUPABASE_ANON_KEY).")
        sys.exit(1)

    upsert_n750(sb, master, include_legacy_columns=not args.no_legacy_columns)

    pruned = 0
    if not args.keep_orphans:
        pruned = prune_dropped_tickers(sb, set(master["ticker"]))
    else:
        print("--keep-orphans set: skipping prune step.")

    errors = [f["ticker"] for f in failures[:10]]
    if pruned:
        errors.append(f"(pruned {pruned} dropped tickers)")
    _write_result(
        succeeded=len(master),
        failed=len(failures),
        skipped=0,
        pruned=pruned,
        total=len(master) + len(failures),
        errors=errors[:10],
    )


if __name__ == "__main__":
    main()
