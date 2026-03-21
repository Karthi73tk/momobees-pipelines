"""
NSE Mainboard Equity Universe Sync — stock_universe_nse_all
------------------------------------------------------------
Populates the stock_universe_nse_all table. Filters to NSE Mainboard
Equity only (excludes ETFs, SME stocks, funds, REITs, InvITs).

Usage:
    python data_sync_engine_nse_all.py               # fetch + preview + upsert (default)
    python data_sync_engine_nse_all.py --preview-only # fetch + preview, no DB writes

Features:
    - Upserts by default; --preview-only skips DB writes
    - Skip-if-today: checks updated_at per ticker, skips already-synced rows
    - Single upsert call (fast, no batching needed)
    - Mainboard Equity filter: type=stock, subtype=common, exchange=NSE
    - SME excluded automatically (NSE_SME exchange != NSE)
    - Clear error reporting for fetch and upsert failures
    - Post-upsert summary matching preview layout

Requirements:
    pip install supabase tradingview-screener pandas python-dotenv
"""

import os
import sys
import logging
import argparse
import traceback
import pandas as pd
from typing import Optional
from datetime import datetime, timezone, date
from supabase import create_client
from tradingview_screener import Query, col
from dotenv import load_dotenv
import json
import pathlib

load_dotenv(".env.local")

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = (
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")
)
TABLE        = "stock_universe_nse_all"
PAGE_SIZE    = 3000
PREVIEW_ROWS = 20

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── TradingView columns ───────────────────────────────────────────────────────
TV_COLUMNS = [
    "name",                 # ticker
    "description",          # company_name
    "sector",
    "industry",
    "close",                # price
    "EMA10",                # ema_10
    "EMA20",                # ema_20
    "EMA50",                # ema_50
    "EMA200",               # ema_200
    "SMA200",               # sma_200
    "RSI",                  # rsi
    "Perf.W",               # perf_w
    "Perf.1M",              # perf_1m
    "price_52_week_high",   # high_52w
    "price_52_week_low",    # low_52w
    "High.All",             # all_time_high
    "High.All.Date",        # all_time_high_day
    "market_cap_basic",     # market_cap (converted to Crores)
]


# ── Step 1: Fetch from TradingView ────────────────────────────────────────────
def fetch_nse_mainboard() -> pd.DataFrame:
    """
    Fetch NSE Mainboard Equity stocks only.
    Filters:
      - exchange = NSE          -> excludes NSE_SME automatically
      - is_primary = True       -> one row per company
      - type = stock            -> excludes ETFs, funds, REITs, InvITs, indices
      - subtype = common        -> excludes preference shares, warrants
      - market_cap_basic > 0    -> excludes shell/unlisted names
    """
    frames = []
    fetch_errors = []

    for offset in [0, 2000]:
        log.info("Fetching TV Screener offset=%d ...", offset)
        try:
            _count, df = (
                Query()
                .set_markets("india")
                .select(*TV_COLUMNS)
                .where(
                    col("exchange") == "NSE",
                    col("is_primary") == True,
                    col("type") == "stock",
                    col("subtype") == "common",
                    col("market_cap_basic") > 0,
                )
                .limit(PAGE_SIZE)
                .offset(offset)
                .get_scanner_data()
            )
            if df.empty:
                log.info("  offset=%d: empty page, stopping pagination.", offset)
                break
            frames.append(df)
            log.info(
                "  offset=%d: %d rows fetched (total available on TV=%d)",
                offset, len(df), _count,
            )
        except Exception as exc:
            msg = f"Fetch failed at offset={offset}: {type(exc).__name__}: {exc}"
            fetch_errors.append(msg)
            log.error("  X %s", msg)
            log.debug(traceback.format_exc())
            break   # don't try next page if this one errored

    if fetch_errors and not frames:
        raise RuntimeError(
            "TradingView Screener returned no data. Errors:\n  "
            + "\n  ".join(fetch_errors)
        )

    if fetch_errors:
        log.warning(
            "Partial fetch -- some pages failed. Proceeding with %d rows.",
            sum(len(f) for f in frames),
        )

    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset="name")
    log.info("Total unique NSE Mainboard stocks fetched: %d", len(df))
    return df


# ── Step 2: Build row payloads ────────────────────────────────────────────────
def _safe_float(value, decimals: int = 2) -> Optional[float]:
    """Return rounded float or None for NaN/None."""
    try:
        if value is None or value != value:   # NaN check
            return None
        return round(float(value), decimals)
    except Exception:
        return None


def _parse_ath_day(raw) -> Optional[str]:
    """Parse TradingView High.All.Date (Unix seconds or date string) -> ISO string."""
    if raw is None or raw != raw:
        return None
    try:
        return pd.Timestamp(int(raw), unit="s").isoformat()
    except Exception:
        pass
    try:
        return pd.Timestamp(raw).isoformat()
    except Exception:
        return None


def build_rows(df: pd.DataFrame, now: str) -> list:
    rows = []
    for _, row in df.iterrows():
        mcap_raw = row.get("market_cap_basic")
        market_cap = None
        if mcap_raw is not None and mcap_raw == mcap_raw:
            try:
                market_cap = round(float(mcap_raw) / 1_00_00_000, 2)   # INR -> Crores
            except Exception:
                pass

        rows.append({
            "ticker":            row["name"],
            "company_name":      row.get("description"),
            "sector":            row.get("sector"),
            "industry":          row.get("industry"),
            "price":             _safe_float(row.get("close")),
            "ema_10":            _safe_float(row.get("EMA10")),
            "ema_20":            _safe_float(row.get("EMA20")),
            "ema_50":            _safe_float(row.get("EMA50")),
            "ema_200":           _safe_float(row.get("EMA200")),
            "sma_200":           _safe_float(row.get("SMA200")),
            "rsi":               _safe_float(row.get("RSI")),
            "perf_w":            _safe_float(row.get("Perf.W"), 4),
            "perf_1m":           _safe_float(row.get("Perf.1M"), 4),
            "high_52w":          _safe_float(row.get("price_52_week_high")),
            "low_52w":           _safe_float(row.get("price_52_week_low")),
            "all_time_high":     _safe_float(row.get("High.All")),
            "all_time_high_day": _parse_ath_day(row.get("High.All.Date")),
            "market_cap":        market_cap,
            "is_active":         True,
            "updated_at":        now,
        })
    return rows


# ── Step 3: Shared summary printer ───────────────────────────────────────────
def _print_summary(rows: list, header: str, footer: str) -> None:
    df = pd.DataFrame(rows)

    print("\n" + "=" * 80)
    print(f"  {header}")
    print("=" * 80)

    # Stats
    print(f"\n{'Total stocks':30s}: {len(df)}")
    print(f"{'Sectors':30s}: {df['sector'].nunique()}")
    print(f"{'Industries':30s}: {df['industry'].nunique()}")
    print(f"{'With RSI':30s}: {df['rsi'].notna().sum()}")
    print(f"{'With EMA200':30s}: {df['ema_200'].notna().sum()}")
    print(f"{'With Market Cap':30s}: {df['market_cap'].notna().sum()}")

    # Sector breakdown
    print("\n-- Sector Distribution " + "-" * 57)
    for sector, count in df["sector"].value_counts().head(15).items():
        bar = "█" * (count // 10)
        print(f"  {str(sector):<35s} {count:>4d}  {bar}")

    # Sample rows
    cols_to_show = ["ticker", "company_name", "sector", "price", "ema_200", "rsi", "market_cap"]
    print(f"\n-- Sample {PREVIEW_ROWS} rows " + "-" * 60)
    print(df[cols_to_show].head(PREVIEW_ROWS).to_string(index=False))

    # Null audit
    print("\n-- Null counts per column " + "-" * 54)
    null_counts = df.isnull().sum()
    had_nulls = False
    for col_name, n in null_counts.items():
        if n > 0:
            print(f"  {col_name:<25s}: {n} nulls")
            had_nulls = True
    if not had_nulls:
        print("  No nulls found across all columns.")

    print("\n" + "=" * 80)
    print(f"  {footer}")
    print("=" * 80 + "\n")


# ── Step 4: Check already-synced tickers ─────────────────────────────────────
def get_already_synced_today(sb, tickers: list) -> set:
    """
    Query Supabase for tickers whose updated_at is already today (UTC).
    Returns a set of ticker strings to skip.
    """
    today_str = date.today().isoformat()
    synced = set()
    CHUNK = 500

    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i : i + CHUNK]
        try:
            resp = (
                sb.table(TABLE)
                .select("ticker, updated_at")
                .in_("ticker", chunk)
                .gte("updated_at", f"{today_str}T00:00:00+00:00")
                .execute()
            )
            for record in resp.data:
                synced.add(record["ticker"])
        except Exception as exc:
            log.warning(
                "Could not check already-synced tickers (chunk %d): %s: %s",
                i, type(exc).__name__, exc,
            )

    log.info("Already synced today: %d tickers -> will skip these.", len(synced))
    return synced


# ── Step 5: Single upsert ────────────────────────────────────────────────────
def upsert_rows(sb, rows: list) -> bool:
    """Upsert all rows in a single call. Returns True on success."""
    log.info("Upserting %d rows to %s ...", len(rows), TABLE)
    try:
        sb.table(TABLE).upsert(rows, on_conflict="ticker").execute()
        log.info("Upsert complete -- %d rows written.", len(rows))
        return True
    except Exception as exc:
        log.error(
            "X Upsert failed: %s: %s",
            type(exc).__name__, exc,
        )
        log.debug(traceback.format_exc())
        return False


# ── Result writer ────────────────────────────────────────────────────────────
def _write_result(succeeded: int, failed: int, skipped: int,
                  total: int, errors: list) -> None:
    """Write JSON result file consumed by the GitHub Actions summary step."""
    pathlib.Path("results").mkdir(exist_ok=True)
    pathlib.Path("results/nse_all.json").write_text(json.dumps({
        "script":    "NSE All Universe Sync",
        "succeeded": succeeded,
        "failed":    failed,
        "skipped":   skipped,
        "total":     total,
        "errors":    errors[:10],
    }))


# ── Orchestrator ──────────────────────────────────────────────────────────────
def run(do_upsert: bool) -> None:
    # 1. Fetch
    try:
        df = fetch_nse_mainboard()
    except RuntimeError as exc:
        log.error("X Could not fetch data from TradingView:\n  %s", exc)
        _write_result(0, 0, 0, 0, [str(exc)])
        sys.exit(1)

    now  = datetime.now(timezone.utc).isoformat()
    rows = build_rows(df, now)
    total = len(rows)

    # 2. Preview-only mode
    if not do_upsert:
        _print_summary(
            rows,
            header="PREVIEW MODE  --  No data written to Supabase",
            footer="Run without --preview-only to write to Supabase.",
        )
        return

    # 3. Validate env vars
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error(
            "X Missing env vars. Need NEXT_PUBLIC_SUPABASE_URL "
            "and SUPABASE_SERVICE_ROLE_KEY in .env.local"
        )
        _write_result(0, 0, 0, total, ["Missing env vars"])
        sys.exit(1)

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    # 4. Skip tickers already synced today
    all_tickers  = [r["ticker"] for r in rows]
    already_done = get_already_synced_today(sb, all_tickers)
    pending_rows = [r for r in rows if r["ticker"] not in already_done]
    skipped      = len(already_done)

    log.info(
        "Pending upsert: %d rows (skipping %d already synced today).",
        len(pending_rows), skipped,
    )

    if not pending_rows:
        log.info("Nothing to upsert -- all tickers already up to date for today.")
        _print_summary(
            rows,
            header="SYNC SKIPPED  --  All tickers already synced today",
            footer=f"Next sync available tomorrow. Table: {TABLE}",
        )
        _write_result(0, 0, skipped, total, [])
        return

    # 5. Upsert
    success = upsert_rows(sb, pending_rows)

    # 6. Post-upsert summary
    if success:
        _print_summary(
            pending_rows,
            header=f"SYNC COMPLETE  --  {len(pending_rows)} stocks written to Supabase",
            footer=f"Table: {TABLE}  |  Synced at: {now}",
        )
        _write_result(len(pending_rows), 0, skipped, total, [])
    else:
        log.error("X Sync failed. Check the error above for details.")
        _write_result(0, len(pending_rows), skipped, total, ["Upsert failed"])
        sys.exit(1)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sync NSE Mainboard Equity universe to Supabase."
    )
    parser.add_argument(
        "--preview-only",
        action="store_true",
        help="Fetch and preview data without writing to Supabase.",
    )
    args = parser.parse_args()
    run(do_upsert=not args.preview_only)