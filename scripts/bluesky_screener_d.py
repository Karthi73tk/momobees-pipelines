"""
Blue Sky Breakout — daily EOD screener  ->  bluesky.daily_candidates
-------------------------------------------------------------------
Scans the whole NSE mainboard equity universe (TradingView Screener), applies the
Blue Sky funnel, and writes the "armed-and-ready" candidate list for the NEXT session
to Supabase, so the intraday runner knows exactly what to place buy-stops on.

The funnel (from the BananaPatterns "How we narrow the market down" panel):
    1. Liquidity floor   — mcap >= ₹500 cr AND >= ₹5 cr/day traded (20d avg value)
    2. At its all-time high — pivot = the all-time high (nothing ever traded above it)
    3. A leader          — RS >= 70 (IBD-blend of 3/6/12-mo performance, ranked across
                           the liquid universe)
    4. Near the trigger  — close within 20% of the pivot (close >= 0.8 * ATH)
   (+ trend: close above its 50-day SMA)

Each qualifying name is tagged:
    ARMED     — within 20% below the pivot, not yet through it -> place a buy-stop at pivot
    TRIGGERED — closed at/above the pivot today (a fresh all-time-high close)

Run this AFTER the NSE close each trading day.

Usage:
    python bluesky_screener_d.py                 # scan + upsert to Supabase (default)
    python bluesky_screener_d.py --preview-only  # scan + print, no DB write
    python bluesky_screener_d.py --top 40        # print only the top-40 by RS in preview

Requires: supabase, tradingview-screener, pandas, python-dotenv
Creds read from ../.env.local (NEXT_PUBLIC_SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY).
"""
from __future__ import annotations
import os, sys, json, argparse, logging, pathlib
from datetime import datetime, date
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd
from supabase import create_client
from tradingview_screener import Query, col
from dotenv import load_dotenv

ROOT = pathlib.Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env.local")
load_dotenv(ROOT / "scripts" / ".env.local")

SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
                or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY"))
SCHEMA = "bluesky"

# ── Funnel parameters (the dials from the panel) ─────────────────────────────
MIN_MCAP_CR      = 500.0     # ₹ crore
MIN_TURNOVER_CR  = 5.0       # ₹ crore/day (20d avg value)
RS_MIN           = 70
WITHIN_PIVOT     = 0.20      # close must be within 20% below the pivot
STOP_PCT         = 0.08

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("bluesky")

TV_COLUMNS = ["name", "description", "close", "High.All", "High.All.Date",
              "market_cap_basic", "average_volume_10d_calc",
              "Perf.3M", "Perf.6M", "Perf.Y", "SMA50"]


def fetch_universe() -> pd.DataFrame:
    """Whole NSE mainboard equity (paginated), same filter as data_sync_engine_nse_all_d."""
    frames = []
    for offset in (0, 2000, 4000):
        try:
            _n, df = (Query().set_markets("india").select(*TV_COLUMNS)
                      .where(col("exchange") == "NSE", col("is_primary") == True,
                             col("type") == "stock", col("subtype") == "common",
                             col("market_cap_basic") > 0)
                      .limit(2000).offset(offset).get_scanner_data())
        except Exception as exc:
            log.info("  offset=%d: no more data (%s)", offset, type(exc).__name__)
            break
        if df is None or df.empty:
            break
        frames.append(df)
        log.info("  fetched offset=%d: %d rows", offset, len(df))
        if len(df) < 2000:
            break
    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset="name")
    log.info("universe fetched: %d NSE mainboard names", len(df))
    return df


def screen(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    # TV returns both 'ticker' (=NSE:XXX) and 'name' (=XXX); drop the former so the
    # name->ticker rename doesn't collide into a duplicate column.
    d = df.drop(columns=["ticker"], errors="ignore")
    d = d.rename(columns={"name": "ticker", "description": "company",
                           "High.All": "ath", "High.All.Date": "ath_ts",
                           "market_cap_basic": "mcap_inr", "average_volume_10d_calc": "avgvol",
                           "Perf.3M": "p3", "Perf.6M": "p6", "Perf.Y": "p12",
                           "SMA50": "sma50"}).copy()
    for c in ("close", "ath", "mcap_inr", "avgvol", "p3", "p6", "p12", "sma50"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d["mcap_cr"] = d["mcap_inr"] / 1e7
    d["turnover_cr"] = (d["avgvol"] * d["close"]) / 1e7     # 20d avg daily traded value
    # step 1 — liquidity floor -> the tradable universe (RS is ranked within this set)
    liquid = (d["mcap_cr"] >= MIN_MCAP_CR) & (d["turnover_cr"] >= MIN_TURNOVER_CR)
    elig = d[liquid].copy()
    # step 3 — RS: IBD-style blend of 3/6/12-mo perf, percentile-ranked across the liquid universe
    elig["rs_score"] = 0.4 * elig["p3"] + 0.2 * elig["p6"] + 0.4 * elig["p12"]
    elig["rs"] = np.ceil(elig["rs_score"].rank(pct=True) * 99).clip(1, 99)
    # steps 2 & 4 — pivot = all-time high; within 20% of it; and above the 50-day
    elig = elig[elig["ath"] > 0].copy()
    elig["dist_to_pivot_pct"] = (elig["ath"] / elig["close"] - 1) * 100
    within = elig["close"] >= (1 - WITHIN_PIVOT) * elig["ath"]
    leader = elig["rs"] >= RS_MIN
    trend = elig["close"] > elig["sma50"]
    cand = elig[within & leader & trend].copy()
    cand["status"] = np.where(cand["close"] >= cand["ath"] * 0.999, "TRIGGERED", "ARMED")
    cand["stop_pivot"] = (cand["ath"] * (1 - STOP_PCT)).round(2)
    cand["trigger_price"] = cand["ath"].round(2)
    cand["above_sma50"] = True
    cand = cand.sort_values(["status", "rs", "dist_to_pivot_pct"],
                            ascending=[True, False, True]).reset_index(drop=True)
    stats = dict(universe=len(df), liquid=int(liquid.sum()), candidates=len(cand),
                 armed=int((cand.status == "ARMED").sum()),
                 triggered=int((cand.status == "TRIGGERED").sum()))
    return cand, stats


def _write_result(succeeded: int, failed: int, skipped: int, total: int, errors: list) -> None:
    """JSON result file consumed by the GitHub Actions daily-summary step."""
    pathlib.Path("results").mkdir(exist_ok=True)
    pathlib.Path("results/bluesky.json").write_text(json.dumps({
        "script": "Blue Sky Screener", "succeeded": succeeded, "failed": failed,
        "skipped": skipped, "total": total, "errors": errors[:10],
    }))


def _ath_date(ts):
    try:
        return pd.Timestamp(int(ts), unit="s").date().isoformat()
    except Exception:
        return None


def build_rows(cand: pd.DataFrame, run_date: str) -> list[dict]:
    now = datetime.now(ZoneInfo("Asia/Kolkata")).isoformat()
    rows = []
    for _, r in cand.iterrows():
        rows.append(dict(
            run_date=run_date, ticker=r["ticker"], company=r.get("company"),
            status=r["status"], rs=int(r["rs"]),
            close=round(float(r["close"]), 2), pivot=round(float(r["ath"]), 2),
            trigger_price=float(r["trigger_price"]),
            dist_to_pivot_pct=round(float(r["dist_to_pivot_pct"]), 2),
            stop_pivot=float(r["stop_pivot"]),
            mcap_cr=round(float(r["mcap_cr"]), 1), turnover_cr=round(float(r["turnover_cr"]), 2),
            above_sma50=True, ath_date=_ath_date(r.get("ath_ts")), screened_at=now,
        ))
    return rows


def upsert(rows: list[dict], stats: dict, run_date: str):
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    if rows:
        sb.schema(SCHEMA).table("daily_candidates").upsert(
            rows, on_conflict="run_date,ticker").execute()
    sb.schema(SCHEMA).table("screener_runs").upsert(dict(
        run_date=run_date, universe_scanned=stats["universe"],
        eligible_liquid=stats["liquid"], candidates_total=stats["candidates"],
        armed=stats["armed"], triggered=stats["triggered"],
        finished_at=datetime.now(ZoneInfo("Asia/Kolkata")).isoformat(), status="ok",
    ), on_conflict="run_date").execute()
    log.info("upserted %d candidates for %s", len(rows), run_date)


def main():
    ap = argparse.ArgumentParser(description="Blue Sky daily EOD screener.")
    ap.add_argument("--preview-only", action="store_true", help="scan + print, no DB write")
    ap.add_argument("--top", type=int, default=25, help="rows to print in preview")
    args = ap.parse_args()

    run_date = datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()
    log.info("=== BLUE SKY SCREENER — run_date %s ===", run_date)
    df = fetch_universe()
    cand, stats = screen(df)
    log.info("universe %d -> liquid %d -> candidates %d (armed %d, triggered %d)",
             stats["universe"], stats["liquid"], stats["candidates"], stats["armed"], stats["triggered"])
    show = (cand.head(args.top)
            .assign(pivot=cand["ath"].round(2), rs=cand["rs"].astype(int))
            [["ticker", "status", "rs", "close", "pivot",
              "dist_to_pivot_pct", "stop_pivot", "mcap_cr", "turnover_cr"]])
    print(f"\nTop {min(args.top, len(cand))} of {len(cand)} candidates (run_date {run_date}):")
    print(show.to_string(index=False))

    if args.preview_only:
        log.info("preview-only: no DB write."); return
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("missing Supabase creds in .env.local — aborting write."); sys.exit(1)
    rows = build_rows(cand, run_date)
    try:
        upsert(rows, stats, run_date)
        _write_result(len(rows), 0, 0, stats["universe"], [])
        try:
            from notify import notify_summary
            notify_summary("bluesky_screener_d", [
                ("Run date", run_date), ("Universe", stats["universe"]),
                ("Liquid", stats["liquid"]), ("Candidates", stats["candidates"]),
                ("Armed", stats["armed"]), ("Triggered", stats["triggered"])])
        except Exception:
            pass
    except Exception as exc:
        log.error("DB write failed: %s", exc)
        _write_result(0, 1, 0, stats["universe"], [str(exc)])
        try:
            from notify import notify_failure
            notify_failure("bluesky_screener_d", str(exc))
        except Exception:
            pass
        sys.exit(1)
    log.info("done.")


if __name__ == "__main__":
    main()
