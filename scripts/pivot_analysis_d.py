"""
pivot_analysis.py
=================
Uses tvDatafeed to fetch OHLC data and calculates:
  - Traditional Pivot Points & CPR  (Daily / Weekly / Monthly)
  - Pivot Trend  (BULLISH-MOMENTUM / BEARISH-MOMENTUM / BULLISH / BEARISH / NEUTRAL)
  - 2-Day CPR Relationship (Daily timeframe only)

Workflow:
  1. Fetch all tickers from Supabase `pivot_trend_analysis` table
  2. Skip any ticker where `last_updated` is already today (IST)
  3. For each remaining ticker, fetch Daily / Weekly / Monthly bars via tvDatafeed
  4. Calculate CPR, trends, 2-day relationship
  5. Upsert results back to Supabase
  6. Print a summary: success count, failure count, list of failed tickers

Requirements:
    pip install tvDatafeed supabase python-dotenv pytz
"""

from __future__ import annotations

import json
import os
import pathlib
import time
import logging
from datetime import datetime, timezone, date
from dataclasses import dataclass, field
from typing import Optional

import pytz
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
from tvDatafeed import TvDatafeed, Interval

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv(".env.local") # reads SUPABASE_URL and SUPABASE_KEY from a .env file

SUPABASE_URL: str = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str = os.environ["NEXT_PUBLIC_SUPABASE_ANON_KEY"]


EXCHANGE         = "NSE"
IST              = pytz.timezone("Asia/Kolkata")
UPSERT_BATCH     = 50
N_DAILY_BARS     = 5      # need at least 3 completed daily bars
N_WEEKLY_BARS    = 5
N_MONTHLY_BARS   = 5
SLEEP_BETWEEN    = 0.3    # seconds between tvDatafeed calls (be polite)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------

@dataclass
class OHLCBar:
    high:  float
    low:   float
    close: float


@dataclass
class CPRLevels:
    pivot:     float
    bcpr:      float
    tcpr:      float
    r1:        float
    s1:        float
    max_pivot: float   # max(pivot, bcpr, tcpr)
    min_pivot: float   # min(pivot, bcpr, tcpr)
    max_high:  float   # max(r1, prev_high)
    max_low:   float   # min(s1, prev_low)


@dataclass
class TickerResult:
    ticker:                str
    close:                 float
    monthly_trend:         str = "UNKNOWN"
    weekly_trend:          str = "UNKNOWN"
    daily_trend:           str = "UNKNOWN"
    cpr_2day_relationship: str = "UNKNOWN"
    width_classification:  str = "UNKNOWN"
    pdh:                   Optional[float] = None
    pdl:                   Optional[float] = None
    last_updated:          str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


# ---------------------------------------------------------------------------
# CPR / Pivot calculations
# ---------------------------------------------------------------------------

def compute_cpr(current: OHLCBar, previous: OHLCBar) -> CPRLevels:
    """
    Build CPR levels from `current` bar's H/L/C.
    `previous` bar's H/L is used for Max High / Max Low boundary.
    """
    h, l, c = current.high, current.low, current.close
    pivot = (h + l + c) / 3
    bcpr  = (h + l) / 2
    tcpr  = (pivot - bcpr) + pivot
    r1    = (2 * pivot) - l
    s1    = (2 * pivot) - h
    return CPRLevels(
        pivot=pivot, bcpr=bcpr, tcpr=tcpr, r1=r1, s1=s1,
        max_pivot = max(pivot, bcpr, tcpr),
        min_pivot = min(pivot, bcpr, tcpr),
        max_high  = max(r1, previous.high),
        max_low   = min(s1, previous.low),
    )


def determine_trend(ltp: float, lvl: CPRLevels) -> str:
    if ltp > lvl.max_high:  return "BULLISH-MOMENTUM"
    if ltp < lvl.max_low:   return "BEARISH-MOMENTUM"
    if ltp > lvl.max_pivot: return "BULLISH"
    if ltp < lvl.min_pivot: return "BEARISH"
    return "NEUTRAL"


def determine_2day_cpr(today: CPRLevels, prev: CPRLevels) -> str:
    ct, cb = today.max_pivot, today.min_pivot
    pt, pb = prev.max_pivot,  prev.min_pivot
    if   ct > pt and cb > pb:              return "BULLISH_HV"
    elif ct < pt and cb < pb:              return "BEARISH_LV"
    elif ct > pt and cb <= pt and cb > pb: return "BULLISH_OHV"
    elif cb < pb and ct >= pb and ct < pt: return "BEARISH_OLV"
    elif ct > pt and cb < pb:              return "SIDEWAYS_OV"
    elif ct < pt and cb > pb:              return "BREAKOUT_IV"
    return "UNKNOWN"


def cpr_width_pct(lvl: CPRLevels) -> float:
    """
    CPR Width % = (TCPR - BCPR) / Pivot * 100
    Uses max_pivot and min_pivot which already equal TCPR and BCPR
    (since TCPR = max and BCPR = min of the CPR band).
    """
    if lvl.pivot == 0:
        return 0.0
    return ((lvl.max_pivot - lvl.min_pivot) / lvl.pivot) * 100


def classify_cpr_width(width_pct: float) -> str:
    """
    Classify CPR width % into a label per the standard cutoff table:

    Width (%)       Classification
    ─────────────── ──────────────
    ≤ 0.1           Super Narrow
    > 0.1 – ≤ 0.25  Narrow
    > 0.25 – ≤ 0.5  Medium
    > 0.5 – ≤ 1.5   Wide
    > 1.5            Very Wide
    """
    if width_pct <= 0.1:
        return "Super Narrow"
    elif width_pct <= 0.25:
        return "Narrow"
    elif width_pct <= 0.5:
        return "Medium"
    elif width_pct <= 1.5:
        return "Wide"
    else:
        return "Very Wide"


# ---------------------------------------------------------------------------
# tvDatafeed helpers
# ---------------------------------------------------------------------------

def _row_to_bar(row: pd.Series) -> OHLCBar:
    return OHLCBar(high=float(row["high"]),
                   low=float(row["low"]),
                   close=float(row["close"]))


def fetch_bars(tv: TvDatafeed, symbol: str, interval: Interval,
               n_bars: int) -> Optional[pd.DataFrame]:
    """
    Fetch n_bars of OHLC from tvDatafeed. Returns None on error.
    Drops the last (incomplete / still-forming) bar to ensure
    we always work with completed candles.
    """
    try:
        df = tv.get_hist(symbol=symbol, exchange=EXCHANGE,
                         interval=interval, n_bars=n_bars + 1)
        if df is None or df.empty or len(df) < 3:
            return None
        # Drop the last row — it's the currently forming (incomplete) bar
        df = df.iloc[:-1]
        return df
    except Exception as exc:
        log.debug("  fetch_bars failed (%s %s): %s", symbol, interval, exc)
        return None


# ---------------------------------------------------------------------------
# Per-ticker analysis
# ---------------------------------------------------------------------------

def analyse_ticker(tv: TvDatafeed, symbol: str) -> TickerResult:
    """
    Fetch Daily, Weekly, Monthly bars and compute all trends.

    Bar layout after dropping the forming bar (index 0 = oldest):
        df.iloc[-1]  →  most recent COMPLETED bar  (D-1 for daily)
        df.iloc[-2]  →  one bar before that         (D-2 for daily)
        df.iloc[-3]  →  two bars before that        (D-3 for daily)

    LTP = close of the most recent completed DAILY bar (df_d.iloc[-1].close).
    This is correct for pivot analysis — we always evaluate against the
    latest *confirmed* price, not a mid-candle intraday tick.
    """
    result = TickerResult(ticker=symbol, close=0.0)

    # ── Fetch all three timeframes ──────────────────────────────────────────
    df_d = fetch_bars(tv, symbol, Interval.in_daily,   N_DAILY_BARS)
    time.sleep(SLEEP_BETWEEN)
    df_w = fetch_bars(tv, symbol, Interval.in_weekly,  N_WEEKLY_BARS)
    time.sleep(SLEEP_BETWEEN)
    df_m = fetch_bars(tv, symbol, Interval.in_monthly, N_MONTHLY_BARS)
    time.sleep(SLEEP_BETWEEN)

    if df_d is None or len(df_d) < 3:
        raise ValueError(f"Insufficient daily data (got {len(df_d) if df_d is not None else 0} bars)")

    # ── Daily ───────────────────────────────────────────────────────────────
    # bar_d1 = yesterday's completed daily candle  → this is the PIVOT bar
    # bar_d2 = D-2 completed candle                → used for Max High/Low boundaries
    # bar_d3 = D-3 completed candle                → used to compute D-1's own CPR for 2-day comparison
    bar_d1 = _row_to_bar(df_d.iloc[-1])   # most recent completed bar
    bar_d2 = _row_to_bar(df_d.iloc[-2])
    bar_d3 = _row_to_bar(df_d.iloc[-3]) if len(df_d) >= 3 else bar_d2

    ltp          = bar_d1.close           # LTP = last confirmed close
    result.close = round(ltp, 2)
    result.pdh   = round(bar_d1.high, 2)  # Previous Day High
    result.pdl   = round(bar_d1.low,  2)  # Previous Day Low

    # Today's pivot levels (CPR built on D-1 bar, bounded by D-2)
    lvl_today = compute_cpr(bar_d1, bar_d2)
    result.daily_trend = determine_trend(ltp, lvl_today)

    # CPR Width % and classification (daily timeframe)
    width_pct = cpr_width_pct(lvl_today)
    result.width_classification = classify_cpr_width(width_pct)

    # D-1's pivot levels (CPR built on D-2 bar, bounded by D-3)
    lvl_d1 = compute_cpr(bar_d2, bar_d3)
    result.cpr_2day_relationship = determine_2day_cpr(lvl_today, lvl_d1)

    # ── Weekly ──────────────────────────────────────────────────────────────
    if df_w is not None and len(df_w) >= 2:
        bar_wc = _row_to_bar(df_w.iloc[-1])   # current (forming) week — already dropped, so this is last complete
        bar_wp = _row_to_bar(df_w.iloc[-2])   # previous completed week
        lvl_w  = compute_cpr(bar_wc, bar_wp)
        result.weekly_trend = determine_trend(ltp, lvl_w)

    # ── Monthly ─────────────────────────────────────────────────────────────
    if df_m is not None and len(df_m) >= 2:
        bar_mc = _row_to_bar(df_m.iloc[-1])   # current (forming) month — already dropped
        bar_mp = _row_to_bar(df_m.iloc[-2])   # previous completed month
        lvl_m  = compute_cpr(bar_mc, bar_mp)
        result.monthly_trend = determine_trend(ltp, lvl_m)

    return result


# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

def fetch_tickers(client: Client) -> list[dict]:
    """Return all rows from pivot_trend_analysis as a list of dicts."""
    response = client.table("pivot_trend_analysis").select("ticker, last_updated").execute()
    return response.data or []


def today_ist() -> date:
    return datetime.now(IST).date()


def already_updated_today(last_updated_str: Optional[str]) -> bool:
    """Return True if last_updated is today in IST."""
    if not last_updated_str:
        return False
    try:
        # Supabase returns ISO strings like "2026-03-06T14:26:12.393558+00:00"
        dt = datetime.fromisoformat(last_updated_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt_ist = dt.astimezone(IST)
        return dt_ist.date() == today_ist()
    except Exception:
        return False


def upsert_batch(client: Client, rows: list[dict]) -> None:
    table = client.table("pivot_trend_analysis")
    for i in range(0, len(rows), UPSERT_BATCH):
        batch = rows[i : i + UPSERT_BATCH]
        table.upsert(batch, on_conflict="ticker").execute()
        log.info("  Upserted rows %d–%d.", i + 1, i + len(batch))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("=" * 60)
    log.info("  Pivot & CPR Analysis  —  %s", datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"))
    log.info("=" * 60)

    # 1. Connect to Supabase
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    log.info("Supabase connected.")

    # 2. Fetch ticker list
    all_rows = fetch_tickers(supabase)
    log.info("Total tickers in DB : %d", len(all_rows))

    # 3. Filter out already-updated tickers
    to_process: list[str] = []
    skipped:    list[str] = []
    for row in all_rows:
        ticker = row["ticker"]
        if already_updated_today(row.get("last_updated")):
            skipped.append(ticker)
        else:
            to_process.append(ticker)

    log.info("Already up-to-date  : %d  (skipping)", len(skipped))
    log.info("To process          : %d", len(to_process))

    if not to_process:
        log.info("Nothing to do — all tickers already updated today.")
        return

    # 4. Connect to tvDatafeed (anonymous — no login needed for EOD data)
    log.info("Connecting to tvDatafeed …")
    tv = TvDatafeed()

    # 5. Process each ticker
    results_rows: list[dict] = []
    succeeded:    list[str]  = []
    failed:       list[tuple[str, str]] = []   # (ticker, reason)

    total = len(to_process)
    for idx, ticker in enumerate(to_process, 1):
        log.info("[%3d/%3d]  %-15s …", idx, total, ticker)
        try:
            result = analyse_ticker(tv, ticker)
            results_rows.append({
                "ticker":                result.ticker,
                "close":                 result.close,
                "monthly_trend":         result.monthly_trend,
                "weekly_trend":          result.weekly_trend,
                "daily_trend":           result.daily_trend,
                "cpr_2day_relationship": result.cpr_2day_relationship,
                "width_classification":  result.width_classification,
                "pdh":                   result.pdh,
                "pdl":                   result.pdl,
                "last_updated":          result.last_updated,
            })
            succeeded.append(ticker)
            log.info(
                "           %-15s  LTP=%-9.2f  M=%-18s  W=%-18s  D=%-18s  CPR2=%-14s  WIDTH=%s",
                ticker, result.close,
                result.monthly_trend, result.weekly_trend,
                result.daily_trend,   result.cpr_2day_relationship,
                result.width_classification,
            )

        except Exception as exc:
            reason = str(exc)
            failed.append((ticker, reason))
            log.warning("           %-15s  FAILED — %s", ticker, reason)

        # Upsert in rolling batches so we don't lose work if interrupted
        if len(results_rows) >= UPSERT_BATCH:
            log.info("  → Flushing batch of %d rows to Supabase …", len(results_rows))
            upsert_batch(supabase, results_rows)
            results_rows.clear()

    # 6. Upsert remaining rows
    if results_rows:
        log.info("  → Flushing final %d rows to Supabase …", len(results_rows))
        upsert_batch(supabase, results_rows)

    # 7. Summary
    log.info("")
    log.info("=" * 60)
    log.info("  SUMMARY")
    log.info("=" * 60)
    log.info("  Total in DB         : %d", len(all_rows))
    log.info("  Skipped (up-to-date): %d", len(skipped))
    log.info("  Processed           : %d", total)
    log.info("  ✓ Succeeded         : %d", len(succeeded))
    log.info("  ✗ Failed            : %d", len(failed))

    if failed:
        log.info("")
        log.info("  Failed tickers:")
        for ticker, reason in failed:
            log.info("    ✗  %-15s  %s", ticker, reason)

    log.info("=" * 60)

    # ── Write result JSON for GitHub Actions summary ──────────────────────────
    pathlib.Path("results").mkdir(exist_ok=True)
    pathlib.Path("results/pivot.json").write_text(json.dumps({
        "script":    "Pivot Analysis",
        "succeeded": len(succeeded),
        "failed":    len(failed),
        "skipped":   len(skipped),
        "total":     len(all_rows),
        "errors":    [t for t, _ in failed[:10]],
    }))


if __name__ == "__main__":
    main()
