"""
stage1_to_stage2_screen.py
===========================
Fast Stage 1 -> Stage 2 "young breakout" screener -- fully DB-driven, with a
two-stage volume check:

  STAGE A -- WEEKLY (the filter):
    Pulls the ticker list from 81-ish young-Stage2 candidates down to the
    ones that show genuine institutional-grade weekly volume expansion.
    Fully DB-driven (no live calls), so it's replayable via --lookback-weeks.

    Fixes vs. the old "latest week / trailing SMA(N)" version:
      1. Pre-breakout baseline -- the baseline window is pulled from BEFORE
         the Stage1->Stage2 transition started (skips the weeks_in_stage2
         most-recent weeks), not just "the last N weeks". Otherwise a stock
         4 weeks into its breakout has its own elevated volume baked into
         the baseline, understating the true spike (this was visible in the
         v1/v2 output, e.g. ADOR).
      2. Median, not mean, baseline -- robust to a single freak week (a bulk
         deal skewing one week of an otherwise-quiet baseline).
      3. Optional liquidity floor (--min-weekly-turnover) -- baseline_volume
         x close must clear a minimum rupee turnover, so a single loud print
         in a near-dead microcap (e.g. TAINWALCHM's 29x on the old logic)
         can't fake a signal.

  STAGE B -- DAILY (entry confirmation, informational only):
    For whatever survives the weekly filter, optionally fetch today's daily
    volume vs SMA(20, daily) from TradingView and flag it as "confirmed" if
    it's also hot right now. This does NOT remove anything from the list --
    it's meant to help you time entries among an already-short watchlist.
    Only meaningful for --lookback-weeks 0 (TradingView only gives you
    *today's* daily bars, so it can't be replayed for a past week).

Because it's fully DB-driven for the weekly stage, the screen is *replayable*:
pass --lookback-weeks N to run the exact same screen as it would have looked
N weekly snapshots ago (N=0 is the latest snapshot in the table).

Steps:
  1. Work out the "target" analysis_date: the latest date in
     weekly_stock_stages, walked back --lookback-weeks snapshots.
  2. Sanity-check the weekly cadence between the two most recent snapshots
     in the table -- if it isn't close to 7 days apart, the latest row might
     be a partial/mid-week candle, which would make the weekly volume check
     unreliable. This is just a heads-up log warning, not a hard stop.
  3. Pull weekly_stock_stages history (stage + close + volume per week) for
     every ticker, from far enough back to cover STAGE1_LOOKBACK_WEEKS + a
     buffer, up through the target date (nothing after it is looked at, so
     the replay is honest -- it can't see the future).
  4. For each ticker, walk the stage sequence with detect_stage2_freshness()
     (same logic the main pipeline uses) to get weeks_in_stage2 and whether
     it's a genuine, young Stage1->Stage2 transition as of the target date.
  5. Join with universe.nse_universe for market_cap, filter on close +
     market_cap.
  6. STAGE A: pre-breakout-median weekly volume filter (see above).
  7. STAGE B: optional live daily confirmation pass on the survivors.

No writes to Supabase. Results printed + saved to
results/stage1_to_stage2_screen.csv.

Requirements:
  - stage_analysis_pipeline_w.py in the same directory (reuses its Supabase/
    TradingView helpers and constants).
  - weekly_stock_stages needs enough history to look back
    STAGE1_LOOKBACK_WEEKS + YOUNG_STAGE2_MAX_WEEKS weeks (default 8 + 4 = 12
    weeks), PLUS weekly-baseline-weeks weeks before the transition, before
    the target date. Tickers with less history than that just won't be able
    to prove a Stage 1 origin / a pre-breakout baseline and will be skipped.

Usage:
    python stage1_to_stage2_screen.py
    python stage1_to_stage2_screen.py --lookback-weeks 1        # last week's watchlist
    python stage1_to_stage2_screen.py --young-weeks 4 --close-min 100 \\
        --close-max 5000 --market-cap-max 30000 --weekly-ratio-min 1.5
    python stage1_to_stage2_screen.py --min-weekly-turnover 20000000  # 2cr liquidity floor
    python stage1_to_stage2_screen.py --skip-weekly-volume       # see all young-Stage2 names
    python stage1_to_stage2_screen.py --no-daily-confirm         # weekly-only, skip live calls
"""

import argparse
import logging
import pathlib
import statistics
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Dict, List, Optional

from stage_analysis_pipeline_w import (   # reuse the pipeline's helpers/constants
    STAGES_SCHEMA, STAGES_TABLE,
    UNIVERSE_MARKET_CAP_COLUMN,
    STAGE1_LOOKBACK_WEEKS,
    MAX_WORKERS,
    Interval,
    get_supabase_client,
    fetch_all_tickers,
    detect_stage2_freshness,
    fetch_daily_volume_stats,
)

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ── Screen thresholds (CLI-overridable, see bottom of file) ──────────────────
YOUNG_STAGE2_MAX_WEEKS       = 4
SCREEN_CLOSE_MIN             = 100.0
SCREEN_CLOSE_MAX             = 5000.0
SCREEN_MARKET_CAP_MAX        = 30000.0
SCREEN_LOOKBACK_WEEKS        = 0          # 0 = latest snapshot in DB; N = N snapshots back

# Stage A -- weekly filter (the one that narrows the group)
SCREEN_SKIP_WEEKLY_VOLUME    = False
SCREEN_WEEKLY_RATIO_MIN      = 1.5        # latest week vs. pre-breakout median baseline
SCREEN_WEEKLY_BASELINE_WEEKS = 10         # size of the pre-breakout baseline window
SCREEN_MIN_WEEKLY_TURNOVER   = 0.0        # rupees; baseline_volume x close must clear this. 0 = off

# Stage B -- daily confirmation (informational, doesn't filter the list)
SCREEN_DAILY_CONFIRM         = True
SCREEN_DAILY_RATIO_MIN       = 1.5

# How many weeks of weekly_stock_stages history to pull. Needs to cover the
# lookback window used for freshness detection, plus a comfortable buffer,
# plus whatever the pre-breakout baseline needs (which itself sits *before*
# up to YOUNG_STAGE2_MAX_WEEKS weeks of breakout).
HISTORY_WEEKS_BUFFER = 12
HISTORY_WEEKS_NEEDED = (
    STAGE1_LOOKBACK_WEEKS + YOUNG_STAGE2_MAX_WEEKS
    + max(HISTORY_WEEKS_BUFFER, SCREEN_WEEKLY_BASELINE_WEEKS + YOUNG_STAGE2_MAX_WEEKS + 1)
)


def get_target_date(supabase, lookback_weeks: int) -> "tuple[str, Optional[int]]":
    """
    Figure out which analysis_date to treat as "current".

    lookback_weeks=0 -> the latest analysis_date present in the table.
    lookback_weeks=N -> the N-th most recent *distinct* analysis_date, i.e.
    what the screen would have produced N weekly snapshots ago.

    Also returns the day-gap between the two most recent distinct dates in
    the table (None if there's only one), so the caller can sanity-check
    whether the latest row looks like a properly closed week.
    """
    if lookback_weeks < 0:
        raise ValueError("--lookback-weeks must be >= 0")

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
        raise RuntimeError(
            f"No rows found in {STAGES_TABLE}. Run stage_analysis_pipeline_w.py "
            "at least once first."
        )
    if lookback_weeks >= len(seen):
        raise ValueError(
            f"--lookback-weeks {lookback_weeks} goes further back than the "
            f"{len(seen)} distinct analysis_date(s) currently in {STAGES_TABLE}."
        )

    gap_days = None
    if len(seen) >= 2:
        gap_days = (date.fromisoformat(seen[0]) - date.fromisoformat(seen[1])).days

    return seen[lookback_weeks], gap_days


def check_week_cadence(gap_days: Optional[int], target_date: str) -> None:
    """
    Heads-up only: if the gap between the two most recent stored snapshots
    isn't close to 7 days, the latest row might be a partial/mid-week candle
    (e.g. the pipeline ran mid-week), which would make weekly volume
    comparisons apples-to-oranges. Can't confirm this without seeing how
    stage_analysis_pipeline_w.py pulls its weekly bar -- just flagging it.
    """
    if gap_days is None:
        return
    if abs(gap_days - 7) > 1:
        log.warning(
            "Gap between the two most recent %s snapshots is %d day(s), not "
            "~7. If your pipeline can run mid-week, the latest stored weekly "
            "volume for %s may be a PARTIAL week -- the weekly volume filter "
            "below would then be unreliable until the week actually closes. "
            "If the pipeline only ever runs after a completed week, ignore this.",
            STAGES_TABLE, gap_days, target_date,
        )


def fetch_stage_history(supabase, cutoff_date: str, target_date: str) -> Dict[str, List[dict]]:
    """
    Pull weekly_stock_stages rows for every ticker in (cutoff_date, target_date],
    inclusive of both ends. Returns {ticker: [rows sorted oldest -> newest]}.
    Nothing after target_date is ever fetched, so a --lookback-weeks replay
    can't see "future" weeks relative to the date it's simulating.
    """
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


def weekly_volume_check(
    rows: List[dict],
    weeks_in_stage2: int,
    close: float,
    min_ratio: float,
    baseline_weeks: int,
    min_turnover: float,
) -> Optional[dict]:
    """
    Stage A: pre-breakout-median weekly volume-spike check. DB-only, no live
    calls, so it works identically whether looking at the live latest week
    or replaying a past one (`rows` must already be truncated to <= target
    date, oldest -> newest).

    latest_vol         = the target week's stored volume
    baseline           = median of `baseline_weeks` weeks, taken from BEFORE
                          the Stage1->Stage2 transition (skips the
                          weeks_in_stage2 most-recent weeks, which are
                          themselves part of the breakout and would
                          otherwise inflate the baseline)
    ratio               = latest_vol / baseline
    turnover floor      = baseline * close must clear min_turnover (if set),
                          so a single loud print in a dead/illiquid name
                          can't pass on volume ratio alone
    """
    vols = [(r["analysis_date"], r["volume"]) for r in rows if r.get("volume") is not None]

    needed = weeks_in_stage2 + baseline_weeks + 1
    if len(vols) < needed:
        return None

    latest_date, latest_vol = vols[-1]
    end_idx = -(weeks_in_stage2 + 1)          # just before the breakout weeks
    start_idx = end_idx - baseline_weeks
    pre_breakout = vols[start_idx:end_idx]
    if len(pre_breakout) < baseline_weeks:
        return None

    baseline_vals = [float(v) for _, v in pre_breakout]
    baseline_median = statistics.median(baseline_vals)
    if not baseline_median:
        return None

    ratio = float(latest_vol) / baseline_median
    if ratio < min_ratio:
        return None

    turnover = baseline_median * close
    if min_turnover > 0 and turnover < min_turnover:
        return None

    return {
        "latest_volume": float(latest_vol),
        "baseline_volume_pre_breakout": round(baseline_median, 2),
        "volume_ratio": round(ratio, 3),
        "avg_weekly_turnover": round(turnover, 2),
    }


def daily_confirmation(ticker: str, min_ratio: float) -> dict:
    """
    Stage B: live TradingView daily volume vs SMA(20, daily). Informational
    only -- never removes a ticker from the list, just flags whether today's
    action also looks hot, to help with entry timing.
    """
    try:
        latest_vol, sma20 = fetch_daily_volume_stats(ticker)
    except Exception as exc:
        log.error("X Error checking daily volume for %s: %s: %s", ticker, type(exc).__name__, exc)
        return {"daily_latest_volume": "", "daily_sma20": "", "daily_volume_ratio": "", "daily_confirmed": "ERROR"}

    if latest_vol is None or not sma20:
        return {"daily_latest_volume": "", "daily_sma20": "", "daily_volume_ratio": "", "daily_confirmed": "N/A"}

    ratio = latest_vol / sma20
    return {
        "daily_latest_volume": latest_vol,
        "daily_sma20": round(sma20, 2),
        "daily_volume_ratio": round(ratio, 3),
        "daily_confirmed": "YES" if ratio >= min_ratio else "no",
    }


def run_screen() -> "tuple[List[dict], str]":
    supabase = get_supabase_client()

    target_date, gap_days = get_target_date(supabase, SCREEN_LOOKBACK_WEEKS)
    log.info(
        "Target analysis_date = %s (--lookback-weeks %d)",
        target_date, SCREEN_LOOKBACK_WEEKS,
    )
    check_week_cadence(gap_days, target_date)

    if SCREEN_DAILY_CONFIRM and SCREEN_LOOKBACK_WEEKS != 0:
        log.warning(
            "--lookback-weeks %d is replaying a past snapshot, but daily "
            "confirmation uses LIVE TradingView data (today only) -- it "
            "can't reflect what daily volume looked like on %s. Skipping "
            "the daily-confirmation pass for this replay.",
            SCREEN_LOOKBACK_WEEKS, target_date,
        )

    log.info("Fetching universe (for market_cap) ...")
    universe    = fetch_all_tickers(supabase)
    market_caps = {r["ticker"]: r.get(UNIVERSE_MARKET_CAP_COLUMN) for r in universe}

    cutoff = (date.fromisoformat(target_date) - timedelta(weeks=HISTORY_WEEKS_NEEDED)).isoformat()
    log.info(
        "Pulling %s history from %s to %s (>= %d weeks needed) ...",
        STAGES_TABLE, cutoff, target_date, HISTORY_WEEKS_NEEDED,
    )
    history = fetch_stage_history(supabase, cutoff, target_date)
    log.info("Got history for %d tickers.", len(history))

    if not history:
        log.warning(
            "No rows found in %s up to %s. Run stage_analysis_pipeline_w.py "
            "(without --preview-only) at least once first.", STAGES_TABLE, target_date,
        )
        return [], target_date

    # ── Pure-DB pass: transition freshness + close + market cap ─────────────
    shortlist: List[dict] = []
    insufficient_history = 0
    for ticker, rows in history.items():
        stages = [r["stage"] for r in rows if r["stage"] is not None]
        if not stages:
            continue

        weeks_in_stage, stage_prev, is_fresh = detect_stage2_freshness(stages)
        latest = rows[-1]

        if latest["stage"] != 2:
            continue
        if not is_fresh or weeks_in_stage > YOUNG_STAGE2_MAX_WEEKS:
            continue

        # A run that spans the whole available history can't prove a Stage 1
        # origin -- it might just be truncated by our cutoff, not evidence of
        # an old advance. Flag it rather than silently including/excluding it.
        if stage_prev is None and len(stages) < HISTORY_WEEKS_NEEDED:
            insufficient_history += 1
            continue

        close = latest["close"]
        if close is None or not (SCREEN_CLOSE_MIN < float(close) < SCREEN_CLOSE_MAX):
            continue

        mcap = market_caps.get(ticker)
        if mcap is None or not (float(mcap) < SCREEN_MARKET_CAP_MAX):
            continue

        shortlist.append({
            "ticker": ticker,
            "close": float(close),
            "market_cap": float(mcap),
            "weeks_in_stage2": weeks_in_stage,
            "analysis_date": latest["analysis_date"],
            "_rows": rows,   # kept only for the weekly-volume check below
        })

    if insufficient_history:
        log.info(
            "%d ticker(s) skipped: not enough history yet to confirm a "
            "genuine Stage 1 origin (need >= %d weeks).",
            insufficient_history, HISTORY_WEEKS_NEEDED,
        )

    log.info(
        "%d tickers pass young-Stage2 + close(%.0f-%.0f) + market_cap(<%.0f) "
        "from DB alone.",
        len(shortlist), SCREEN_CLOSE_MIN, SCREEN_CLOSE_MAX, SCREEN_MARKET_CAP_MAX,
    )

    if not shortlist:
        return [], target_date

    # ── Stage A: weekly volume filter (narrows the group) ───────────────────
    if SCREEN_SKIP_WEEKLY_VOLUME:
        log.info("Skipping weekly volume filter as requested (--skip-weekly-volume).")
        matches = shortlist
        for row in matches:
            row.pop("_rows", None)
            row.update({
                "latest_volume": "", "baseline_volume_pre_breakout": "",
                "volume_ratio": "", "avg_weekly_turnover": "",
            })
    else:
        log.info(
            "Checking weekly volume: latest week >= %.1fx median of the %d "
            "pre-breakout week(s)%s ...",
            SCREEN_WEEKLY_RATIO_MIN, SCREEN_WEEKLY_BASELINE_WEEKS,
            f", turnover floor Rs {SCREEN_MIN_WEEKLY_TURNOVER:,.0f}" if SCREEN_MIN_WEEKLY_TURNOVER > 0 else "",
        )
        matches = []
        for row in shortlist:
            vol_info = weekly_volume_check(
                row.pop("_rows"),
                row["weeks_in_stage2"],
                row["close"],
                SCREEN_WEEKLY_RATIO_MIN,
                SCREEN_WEEKLY_BASELINE_WEEKS,
                SCREEN_MIN_WEEKLY_TURNOVER,
            )
            if vol_info:
                row.update(vol_info)
                matches.append(row)

    log.info(
        "%d ticker(s) remain after the weekly volume filter -- %s.",
        len(matches),
        "running daily confirmation" if (SCREEN_DAILY_CONFIRM and SCREEN_LOOKBACK_WEEKS == 0 and matches)
        else "skipping daily confirmation",
    )

    if not matches:
        return [], target_date

    # ── Stage B: daily confirmation (informational only) ────────────────────
    if SCREEN_DAILY_CONFIRM and SCREEN_LOOKBACK_WEEKS == 0:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(daily_confirmation, row["ticker"], SCREEN_DAILY_RATIO_MIN): row for row in matches}
            for future in as_completed(futures):
                row = futures[future]
                row.update(future.result())
    else:
        for row in matches:
            row.update({
                "daily_latest_volume": "", "daily_sma20": "",
                "daily_volume_ratio": "", "daily_confirmed": "N/A",
            })

    for row in matches:
        vr = row["volume_ratio"]
        vr_str = f"{vr:.2f}x" if isinstance(vr, (int, float)) else "N/A"
        log.info(
            "  MATCH %-12s | Close: %-9.2f | MCap: %-10.2f | Weekly Vol Ratio: %-7s | "
            "Weeks in Stage 2: %d | Daily confirm: %s",
            row["ticker"], row["close"], row["market_cap"], vr_str,
            row["weeks_in_stage2"], row["daily_confirmed"],
        )

    return matches, target_date


def write_report(matches: List[dict], target_date: str) -> None:
    pathlib.Path("results").mkdir(exist_ok=True)
    suffix = "" if SCREEN_LOOKBACK_WEEKS == 0 else f"_{target_date}"
    out_path = pathlib.Path(f"results/stage1_to_stage2_screen{suffix}.csv")

    matches = sorted(
        matches,
        key=lambda r: float(r["volume_ratio"]) if isinstance(r["volume_ratio"], (int, float)) else -1.0,
        reverse=True
    )
    header = [
        "ticker", "close", "market_cap", "weeks_in_stage2",
        "latest_volume", "baseline_volume_pre_breakout", "volume_ratio", "avg_weekly_turnover",
        "daily_latest_volume", "daily_sma20", "daily_volume_ratio", "daily_confirmed",
        "analysis_date",
    ]
    lines = [",".join(header)]
    for r in matches:
        lines.append(",".join(str(r[h]) for h in header))
    out_path.write_text("\n".join(lines))

    print()
    print("=" * 78)
    print(f"  STAGE 1 -> STAGE 2 YOUNG BREAKOUT SCREEN -- {len(matches)} match(es)")
    print(f"  As of: {target_date}  (--lookback-weeks {SCREEN_LOOKBACK_WEEKS})")
    weekly_str = (
        "skipped"
        if SCREEN_SKIP_WEEKLY_VOLUME
        else f">= {SCREEN_WEEKLY_RATIO_MIN:.1f}x median({SCREEN_WEEKLY_BASELINE_WEEKS}wk pre-breakout)"
    )
    if SCREEN_MIN_WEEKLY_TURNOVER > 0 and not SCREEN_SKIP_WEEKLY_VOLUME:
        weekly_str += f", turnover >= Rs {SCREEN_MIN_WEEKLY_TURNOVER:,.0f}"
    daily_str = (
        f">= {SCREEN_DAILY_RATIO_MIN:.1f}x SMA(20d)"
        if (SCREEN_DAILY_CONFIRM and SCREEN_LOOKBACK_WEEKS == 0)
        else "skipped (informational only, lookback!=0 or disabled)"
    )
    print(f"  Filters: close in ({SCREEN_CLOSE_MIN:.0f}, {SCREEN_CLOSE_MAX:.0f}) | "
          f"market_cap < {SCREEN_MARKET_CAP_MAX:.0f} | "
          f"weeks_in_stage2 <= {YOUNG_STAGE2_MAX_WEEKS}")
    print(f"  Weekly volume (Stage A, filters the list): {weekly_str}")
    print(f"  Daily confirmation (Stage B, informational): {daily_str}")
    print("=" * 78)
    for r in matches:
        vr = r["volume_ratio"]
        vr_str = f"{vr:.2f}x" if isinstance(vr, (int, float)) else "N/A"
        dr = r.get("daily_volume_ratio")
        dr_str = f"{dr:.2f}x" if isinstance(dr, (int, float)) else "N/A"
        print(
            f"  {r['ticker']:<12} close={r['close']:<9.2f} mcap={r['market_cap']:<10.2f} "
            f"weeks_in_st2={r['weeks_in_stage2']:<3} wk_vol_ratio={vr_str:<8} "
            f"daily_ratio={dr_str:<8} confirm={r.get('daily_confirmed', 'N/A')}"
        )
    print(f"  Saved to {out_path}")
    print("=" * 78)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fast, DB-driven Stage1->Stage2 young breakout screener "
                    "with a pre-breakout weekly volume filter + optional live daily confirmation."
    )
    parser.add_argument("--lookback-weeks", type=int, default=SCREEN_LOOKBACK_WEEKS,
                         help="0 = latest snapshot in DB (default). N = replay the screen as of "
                              "N weekly snapshots ago, e.g. 1 = last week's watchlist.")
    parser.add_argument("--young-weeks", type=int, default=YOUNG_STAGE2_MAX_WEEKS)
    parser.add_argument("--close-min", type=float, default=SCREEN_CLOSE_MIN)
    parser.add_argument("--close-max", type=float, default=SCREEN_CLOSE_MAX)
    parser.add_argument("--market-cap-max", type=float, default=SCREEN_MARKET_CAP_MAX)

    parser.add_argument("--weekly-ratio-min", type=float, default=SCREEN_WEEKLY_RATIO_MIN,
                         help="Stage A: latest week's volume must be >= this x the pre-breakout "
                              "baseline median. This is the filter that narrows the group.")
    parser.add_argument("--weekly-baseline-weeks", type=int, default=SCREEN_WEEKLY_BASELINE_WEEKS,
                         help="Size of the pre-breakout baseline window (weeks), taken from "
                              "BEFORE the Stage1->Stage2 transition, not just 'trailing N weeks'.")
    parser.add_argument("--min-weekly-turnover", type=float, default=SCREEN_MIN_WEEKLY_TURNOVER,
                         help="Liquidity floor in rupees: baseline_volume x close must clear this. "
                              "0 (default) = off. Try e.g. 20000000 (2cr) to filter out illiquid names.")
    parser.add_argument("--skip-weekly-volume", action="store_true",
                         help="Bypass the weekly volume filter entirely (see the full young-Stage2 list).")

    parser.add_argument("--daily-confirm", dest="daily_confirm", action="store_true", default=SCREEN_DAILY_CONFIRM,
                         help="Stage B: fetch live daily volume vs SMA(20) for whatever survives the weekly "
                              "filter, as an informational entry-timing signal (default: on, --lookback-weeks 0 only).")
    parser.add_argument("--no-daily-confirm", dest="daily_confirm", action="store_false",
                         help="Disable the live daily-confirmation pass.")
    parser.add_argument("--daily-ratio-min", type=float, default=SCREEN_DAILY_RATIO_MIN,
                         help="Threshold for daily_confirmed=YES (informational, doesn't filter the list).")

    args = parser.parse_args()

    SCREEN_LOOKBACK_WEEKS        = args.lookback_weeks
    YOUNG_STAGE2_MAX_WEEKS       = args.young_weeks
    SCREEN_CLOSE_MIN             = args.close_min
    SCREEN_CLOSE_MAX             = args.close_max
    SCREEN_MARKET_CAP_MAX        = args.market_cap_max

    SCREEN_WEEKLY_RATIO_MIN      = args.weekly_ratio_min
    SCREEN_WEEKLY_BASELINE_WEEKS = args.weekly_baseline_weeks
    SCREEN_MIN_WEEKLY_TURNOVER   = args.min_weekly_turnover
    SCREEN_SKIP_WEEKLY_VOLUME    = args.skip_weekly_volume

    SCREEN_DAILY_CONFIRM         = args.daily_confirm
    SCREEN_DAILY_RATIO_MIN       = args.daily_ratio_min

    HISTORY_WEEKS_NEEDED = (
        STAGE1_LOOKBACK_WEEKS + YOUNG_STAGE2_MAX_WEEKS
        + max(HISTORY_WEEKS_BUFFER, SCREEN_WEEKLY_BASELINE_WEEKS + YOUNG_STAGE2_MAX_WEEKS + 1)
    )

    results, target_date_used = run_screen()
    write_report(results, target_date_used)
