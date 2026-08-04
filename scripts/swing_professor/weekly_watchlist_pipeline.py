"""
weekly_watchlist_pipeline.py
============================
Weekly Swing-Trading Watchlist Pipeline Orchestrator.

Orchestrates Components B through G into a cohesive weekly watchlist workflow:
  1. Load persisted watchlist state (WatchlistStore SQLite) -> active_watchlist_tickers
  2. Component B: Run momentum scan fresh against the universe -> momentum_scan_survivors
     (caches daily bars per ticker)
  3. Component C: Run young Stage-2 screen fresh against the universe -> young_stage2_tickers
  4. Candidate Set: Deduplicate (active_watchlist_tickers ∪ momentum_scan_survivors ∪ young_stage2_tickers)
  5. Component D: Run unified weekly Clean Base check (clean_base_lib) on candidate_set with MIN_EXPANSION_NUMBER=1
  6. Component E: Run position-weighted purple/red-dot scoring on current leg (dot_scoring)
     (daily bars reused from Component B cache; optionally fetched for young-only via --score-young-daily-bars)
  7. Component F: Compute composite ranking score for every candidate (composite_scoring)
  8. Component G: Apply lifecycle table decisions (lifecycle) -> update WatchlistStore
  9. Persist updated watchlist state to SQLite
  10. Generate weekly markdown summary report with 4 sections: Added, Kept, Promoted, Removed
"""

import argparse
import json
import logging
import os
import pathlib
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
from dotenv import load_dotenv

# ── Domain modules ────────────────────────────────────────────────────────────
from clean_base_lib import (
    DEPTH_HARD_CEILING_PCT,
    BASE_DURATION_MANUAL_REVIEW_WEEKS,
    MIN_BASE_WEEKS,
    MA_PERIOD_WEEKS,
    MA_PERIOD_DAYS_50DMA,
    ZIGZAG_THRESHOLD_PCT,
    find_stage2_epoch_start,
    zigzag_pivots,
    evaluate_current_base,
    check_daily_50dma_respect,
)
from composite_scoring import (
    SCORING_CONFIG,
    compute_composite_score,
    normalize_scores,
    rank_candidates,
)
from dot_scoring import (
    compute_leg_dot_score,
    count_dots_in_window,
    PURPLE_DOT_MOVE_PCT,
    PURPLE_DOT_MIN_VOLUME,
    RED_DOT_PENALTY_MULTIPLIER,
)
from stage2_checklist import (
    CHECKLIST_MIN_PURPLE_DOTS,
    check_moving_averages_sloping_up,
    check_prior_force,
    check_pullback_depth,
)
from lifecycle import (
    ACTION_ADD,
    ACTION_DELIST,
    ACTION_IGNORE,
    ACTION_KEEP,
    ACTION_PROMOTE,
    ACTION_REMOVE,
    decide_lifecycle,
)
from supabase_watchlist_store import SupabaseWatchlistStore
from weekly_report_writer import generate_weekly_report

# ── Upstream screener / DB imports ────────────────────────────────────────────
from stage_analysis_pipeline_w import (
    STAGES_SCHEMA,
    STAGES_TABLE,
    UNIVERSE_MARKET_CAP_COLUMN,
    STAGE1_LOOKBACK_WEEKS,
    MAX_WORKERS,
    get_supabase_client,
    fetch_all_tickers,
    detect_stage2_freshness,
)
from momentum_scanner_tvdatafeed import (
    run_scan as run_momentum_scan,
    get_daily_history_with_retry,
    PRICE_MIN as MOMENTUM_PRICE_MIN,
)
from stage2_data_lib import (
    fetch_weekly_bars_with_retry,
    fetch_universe,
    fetch_stage_history,
    get_target_date,
    EPOCH_HISTORY_WEEKS,
    WEEKLY_SMA_WARMUP_BARS,
)
import stage1_to_stage2_screen_v3 as young_screener

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%H:%M:%S",
    force=True,
)
log = logging.getLogger("weekly_watchlist_pipeline")


# ── Step 5: Clean Base Evaluator Worker ────────────────────────────────────────
def evaluate_candidate_base(
    ticker: str,
    epoch_start_date: str,
    lookback_weeks: int = 0,
) -> Tuple[str, Optional[dict], Optional[str]]:
    """Fetch weekly bars and evaluate base quality for a single candidate.
    
    Uses MIN_EXPANSION_NUMBER = 1 (unlike standalone stage2_expansion_screen
    which filters for leg 2+).
    """
    weeks_since_epoch = max(
        1, (date.today() - date.fromisoformat(epoch_start_date)).days // 7
    )
    if lookback_weeks > 0:
        weeks_since_epoch += lookback_weeks
    n_bars = min(weeks_since_epoch + WEEKLY_SMA_WARMUP_BARS + 10, 500)

    weekly_df = fetch_weekly_bars_with_retry(ticker, n_bars)
    if weekly_df is None or weekly_df.empty:
        return ticker, None, "no_weekly_data"

    if lookback_weeks > 0:
        if len(weekly_df) <= lookback_weeks:
            return ticker, None, "insufficient_history_for_replay"
        weekly_df = weekly_df.iloc[:-lookback_weeks]

    weekly_df["sma_10w"] = weekly_df["close"].rolling(
        window=MA_PERIOD_WEEKS, min_periods=MA_PERIOD_WEEKS
    ).mean()

    epoch_start_ts = pd.Timestamp(epoch_start_date)
    on_or_after = weekly_df.index[weekly_df.index >= epoch_start_ts]
    if len(on_or_after) == 0:
        return ticker, None, "epoch_start_not_in_weekly_range"

    epoch_weekly = weekly_df.loc[on_or_after[0]:].copy()
    if len(epoch_weekly) < MIN_BASE_WEEKS + 4:
        return ticker, None, "epoch_too_short_on_weekly_bars"

    if epoch_weekly["sma_10w"].isna().all():
        return ticker, None, "insufficient_sma10w_warmup"

    pivots = zigzag_pivots(epoch_weekly["close"].tolist(), ZIGZAG_THRESHOLD_PCT)
    result = evaluate_current_base(epoch_weekly, pivots, epoch_weekly.index)

    if result is None:
        return ticker, None, "no_active_base"

    result["ticker"] = ticker
    result["latest_close"] = round(float(epoch_weekly["close"].iloc[-1]), 2)

    # ── Stage 2 Checklist (weekly-only criteria) ──────────────────────────
    # Full weekly_df (not epoch_weekly) so the 20-week SMA slope has enough
    # pre-epoch warmup, especially for young epochs. Only meaningful once a
    # real base exists (hard_reject is None) -- daily-bar criteria (purple
    # dot count, red dots in pullback) are completed later in Component E,
    # which already fetches daily bars for dot scoring.
    if result.get("hard_reject") is None:
        # MA slope evaluated AT THE LEG'S PEAK (last_high_date), not today --
        # checking "today" routinely fails on genuinely healthy pullbacks,
        # since the 10-week MA naturally flattens/dips while price
        # consolidates (confirmed empirically: this failed half of 14 real
        # Category A candidates on a negative 10w slope alone, even with
        # textbook-ideal base depth). This tests "was this a genuine uptrend
        # when the rally leg completed," matching how Prior Force and depth
        # are already evaluated at/around the peak, not today.
        last_high_ts = pd.Timestamp(result["last_high_date"])
        closes_up_to_peak = weekly_df.loc[weekly_df.index <= last_high_ts, "close"]
        ma_check = check_moving_averages_sloping_up(closes_up_to_peak)
        force_check = check_prior_force(result["current_leg_pct"])
        depth_check = check_pullback_depth(result["base_depth_pct"])
        result["slope_10w"] = ma_check.get("slope_10w")
        result["slope_20w"] = ma_check.get("slope_20w")
        result["checklist_weekly_passed"] = (
            ma_check["passed"] and force_check["passed"] and depth_check["passed"]
        )
        result["checklist_weekly_failed_reasons"] = [
            r for r in (
                None if ma_check["passed"] else (ma_check.get("reason") or "sma_not_sloping_up"),
                None if force_check["passed"] else force_check["reason"],
                None if depth_check["passed"] else depth_check["reason"],
            ) if r
        ]

    return ticker, result, None



# ── Main Orchestration ────────────────────────────────────────────────────────
def run_weekly_pipeline(
    db_path: str = "watchlist.db",
    lookback_weeks: int = 0,
    score_young_daily_bars: bool = False,
    skip_momentum_scan: bool = False,
    skip_young_scan: bool = False,
    reports_dir: str = "reports",
    store_backend: str = "sqlite",
) -> dict:
    """Execute the full 10-step weekly watchlist pipeline."""
    load_dotenv(".env.local")
    supabase = get_supabase_client()
    today_str = date.today().isoformat()
    if store_backend == "supabase":
        store = SupabaseWatchlistStore(supabase)
    else:
        # Lazy import -- production always runs --store-backend supabase, so
        # watchlist_store.py (SQLite) doesn't need to be present in a
        # deployed copy that never takes this branch.
        from watchlist_store import WatchlistStore
        store = WatchlistStore(db_path=db_path)

    log.info("=" * 78)
    log.info("WEEKLY WATCHLIST PIPELINE — RUN START (%s)", today_str)
    log.info("Store backend: %s | Database: %s | Lookback weeks: %d | Score young daily: %s",
             store_backend, db_path, lookback_weeks, score_young_daily_bars)
    log.info("=" * 78)

    # ── Step 1: Load active watchlist ─────────────────────────────────────────
    active_watchlist = store.get_active()
    active_lookup = {r["ticker"]: r for r in active_watchlist}
    log.info("Step 1: Loaded %d ACTIVE ticker(s) from persistent watchlist store.", len(active_watchlist))

    # ── Step 2: Component B — Momentum Scanner (caches daily bars) ────────────
    daily_bars_cache: Dict[str, pd.DataFrame] = {}
    momentum_survivors: Dict[str, dict] = {}

    if not skip_momentum_scan:
        log.info("Step 2: Running Component B (Momentum Scanner with daily bars cache) ...")
        try:
            mom_df = run_momentum_scan(
                from_universe=True,
                universe_min_price=MOMENTUM_PRICE_MIN,
                save_csv=False,
                bars_cache=daily_bars_cache,
            )
            if not mom_df.empty:
                for _, row in mom_df.iterrows():
                    momentum_survivors[row["ticker"]] = dict(row)
            log.info("Component B finished: %d momentum survivor(s), %d daily bar cache entries.",
                     len(momentum_survivors), len(daily_bars_cache))
        except Exception as exc:
            log.error("Component B failed: %s", exc, exc_info=True)
    else:
        log.info("Step 2: Skipping Component B as requested.")

    # ── Step 3: Component C — Young Stage 1->2 Breakouts ──────────────────────
    young_stage2_matches: Dict[str, dict] = {}
    if not skip_young_scan:
        log.info("Step 3: Running Component C (Young Stage 1->2 Screen) ...")
        try:
            matches, _ = young_screener.run_screen()
            for m in matches:
                young_stage2_matches[m["ticker"]] = m
            log.info("Component C finished: %d young Stage-2 match(es).", len(young_stage2_matches))
        except Exception as exc:
            log.error("Component C failed: %s", exc, exc_info=True)
    else:
        log.info("Step 3: Skipping Component C as requested.")

    # ── Step 4: Candidate Set Union ───────────────────────────────────────────
    # candidate_set = dedupe(ACTIVE ∪ momentum_survivors ∪ young_stage2_matches)
    all_candidate_tickers = set(active_lookup.keys()) | set(momentum_survivors.keys()) | set(young_stage2_matches.keys())
    log.info("Step 4: Candidate Set Union: %d unique tickers to evaluate.", len(all_candidate_tickers))
    log.info("  -> Active on watchlist: %d", len(active_lookup))
    log.info("  -> In momentum scan:    %d", len(momentum_survivors))
    log.info("  -> Young Stage 2:       %d", len(young_stage2_matches))

    # Fetch stage history for the entire candidate set
    cutoff = (date.fromisoformat(today_str) - timedelta(weeks=EPOCH_HISTORY_WEEKS)).isoformat()
    history = fetch_stage_history(supabase, cutoff, today_str)
    universe_map = fetch_universe(supabase)

    # ── Step 4b: Component D0 — Full-Universe Stage 2 Epoch Pre-Scan ──────────
    # Momentum survivors + young breakouts alone are too narrow a candidate
    # source: an established Stage 2 stock on its 2nd/3rd expansion leg with a
    # genuinely clean base goes unnoticed indefinitely if it doesn't ALSO
    # currently lead this week's momentum scan or fall inside the 4-week young
    # window. Confirmed empirically before this was added: 253/2964 active
    # universe tickers had a valid Stage 2 epoch on a given week, but only 34
    # of those were ever in the momentum/young candidate set -- real,
    # well-known tickers with clean 2nd/3rd-expansion bases (e.g. FINCABLES on
    # leg #3) were structurally never evaluated. This scans every active
    # universe ticker with the SAME find_stage2_epoch_start used everywhere
    # else in this pipeline (not a separate/stale duplicate), and adds any
    # with a valid epoch to the candidate set -- additive to momentum/young,
    # never a replacement for them.
    full_universe_epoch_tickers: Set[str] = set()
    for ticker in universe_map:
        rows = history.get(ticker, [])
        if not rows:
            continue
        epoch, _reason = find_stage2_epoch_start(rows)
        if epoch is not None:
            full_universe_epoch_tickers.add(ticker)

    newly_added = full_universe_epoch_tickers - all_candidate_tickers
    log.info(
        "Step 4b: Full-universe Stage 2 epoch pre-scan: %d/%d active tickers have a valid epoch -> "
        "%d new candidate(s) not already in the momentum/young candidate set.",
        len(full_universe_epoch_tickers), len(universe_map), len(newly_added),
    )
    all_candidate_tickers |= full_universe_epoch_tickers
    log.info("Step 4b: Candidate set expanded to %d unique tickers total.", len(all_candidate_tickers))

    # ── Step 5: Component D — Unified Clean Base Check on candidate_set ───────
    log.info("Step 5: Running Component D (Clean Base Evaluation) on candidate set (%d candidates) ...", len(all_candidate_tickers))
    base_results: Dict[str, dict] = {}
    base_status: Dict[str, str] = {}  # ticker -> outcome status
    epoch_skip_counts: Dict[str, int] = defaultdict(int)

    # Prepare candidates with valid epoch
    eval_queue = []
    for ticker in all_candidate_tickers:
        rows = history.get(ticker, [])
        if not rows:
            epoch_skip_counts["no_weekly_stock_stages_rows"] += 1
            base_status[ticker] = "no_weekly_stock_stages_rows"
            continue

        epoch, reason = find_stage2_epoch_start(rows)
        if epoch is None and reason == "mature_run_at_data_edge":
            if len(rows) < EPOCH_HISTORY_WEEKS * 0.75:
                reason = "insufficient_history_available"

        if epoch is None:
            reason_key = reason or "no_valid_stage2_epoch"
            epoch_skip_counts[reason_key] += 1
            base_status[ticker] = reason_key
            continue

        eval_queue.append((ticker, epoch["epoch_start_date"]))

    log.info(
        "Step 5 Funnel: %d in candidate_set -> %d queued for weekly-bar fetch (%d skipped during epoch detection)",
        len(all_candidate_tickers), len(eval_queue), len(all_candidate_tickers) - len(eval_queue),
    )
    for reason, count in sorted(epoch_skip_counts.items(), key=lambda x: -x[1]):
        log.info("    - %d candidate(s) skipped: %s", count, reason)

    fetch_failures: Dict[str, int] = defaultdict(int)
    if eval_queue:
        log.info("Fetching weekly bars and evaluating bases for %d queued candidate(s) ...", len(eval_queue))
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(evaluate_candidate_base, ticker, epoch_start, lookback_weeks): ticker
                for ticker, epoch_start in eval_queue
            }
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    res_ticker, res_dict, error = future.result()
                    if res_dict:
                        base_results[ticker] = res_dict
                        base_status[ticker] = res_dict.get("quality_flag", "OK")
                    else:
                        fail_reason = error or "no_result"
                        fetch_failures[fail_reason] += 1
                        base_status[ticker] = fail_reason
                except Exception as exc:
                    err_msg = f"error: {type(exc).__name__}"
                    fetch_failures[err_msg] += 1
                    base_status[ticker] = err_msg

    clean_or_ok = sum(
        1 for r in base_results.values()
        if r.get("quality_flag") in ("CLEAN", "OK") and not r.get("hard_reject")
    )
    log.info(
        "Step 5 Evaluation Complete: %d candidates queued -> %d fetched -> %d evaluated -> %d passed clean/ok.",
        len(eval_queue),
        len(eval_queue) - sum(fetch_failures.values()),
        len(base_results),
        clean_or_ok,
    )
    if fetch_failures:
        for reason, count in sorted(fetch_failures.items(), key=lambda x: -x[1]):
            log.info("    - %d fetch/eval failure(s): %s", count, reason)

    # ── Step 6: Component E — Position-Weighted Dot Scoring ───────────────────
    log.info("Step 6: Running Component E (Position-Weighted Dot Scoring) ...")
    dot_scores: Dict[str, Optional[float]] = {}
    # Stage 2 Checklist -- daily-bar criteria (purple dot count, red dots in
    # pullback, daily 50DMA respect) completed here since daily bars are
    # already being fetched/cached for dot scoring; combined with the
    # weekly-only criteria already stored on `res` by evaluate_candidate_base.
    checklist_results: Dict[str, dict] = {}

    for ticker in all_candidate_tickers:
        res = base_results.get(ticker)
        if not res or not res.get("prior_low_date") or not res.get("last_high_date"):
            dot_scores[ticker] = None
            continue

        daily_df = daily_bars_cache.get(ticker)
        # If not cached from Component B, fetch if flag is set
        if daily_df is None and score_young_daily_bars:
            daily_df, _ = get_daily_history_with_retry(ticker)
            if daily_df is not None:
                daily_bars_cache[ticker] = daily_df

        # get_daily_history_with_retry() (momentum_scanner_tvdatafeed.py) does
        # df.reset_index().rename(columns={"index": "datetime", ...}) before
        # returning/caching -- every cached daily_df has "datetime" as a plain
        # column with a fresh RangeIndex, NOT a DatetimeIndex. compute_leg_dot_score
        # and count_dots_in_window both require a real DatetimeIndex for their
        # date-range filtering; this was a latent bug the whole session (crashes
        # with "'>=' not supported between numpy.ndarray and Timestamp") that
        # never surfaced until a real evaluated base actually reached this line
        # for the first time.
        if daily_df is not None and not daily_df.empty and "datetime" in daily_df.columns:
            daily_df = daily_df.set_index(pd.to_datetime(daily_df["datetime"]))

        if daily_df is not None and not daily_df.empty:
            _, _, leg_score = compute_leg_dot_score(
                daily_df=daily_df,
                leg_start_date=res["prior_low_date"],
                leg_end_date=res["last_high_date"],
            )
            dot_scores[ticker] = leg_score
        else:
            dot_scores[ticker] = None

        # Checklist only applies to candidates with a real (non-hard-rejected)
        # base -- matches evaluate_candidate_base's weekly-checklist gate.
        if res.get("hard_reject") is not None or "checklist_weekly_passed" not in res:
            continue

        purple_dot_count = 0
        red_dot_count_in_pullback = 0
        daily_50dma_status = None
        if daily_df is not None and not daily_df.empty:
            purple_dot_count, _ = count_dots_in_window(
                daily_df, res["prior_low_date"], res["last_high_date"]
            )
            _, red_dot_count_in_pullback = count_dots_in_window(
                daily_df, res["last_high_date"], today_str
            )

            daily_df_50dma = daily_df.copy()
            daily_df_50dma["sma_50d"] = daily_df_50dma["close"].rolling(
                window=MA_PERIOD_DAYS_50DMA, min_periods=MA_PERIOD_DAYS_50DMA
            ).mean()
            base_start_ts = pd.Timestamp(res["last_high_date"])
            daily_base_slice = daily_df_50dma.loc[daily_df_50dma.index >= base_start_ts]
            if not daily_base_slice.empty and not daily_base_slice["sma_50d"].isna().all():
                daily_50dma_status, _ = check_daily_50dma_respect(daily_base_slice)

        checklist_daily_passed = (
            purple_dot_count >= CHECKLIST_MIN_PURPLE_DOTS and red_dot_count_in_pullback == 0
        )
        daily_failed_reasons = []
        if purple_dot_count < CHECKLIST_MIN_PURPLE_DOTS:
            daily_failed_reasons.append(f"purple_dots {purple_dot_count} < {CHECKLIST_MIN_PURPLE_DOTS} min")
        if red_dot_count_in_pullback != 0:
            daily_failed_reasons.append(f"red_dots_in_pullback {red_dot_count_in_pullback} > 0")

        checklist_results[ticker] = {
            "passed": res["checklist_weekly_passed"] and checklist_daily_passed,
            "failed_reasons": res["checklist_weekly_failed_reasons"] + daily_failed_reasons,
            "purple_dot_count": purple_dot_count,
            "red_dot_count_in_pullback": red_dot_count_in_pullback,
            "daily_50dma_status": daily_50dma_status,
        }

    checklist_passed_count = sum(1 for c in checklist_results.values() if c["passed"])
    log.info(
        "Component E complete: %d ticker(s) with a real base checked against the Stage 2 Checklist, %d passed.",
        len(checklist_results), checklist_passed_count,
    )

    # ── Step 7: Component F — Composite Ranking ───────────────────────────────
    log.info("Step 7: Running Component F (Composite Scoring) ...")
    scored_candidates = []
    for ticker in all_candidate_tickers:
        res = base_results.get(ticker)
        is_young = ticker in young_stage2_matches
        young_info = young_stage2_matches.get(ticker, {})

        checklist = checklist_results.get(ticker)
        checklist_failed = res is not None and res.get("hard_reject") is None and checklist is not None and not checklist["passed"]

        # Candidate is scored if it:
        # (1) passed Component D as CLEAN or OK AND the Stage 2 Checklist, OR
        # (2) is a fresh young Stage 2 breakout (pending base evaluation)
        # Checklist failure excludes exactly like hard_reject does -- it's a
        # hard pre-filter gate, not a soft score (see stage2_checklist.py).
        if res:
            quality_flag = res.get("quality_flag")
            if quality_flag not in ("CLEAN", "OK") or res.get("hard_reject") or checklist_failed:
                if not is_young:
                    continue
                if res.get("hard_reject") or checklist_failed:
                    continue
        else:
            if not is_young:
                continue

        mom_info = momentum_survivors.get(ticker)
        mom_score = float(mom_info["score"]) if mom_info and "score" in mom_info else None

        candidate_record = {
            "ticker": ticker,
            "quality_flag": res.get("quality_flag") if res else None,
            "depth_tier": res.get("base_depth_tier") if res else None,
            "base_depth_pct": res.get("base_depth_pct") if res else None,
            "base_duration_weeks": res.get("base_duration_weeks") if res else None,
            "expansion_number": res.get("expansion_number") if res else (1 if is_young else None),
            "base_category": res.get("base_category") if res else None,
            "ma_respect_status": res.get("ma_respect_status") if res else None,
            "ma_respect_status_daily_50dma": checklist.get("daily_50dma_status") if checklist else None,
            "checklist_passed": checklist.get("passed") if checklist else None,
            "slope_10w": res.get("slope_10w") if res else None,
            "slope_20w": res.get("slope_20w") if res else None,
            "purple_dot_count": checklist.get("purple_dot_count") if checklist else None,
            "is_young_stage2": is_young,
            "weeks_in_stage2": young_info.get("weeks_in_stage2"),
            "weekly_vol_ratio": young_info.get("volume_ratio"),
            "in_momentum_scan": ticker in momentum_survivors,
            "leg_dot_score": dot_scores.get(ticker),
            "momentum_score": mom_score,
            "extra_data": {
                "prior_low_date": res.get("prior_low_date") if res else None,
                "last_high_date": res.get("last_high_date") if res else None,
                "latest_close": res.get("latest_close") if res else None,
                "vcp_ratio": res.get("vcp_ratio") if res else None,
                "checklist_failed_reasons": checklist.get("failed_reasons") if checklist else None,
            }
        }
        scored_candidates.append(candidate_record)

    ranked_candidates = rank_candidates(scored_candidates)
    ranked_lookup = {c["ticker"]: c for c in ranked_candidates}
    log.info("Component F complete: %d candidates scored and ranked.", len(ranked_candidates))

    # ── Step 8 & 9: Component G — Lifecycle Decision & Persistence ────────────
    log.info("Step 8 & 9: Applying Component G Lifecycle Table and Persisting ...")
    added_list = []
    kept_list = []
    promoted_list = []
    removed_list = []

    for ticker in all_candidate_tickers:
        current_rec = active_lookup.get(ticker)
        current_status = current_rec["status"] if current_rec else None

        res = base_results.get(ticker)
        status_str = base_status.get(ticker, "")

        quality_flag = res.get("quality_flag") if res else None
        checklist = checklist_results.get(ticker)
        # Checklist failure is treated exactly like a hard_reject for
        # lifecycle purposes too -- it's a hard gate, not a soft score (see
        # Component F above, same reasoning applied consistently here).
        checklist_failed = res is not None and res.get("hard_reject") is None and checklist is not None and not checklist["passed"]
        is_hard_reject = (
            (res is not None and bool(res.get("hard_reject")))
            or "hard_reject" in status_str
            or checklist_failed
        )
        has_active_base = res is not None and res.get("hard_reject") is None
        in_universe = ticker in universe_map

        is_young = ticker in young_stage2_matches
        young_info = young_stage2_matches.get(ticker, {})

        # Compute stage and pending age if active
        ticker_history = history.get(ticker, [])
        current_stage = ticker_history[-1]["stage"] if ticker_history else None

        if current_rec and current_stage is None:
            log.warning("%s: no weekly_stock_stages row this week, stage unverified", ticker)

        was_pending = False
        weeks_pending = 0
        if current_rec:
            was_pending = current_rec.get("quality_flag") is None
            first_added = current_rec.get("first_added_date")
            if first_added:
                try:
                    d_first = date.fromisoformat(first_added)
                    d_today = date.fromisoformat(today_str)
                    weeks_pending = max(0, (d_today - d_first).days // 7)
                except Exception:
                    weeks_pending = 0

        action = decide_lifecycle(
            current_status=current_status,
            quality_flag=quality_flag,
            has_active_base=has_active_base,
            is_hard_reject=is_hard_reject,
            in_universe=in_universe,
            is_young_stage2=is_young,
            current_stage=current_stage,
            weeks_pending=weeks_pending,
            max_pending_weeks=8,
            was_pending=was_pending,
        )

        candidate_data = ranked_lookup.get(ticker) or {
            "ticker": ticker,
            "quality_flag": quality_flag,
            "composite_score": None,
            "expansion_number": res.get("expansion_number") if res else (1 if is_young else None),
            "base_depth_pct": res.get("base_depth_pct") if res else None,
            "base_duration_weeks": res.get("base_duration_weeks") if res else None,
            "base_category": res.get("base_category") if res else None,
            "ma_respect_status": res.get("ma_respect_status") if res else None,
            "ma_respect_status_daily_50dma": checklist.get("daily_50dma_status") if checklist else None,
            "checklist_passed": checklist.get("passed") if checklist else None,
            "slope_10w": res.get("slope_10w") if res else None,
            "slope_20w": res.get("slope_20w") if res else None,
            "purple_dot_count": checklist.get("purple_dot_count") if checklist else None,
            "is_young_stage2": is_young,
            "weeks_in_stage2": young_info.get("weeks_in_stage2"),
            "weekly_vol_ratio": young_info.get("volume_ratio"),
            "in_momentum_scan": ticker in momentum_survivors,
            "leg_dot_score": dot_scores.get(ticker),
            "extra_data": {
                "checklist_failed_reasons": checklist.get("failed_reasons") if checklist else None,
            },
        }
        if current_stage is None and current_rec:
            candidate_data.setdefault("extra_data", {})["stage_unverified"] = True

        # Route strictly by lifecycle action
        if action == ACTION_ADD:
            store.upsert(ticker, candidate_data, as_of_date=today_str)
            added_list.append(candidate_data)
        elif action == ACTION_KEEP:
            store.upsert(ticker, candidate_data, as_of_date=today_str)
            # Fetch updated record with history for the report
            updated_rec = store.get_ticker(ticker)
            kept_list.append(updated_rec or candidate_data)
        elif action == ACTION_PROMOTE:
            store.mark_promoted(ticker, as_of_date=today_str)
            promoted_rec = store.get_ticker(ticker)
            promoted_list.append(promoted_rec or candidate_data)
        elif action == ACTION_REMOVE:
            if was_pending and current_stage in (1, 4):
                rem_reason = "stage2_lost"
            elif was_pending and weeks_pending >= 8:
                rem_reason = "young_breakout_stale"
            elif is_hard_reject or quality_flag == "FAULTY":
                rem_reason = "base_broken"
            else:
                rem_reason = "base_broken"

            store.mark_removed(ticker, reason=rem_reason, as_of_date=today_str)
            removed_rec = store.get_ticker(ticker)
            removed_list.append(removed_rec or candidate_data)
        elif action == ACTION_DELIST:
            store.mark_removed(ticker, reason="delisted", as_of_date=today_str)
            delisted_rec = store.get_ticker(ticker)
            removed_list.append(delisted_rec or candidate_data)
        elif action == ACTION_IGNORE:
            pass

    # Sort each list by composite score descending (None values last)
    for lst in (added_list, kept_list, promoted_list, removed_list):
        lst.sort(key=lambda x: (x.get("composite_score") is not None, x.get("composite_score") or -999), reverse=True)

    log.info("Lifecycle summary: %d Added, %d Kept, %d Promoted, %d Removed.",
             len(added_list), len(kept_list), len(promoted_list), len(removed_list))

    # ── Step 10: Generate Weekly Report ───────────────────────────────────────
    report_filename = f"watchlist_report_{today_str}.md"
    report_file_path = os.path.join(reports_dir, report_filename)
    report_md = generate_weekly_report(
        as_of_date=today_str,
        added_tickers=added_list,
        kept_tickers=kept_list,
        promoted_tickers=promoted_list,
        removed_tickers=removed_list,
        report_path=report_file_path,
    )

    store.close()
    log.info("=" * 78)
    log.info("WEEKLY WATCHLIST PIPELINE — RUN COMPLETED")
    log.info("=" * 78)

    return {
        "as_of_date": today_str,
        "added": added_list,
        "kept": kept_list,
        "promoted": promoted_list,
        "removed": removed_list,
        "report_path": report_file_path,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weekly Swing-Trading Watchlist Pipeline")
    parser.add_argument("--db-path", type=str, default="watchlist.db", help="Path to SQLite database")
    parser.add_argument("--lookback-weeks", type=int, default=0, help="Replay pipeline N weeks ago")
    parser.add_argument("--score-young-daily-bars", action="store_true", help="Fetch daily bars for young-only tickers")
    parser.add_argument("--skip-momentum-scan", action="store_true", help="Skip Component B (useful for fast testing)")
    parser.add_argument("--skip-young-scan", action="store_true", help="Skip Component C (useful for fast testing)")
    parser.add_argument("--reports-dir", type=str, default="reports", help="Directory to save weekly reports")
    parser.add_argument("--store-backend", type=str, choices=["sqlite", "supabase"], default="sqlite",
                         help="Which WatchlistStore implementation to persist to")
    args = parser.parse_args()

    run_weekly_pipeline(
        db_path=args.db_path,
        lookback_weeks=args.lookback_weeks,
        score_young_daily_bars=args.score_young_daily_bars,
        skip_momentum_scan=args.skip_momentum_scan,
        skip_young_scan=args.skip_young_scan,
        reports_dir=args.reports_dir,
        store_backend=args.store_backend,
    )
