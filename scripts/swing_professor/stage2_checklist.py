"""
stage2_checklist.py
====================
Component D0 -- Stage 2 Checklist: a fast, count-based pre-filter applied
BEFORE the detailed Clean Base evaluation (clean_base_lib.py). A candidate
must pass every checklist criterion below to be eligible for Clean Base
scoring at all -- this is a hard gate, not a soft score, and it's a
separate, simpler rule set from clean_base_lib.py's nuanced per-category
depth bands (CAT_A_IDEAL/CAT_B_IDEAL etc.) -- checklist first, detailed
scoring second, not a replacement for it.

Checklist (all 5 must pass):
    1. Moving Averages  -- 10-week AND 20-week SMA both sloping up, evaluated
                           AT THE LEG'S PEAK (last_high_date), not today --
                           evaluating "today" routinely fails on genuinely
                           healthy pullbacks, since the 10-week MA naturally
                           flattens/dips while price consolidates (the same
                           reason clean_base_lib.check_ma_respect tolerates
                           exactly this rather than requiring a constant
                           uptrend). Confirmed empirically: checking "today"
                           against 14 real Category A candidates failed half
                           of them on a negative 10w slope alone, even though
                           their bases were textbook-ideal by depth.
    2. Prior Force      -- current leg's advance from its swing low >= a
                           category-specific minimum (see PRIOR_FORCE_MIN_PCT_*
                           below) -- NOT one flat 30% for every leg size.
                           Confirmed empirically: a flat 30% minimum rejected
                           13 of 14 real Category A candidates (whose OWN ideal
                           depth band, per clean_base_lib.CAT_A_IDEAL, is
                           12-15%) -- a <80% leg structurally almost never
                           also clears a 30%+ advance, so the two criteria
                           described different populations rather than
                           refining the same one.
    3. Speed Filter     -- >= 3 qualifying "purple dot" days within the rally leg
    4. Pullback Depth   -- current base_depth_pct <= 30% (ideal <= 20%)
    5. Supply Filter    -- zero qualifying "red dot" days within the pullback/base window

Criteria 2 and 4 reuse clean_base_lib.py's own current_leg_pct/base_depth_pct
(must be computed first via evaluate_current_base()). Criteria 3 and 5 need
raw dot counts from dot_scoring.count_dots_in_window() -- this module has no
daily-bar-fetch responsibility of its own, callers pass the counts in.

All thresholds are untuned starting-point defaults, same status as
clean_base_lib.py's and composite_scoring.py's constants -- backtesting-sweep
candidates, not derived from domain rules. "Pullback depth max 25-30%" in the
original spec was ambiguous on the exact ceiling; this uses 30% to match
clean_base_lib.DEPTH_HARD_CEILING_PCT for consistency, with 20% as the ideal
sub-threshold -- revisit if a distinct number is intended.
"""

from typing import Optional, Tuple

import pandas as pd

from clean_base_lib import CAT_A_MAX_EXPANSION_PCT

# ── Config constants (untuned starting-point defaults) ────────────────────────
CHECKLIST_SMA_SHORT_WEEKS: int = 10
CHECKLIST_SMA_LONG_WEEKS: int = 20
# Matches stage_analysis_pipeline_w.py's SLOPE_LOOKBACK/FLAT_THRESHOLD convention
# -- reused deliberately rather than inventing a second "sloping up" definition.
CHECKLIST_SLOPE_LOOKBACK_WEEKS: int = 4
CHECKLIST_FLAT_THRESHOLD: float = 0.015
# Category-specific, not one flat number -- see the module docstring for the
# empirical reasoning (a flat 30% rejected 13/14 real Category A candidates).
# Category A = <80% leg (clean_base_lib.CAT_A_MAX_EXPANSION_PCT), Category B+
# = everything at or above that. The original spec's "30%" is kept as-is for
# Category B+, where big legs make it an easy, meaningful bar; Category A
# gets half that, scaled roughly the same way its own ideal depth band
# (12-15%) is roughly half Category B's (16-26%).
PRIOR_FORCE_MIN_PCT_CATEGORY_A: float = 15.0
PRIOR_FORCE_MIN_PCT_CATEGORY_B: float = 30.0
CHECKLIST_MIN_PURPLE_DOTS: int = 3
CHECKLIST_PULLBACK_IDEAL_MAX_PCT: float = 20.0
CHECKLIST_PULLBACK_HARD_MAX_PCT: float = 30.0


def compute_sma_slope(
    closes: pd.Series, period: int, lookback: int = CHECKLIST_SLOPE_LOOKBACK_WEEKS
) -> Tuple[Optional[float], Optional[float]]:
    """Returns (latest_sma, slope), slope = (sma[-1]-sma[-1-lookback])/sma[-1-lookback]
    -- identical convention to stage_analysis_pipeline_w.py's sma_slope. Returns
    (None, None) if there isn't enough history yet."""
    sma = closes.rolling(window=period, min_periods=period).mean()
    valid = sma.dropna()
    if len(valid) <= lookback:
        return None, None
    latest_sma = sma.iloc[-1]
    lookback_sma = sma.iloc[-1 - lookback]
    if pd.isna(latest_sma) or pd.isna(lookback_sma) or lookback_sma == 0:
        return None, None
    slope = (latest_sma - lookback_sma) / lookback_sma
    return float(latest_sma), float(slope)


def check_moving_averages_sloping_up(weekly_closes: pd.Series) -> dict:
    """Criterion 1: both 10-week and 20-week SMA must be sloping up
    (slope > CHECKLIST_FLAT_THRESHOLD)."""
    sma10, slope10 = compute_sma_slope(weekly_closes, CHECKLIST_SMA_SHORT_WEEKS)
    sma20, slope20 = compute_sma_slope(weekly_closes, CHECKLIST_SMA_LONG_WEEKS)
    if slope10 is None or slope20 is None:
        return {
            "passed": False,
            "reason": "insufficient_history_for_sma_slope",
            "slope_10w": slope10,
            "slope_20w": slope20,
        }
    passed = slope10 > CHECKLIST_FLAT_THRESHOLD and slope20 > CHECKLIST_FLAT_THRESHOLD
    return {
        "passed": passed,
        "reason": None if passed else "sma_not_sloping_up",
        "slope_10w": round(slope10, 4),
        "slope_20w": round(slope20, 4),
    }


def check_prior_force(current_leg_pct: float) -> dict:
    """Criterion 2: current leg's advance from its swing low must clear a
    category-specific minimum -- Category A (<80% leg) needs less than
    Category B+ (>=80% leg), since a flat 30% for every leg size structurally
    excludes almost all Category A setups (see module docstring)."""
    is_category_a = current_leg_pct < CAT_A_MAX_EXPANSION_PCT
    min_pct = PRIOR_FORCE_MIN_PCT_CATEGORY_A if is_category_a else PRIOR_FORCE_MIN_PCT_CATEGORY_B
    passed = current_leg_pct >= min_pct
    category_label = "A" if is_category_a else "B+"
    return {
        "passed": passed,
        "reason": None if passed else f"leg_advance {current_leg_pct:.1f}% < {min_pct:.0f}% min (category {category_label})",
    }


def check_pullback_depth(base_depth_pct: float) -> dict:
    """Criterion 4: base_depth_pct must not exceed the hard max (30%);
    <= 20% is flagged as ideal for downstream scoring/display."""
    passed = base_depth_pct <= CHECKLIST_PULLBACK_HARD_MAX_PCT
    is_ideal = base_depth_pct <= CHECKLIST_PULLBACK_IDEAL_MAX_PCT
    return {
        "passed": passed,
        "is_ideal": is_ideal,
        "reason": None if passed else f"base_depth {base_depth_pct:.1f}% > {CHECKLIST_PULLBACK_HARD_MAX_PCT:.0f}% max",
    }


def evaluate_stage2_checklist(
    weekly_closes: pd.Series,
    current_leg_pct: float,
    base_depth_pct: float,
    purple_dot_count: int,
    red_dot_count_in_pullback: int,
) -> dict:
    """
    Runs all 5 checklist criteria. All must pass for `passed` to be True.
    purple_dot_count / red_dot_count_in_pullback must be computed by the
    caller via dot_scoring.count_dots_in_window() -- the former scoped to
    the rally leg (prior_low_date -> last_high_date), the latter scoped to
    the pullback/base window (last_high_date -> now).
    """
    ma_check = check_moving_averages_sloping_up(weekly_closes)
    force_check = check_prior_force(current_leg_pct)
    depth_check = check_pullback_depth(base_depth_pct)
    speed_passed = purple_dot_count >= CHECKLIST_MIN_PURPLE_DOTS
    supply_passed = red_dot_count_in_pullback == 0

    checks = {
        "moving_averages": ma_check["passed"],
        "prior_force": force_check["passed"],
        "speed_filter": speed_passed,
        "pullback_depth": depth_check["passed"],
        "supply_filter": supply_passed,
    }
    all_passed = all(checks.values())

    failed_reasons = []
    if not ma_check["passed"]:
        failed_reasons.append(ma_check["reason"])
    if not force_check["passed"]:
        failed_reasons.append(force_check["reason"])
    if not speed_passed:
        failed_reasons.append(f"purple_dots {purple_dot_count} < {CHECKLIST_MIN_PURPLE_DOTS} min")
    if not depth_check["passed"]:
        failed_reasons.append(depth_check["reason"])
    if not supply_passed:
        failed_reasons.append(f"red_dots_in_pullback {red_dot_count_in_pullback} > 0")

    return {
        "passed": all_passed,
        "checks": checks,
        "failed_reasons": failed_reasons,
        "slope_10w": ma_check.get("slope_10w"),
        "slope_20w": ma_check.get("slope_20w"),
        "purple_dot_count": purple_dot_count,
        "red_dot_count_in_pullback": red_dot_count_in_pullback,
        "pullback_depth_ideal": depth_check.get("is_ideal"),
    }
