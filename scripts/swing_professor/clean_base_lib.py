"""
clean_base_lib.py
=================
Shared library of pure/analytical functions for the Clean Base framework.

Extracted from stage2_expansion_screen.py so that both the standalone CLI
script and the weekly watchlist pipeline orchestrator can import the same
code without triggering module-level side effects (logging.basicConfig,
load_dotenv, etc.).

Functions:
    find_stage2_epoch_start  — Stage-2 epoch origin detection from DB stage history
    zigzag_pivots            — threshold-based pivot segmentation of weekly closes
    classify_base_depth      — depth-vs-expansion-size band classification
    check_ma_respect         — converging/diverging 10-week SMA respect check
    check_volatility_contraction — VCP ratio (2nd-half vs 1st-half weekly range)
    evaluate_current_base    — full base-quality checklist on the current base
"""

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

STAGE1_LOOKBACK_WEEKS = 8
ZIGZAG_THRESHOLD_PCT = 8.0
DEPTH_HARD_CEILING_PCT = 30.0
CAT_A_MAX_EXPANSION_PCT = 80.0
CAT_A_IDEAL = (12.0, 15.0)
CAT_A_ACCEPTABLE = (10.0, 17.0)
CAT_B_IDEAL = (16.0, 26.0)
CAT_B_ACCEPTABLE = (14.0, 29.0)
MIN_BASE_WEEKS = 4
BASE_DURATION_IDEAL_WEEKS = 12.0
BASE_DURATION_MANUAL_REVIEW_WEEKS = 16.0
MA_PERIOD_WEEKS = 10
MA_TREND_LOOKBACK_WEEKS = 3
MA_PERIOD_DAYS_50DMA = 50
# Daily-bar analogue of MA_TREND_LOOKBACK_WEEKS -- daily gap/SMA distance is
# noisier day-to-day than week-to-week, so a straight "3" would be too short
# a window to read trend intent from. ~2 trading weeks, untuned starting point.
MA_TREND_LOOKBACK_DAYS_50DMA = 10
VCP_CLEAN_RATIO = 0.8
VCP_FAULTY_RATIO = 1.0

def find_stage2_epoch_start(rows: List[dict]) -> Tuple[Optional[dict], Optional[str]]:
    """
    rows: weekly_stock_stages rows for one ticker, oldest -> newest, each with
    'stage' and 'analysis_date'.

    An "epoch" is the current unbroken Stage-2 advance: it tolerates normal
    Stage 2 <-> Stage 3 oscillation (SMA flattening during a base is expected
    and healthy) but ends the moment a Stage 4 week appears, since that's a
    real breakdown, not a pause.

    Returns:
        ( {'epoch_start_idx': int, 'epoch_start_date': str}, None ) on success.
        ( None, reason_str ) on rejection with granular reason:
            - 'no_stage_history_rows'
            - 'latest_stage_not_2 (stage_X)'
            - 'no_stage2_in_run'
            - 'mature_run_at_data_edge' (>104wk continuous run reaches index 0)
            - 'stage4_in_origin_lookback'
            - 'no_stage1_in_origin_lookback'
    """
    if not rows:
        return None, "no_stage_history_rows"

    stages = [r["stage"] for r in rows if r.get("stage") is not None]
    if not stages:
        return None, "no_stage_history_rows"
    if stages[-1] != 2:
        return None, f"latest_stage_not_2 (stage_{stages[-1]})"

    n = len(stages)
    # Walk backward while the week is Stage 2 or Stage 3 (oscillation allowed
    # within an epoch) -- stop the moment Stage 4 or Stage 1 appears.
    i = n - 1
    while i > 0 and stages[i - 1] in (2, 3):
        i -= 1
    # i now points at the first Stage-2-or-3 week of this run. Walk forward
    # from i to find the first actual Stage 2 week (an epoch must begin on a
    # Stage 2 week, not a leading Stage 3 remnant from an even earlier run).
    epoch_start_idx = None
    for k in range(i, n):
        if stages[k] == 2:
            epoch_start_idx = k
            break
    if epoch_start_idx is None:
        return None, "no_stage2_in_run"

    # Confirm a genuine Stage 1 base precedes the epoch: skip back over any
    # immediately-preceding Stage 3 remnant, then look STAGE1_LOOKBACK_WEEKS
    # further back for Stage 1, rejecting if Stage 4 appears in that window.
    j = epoch_start_idx
    while j > 0 and stages[j - 1] == 3:
        j -= 1

    if j == 0:
        return None, "mature_run_at_data_edge"

    window_start = max(0, j - STAGE1_LOOKBACK_WEEKS)
    context = stages[window_start:j]
    if 4 in context:
        return None, "stage4_in_origin_lookback"
    if 1 not in context:
        return None, "no_stage1_in_origin_lookback"

    return {
        "epoch_start_idx": epoch_start_idx,
        "epoch_start_date": rows[epoch_start_idx]["analysis_date"],
    }, None


def zigzag_pivots(closes: List[float], threshold_pct: float) -> List[dict]:
    """
    Segment a chronological weekly close-price series into alternating pivot
    lows/highs using a threshold reversal rule. Seeds the series as a LOW
    (the epoch/breakout origin). Returns a list of dicts:
        {'idx', 'price', 'type': 'low'|'high', 'confirmed': bool}
    The final entry may be 'confirmed=False' -- the currently-forming swing
    that hasn't reversed by threshold_pct yet.
    """
    if not closes:
        return []

    pivots = [{"idx": 0, "price": closes[0], "type": "low", "confirmed": True}]
    direction: Optional[str] = None
    extreme_idx, extreme_price = 0, closes[0]

    for i in range(1, len(closes)):
        price = closes[i]

        if direction is None:
            if price >= closes[0] * (1 + threshold_pct / 100):
                direction, extreme_idx, extreme_price = "up", i, price
            elif price <= closes[0] * (1 - threshold_pct / 100):
                direction, extreme_idx, extreme_price = "down", i, price
            continue

        if direction == "up":
            if price > extreme_price:
                extreme_idx, extreme_price = i, price
            elif price <= extreme_price * (1 - threshold_pct / 100):
                pivots.append({"idx": extreme_idx, "price": extreme_price, "type": "high", "confirmed": True})
                direction, extreme_idx, extreme_price = "down", i, price
        else:  # direction == "down"
            if price < extreme_price:
                extreme_idx, extreme_price = i, price
            elif price >= extreme_price * (1 + threshold_pct / 100):
                pivots.append({"idx": extreme_idx, "price": extreme_price, "type": "low", "confirmed": True})
                direction, extreme_idx, extreme_price = "up", i, price

    if direction is not None:
        pivots.append({
            "idx": extreme_idx, "price": extreme_price,
            "type": "high" if direction == "up" else "low",
            "confirmed": False,
        })

    return pivots


def classify_base_depth(current_leg_pct: float, base_depth_pct: float) -> Tuple[str, str]:
    """Returns (category_label, depth_tier) where depth_tier in
    {'ideal','acceptable','outside_band'}. SOFT flag only -- the 30% hard
    ceiling is checked by the caller separately."""
    if current_leg_pct < CAT_A_MAX_EXPANSION_PCT:
        category = "A (<80% leg)"
        ideal, acceptable = CAT_A_IDEAL, CAT_A_ACCEPTABLE
    else:
        # covers 80-150% AND the undefined >150% case, per the agreed
        # fallback (flag if you want a distinct Category C).
        category = "B (80-150% leg)" if current_leg_pct <= 150.0 else "B-fallback (>150% leg)"
        ideal, acceptable = CAT_B_IDEAL, CAT_B_ACCEPTABLE

    if ideal[0] <= base_depth_pct <= ideal[1]:
        tier = "ideal"
    elif acceptable[0] <= base_depth_pct <= acceptable[1]:
        tier = "acceptable"
    else:
        tier = "outside_band"
    return category, tier


def check_ma_respect(
    df_base: pd.DataFrame, sma_col: str = "sma_10w", lookback: int = MA_TREND_LOOKBACK_WEEKS
) -> Tuple[str, float]:
    """
    Converging/diverging SMA respect check ("look out for the intent... should
    try to go up" -- your original rule, taken literally rather than as a
    fixed distance threshold). Timeframe-agnostic: works on weekly bars with
    sma_col="sma_10w" (the original use), or daily bars with sma_col="sma_50d"
    and a daily-appropriate lookback (see check_daily_50dma_respect below) --
    same mechanism, different bar cadence.

    gap[i] = (sma[i] - close[i]) / sma[i] * 100  (>0 = below the MA)

    - Never below the MA at all -> 'clean'.
    - Currently at/above the MA (even if it dipped below earlier in the
      base) -> 'clean' -- it already reclaimed.
    - Currently below the MA, but the gap has SHRUNK vs `lookback` bars
      ago -> 'minor_pierce' -- converging back toward the MA, showing intent.
    - Currently below the MA, and the gap is flat or still WIDER than
      `lookback` bars ago -> 'faulty_breakdown' -- no reclaim intent,
      "clean breakdown and follow-through."
    - Not enough bars yet in the base to judge a trend -> 'minor_pierce'
      (don't fail it on insufficient data alone).

    Returns (status, worst_pct_below) where status in
    {'clean','minor_pierce','faulty_breakdown'}.
    """
    sma = df_base[sma_col].to_numpy()
    close = df_base["close"].to_numpy()
    gap = (sma - close) / sma * 100.0  # >0 = below MA

    if (gap <= 0).all():
        return "clean", 0.0

    worst = float(gap.max())
    latest_gap = float(gap[-1])

    if latest_gap <= 0:
        return "clean", worst  # already reclaimed, regardless of history

    n = len(gap)
    effective_lookback = min(lookback, n - 1)
    if effective_lookback < 1:
        return "minor_pierce", worst  # not enough bars to judge a trend yet

    reference_gap = float(gap[-1 - effective_lookback])
    if latest_gap < reference_gap:
        return "minor_pierce", worst  # gap shrinking -- reclaim intent
    return "faulty_breakdown", worst  # gap flat/widening -- no reclaim intent


def check_daily_50dma_respect(daily_df_base: pd.DataFrame) -> Tuple[str, float]:
    """
    Daily-bar analogue of check_ma_respect, added alongside (not replacing)
    the existing weekly 10-week-SMA check per the Clean Base rules: "these
    bases respect MA (50DMA/10WMA) -- it can break 50DMA but look out for the
    intent near it and should try to go up." Faulty-base rule #1 ("stock
    disrespecting 50DMA -- clean breakdown and a follow through") maps to the
    'faulty_breakdown' status here, same as the weekly check's semantics.

    daily_df_base must already have 'sma_50d' computed over enough prior
    history to seed the rolling window (>= MA_PERIOD_DAYS_50DMA days before
    the slice start), then be sliced down to just the base/pullback window
    (base_start_date onward) before calling this -- mirrors exactly how the
    weekly version is called with df_base already sliced in evaluate_current_base.

    Returns (status, worst_pct_below), same three-value contract as
    check_ma_respect.
    """
    return check_ma_respect(daily_df_base, sma_col="sma_50d", lookback=MA_TREND_LOOKBACK_DAYS_50DMA)


def check_volatility_contraction(df_base: pd.DataFrame) -> float:
    """Ratio of 2nd-half avg weekly range% to 1st-half avg weekly range%.
    <0.8 = strong contraction, >=1.0 = expansion (faulty)."""
    n = len(df_base)
    if n < 4:
        return 1.0  # too short to judge -- treated as neutral, not clean
    half = n // 2
    left, right = df_base.iloc[:half], df_base.iloc[half:]
    left_range = ((left["high"] - left["low"]) / left["close"] * 100.0).mean()
    right_range = ((right["high"] - right["low"]) / right["close"] * 100.0).mean()
    if not left_range or np.isnan(left_range) or left_range == 0:
        return 1.0
    return float(right_range / left_range)


def evaluate_current_base(weekly_df: pd.DataFrame, pivots: List[dict], dates: pd.DatetimeIndex) -> Optional[dict]:
    """
    Given the full weekly bar frame (with sma_10w precomputed) and its zigzag
    pivots, evaluate the CURRENT base -- i.e. the stock must currently be
    pulling back from its last confirmed high pivot with no new high yet.
    Returns None if there's no active base to evaluate (still mid-expansion,
    or not enough bars in the base yet).
    """
    high_pivots = [p for p in pivots if p["type"] == "high" and p["confirmed"]]
    low_pivots_confirmed = [p for p in pivots if p["type"] == "low" and p["confirmed"]]
    if not high_pivots:
        return None  # never completed a single leg -- nothing to segment yet

    last_high = high_pivots[-1]
    if pivots[-1]["type"] == "high" and pivots[-1]["idx"] != last_high["idx"]:
        return None  # already made a fresh high beyond last_high -- not "last" anymore
    if pivots[-1]["confirmed"] is False and pivots[-1]["type"] == "high":
        return None  # still expanding, unconfirmed new high -- no base yet

    expansion_number = len(high_pivots)

    # Prior confirmed low immediately before last_high -> per-LEG gain (not
    # cumulative from the original breakout -- this is the core fix vs.
    # clean_base_screener.py's cumulative expansion_pct bug).
    prior_lows_before_high = [p for p in low_pivots_confirmed if p["idx"] < last_high["idx"]]
    if not prior_lows_before_high:
        return None
    prior_low = prior_lows_before_high[-1]

    current_leg_pct = (last_high["price"] - prior_low["price"]) / prior_low["price"] * 100.0

    base_start_idx = last_high["idx"]
    df_base = weekly_df.iloc[base_start_idx:].copy()
    base_len_weeks = len(df_base)
    if base_len_weeks < MIN_BASE_WEEKS:
        return None  # data-sufficiency guard, not a quality judgment

    high_price = float(last_high["price"])
    low_in_base = float(df_base["low"].min())
    base_depth_pct = (high_price - low_in_base) / high_price * 100.0
    base_duration_weeks = float(base_len_weeks)

    # ── Hard rejects ──────────────────────────────────────────────────────
    long_base_manual_review = base_duration_weeks > BASE_DURATION_IDEAL_WEEKS

    if base_depth_pct > DEPTH_HARD_CEILING_PCT:
        return {"hard_reject": f"base_depth {base_depth_pct:.1f}% > {DEPTH_HARD_CEILING_PCT:.0f}% ceiling"}
    if base_duration_weeks > BASE_DURATION_MANUAL_REVIEW_WEEKS:
        return {"hard_reject": f"base_duration {base_duration_weeks:.1f}w > {BASE_DURATION_MANUAL_REVIEW_WEEKS:.0f}w ceiling"}
    if long_base_manual_review and expansion_number == 1:
        return {
            "hard_reject": (
                f"base_duration {base_duration_weeks:.1f}w > {BASE_DURATION_IDEAL_WEEKS:.0f}w ideal "
                "on the 1st expansion leg -- only 2nd+ expansion legs are tradeable once a base runs long"
            )
        }

    # ── Soft depth-band flag ─────────────────────────────────────────────
    category, depth_tier = classify_base_depth(current_leg_pct, base_depth_pct)

    # ── MA respect, VCP ──────────────────────────────────────────────────
    ma_status, ma_worst_pct = check_ma_respect(df_base)
    vcp_ratio = check_volatility_contraction(df_base)

    # ── Composite classification ─────────────────────────────────────────
    faulty_triggers = []
    if ma_status == "faulty_breakdown":
        faulty_triggers.append(f"10WMA breakdown ({ma_worst_pct:.1f}% below)")
    if vcp_ratio >= VCP_FAULTY_RATIO:
        faulty_triggers.append(f"volatility expanding, not contracting (ratio {vcp_ratio:.2f})")

    if faulty_triggers:
        quality_flag = "FAULTY"
    elif (
        depth_tier == "ideal"
        and not long_base_manual_review
        and ma_status == "clean"
        and vcp_ratio < VCP_CLEAN_RATIO
    ):
        quality_flag = "CLEAN"
    else:
        quality_flag = "OK"

    return {
        "hard_reject": None,
        "expansion_number": expansion_number,
        "current_leg_pct": round(current_leg_pct, 1),
        "base_category": category,
        "base_depth_pct": round(base_depth_pct, 1),
        "base_depth_tier": depth_tier,
        "base_duration_weeks": round(base_duration_weeks, 1),
        "long_base_manual_review": long_base_manual_review,
        "ma_respect_status": ma_status,
        "ma_worst_pct_below": round(ma_worst_pct, 1),
        "vcp_ratio": round(vcp_ratio, 3),
        "quality_flag": quality_flag,
        "faulty_reasons": "; ".join(faulty_triggers) if faulty_triggers else "",
        "base_start_date": str(dates[base_start_idx].date()),
        "last_high_price": round(high_price, 2),
        "prior_low_price": round(float(prior_low["price"]), 2),
        "prior_low_date": str(dates[prior_low["idx"]].date()),
        "last_high_date": str(dates[last_high["idx"]].date()),
    }
