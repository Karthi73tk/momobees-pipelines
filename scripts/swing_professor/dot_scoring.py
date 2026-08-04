"""
dot_scoring.py
==============
Component E — Position-weighted purple/red-dot leg score.

For each candidate's current expansion leg (prior confirmed low → last confirmed
high, from the zigzag segmentation in clean_base_lib), this module scores the
quality of institutional accumulation by weighting daily purple dots (≥5% up on
≥500k volume) and red dots (≤−5% down on ≥500k volume) by their position within
the leg.

Dots EARLIER in the leg (closer to the breakout origin) get weight ≈1.0.
Dots LATER in the leg (closer to the peak) get weight ≈0.0.

Rationale: dots earlier in a leg reflect genuine sustained conviction from the
start of the move; dots clustered right before the peak can reflect late-stage/
blow-off buying rather than healthy accumulation. Same logic for red dots as a
penalty — early red dots are a bigger red flag about the leg's origin than a
single red dot near the very top.

All thresholds are config constants at the top of this file, marked as untuned
starting-point defaults designed for future backtesting sweeps.
"""

from typing import Optional, Tuple

import pandas as pd

# ── Config constants (starting-point defaults — NOT tuned) ────────────────────
PURPLE_DOT_MOVE_PCT = 5.0        # min daily % gain to qualify as a purple dot
PURPLE_DOT_MIN_VOLUME = 500_000  # min daily volume for purple/red dots
RED_DOT_PENALTY_MULTIPLIER = 1.5 # red-dot penalty scaling factor (not tuned,
                                  # mark as a config constant the user can adjust)


def compute_leg_dot_score(
    daily_df: pd.DataFrame,
    leg_start_date: str,
    leg_end_date: str,
) -> Tuple[float, float, float]:
    """
    Compute the position-weighted purple/red-dot score for a single expansion leg.

    Parameters
    ----------
    daily_df : pd.DataFrame
        Daily OHLCV bars with a DatetimeIndex, columns including 'close' and
        'volume'. Must span at least the leg's date range.
    leg_start_date : str
        ISO date string for the prior confirmed low pivot (leg origin).
    leg_end_date : str
        ISO date string for the last confirmed high pivot (leg peak).

    Returns
    -------
    (purple_score, red_penalty, leg_dot_score) where:
        purple_score: sum of recency_weight for each purple dot in the leg
        red_penalty: sum of recency_weight for each red dot in the leg
        leg_dot_score = purple_score - RED_DOT_PENALTY_MULTIPLIER * red_penalty
    """
    start_ts = pd.Timestamp(leg_start_date)
    end_ts = pd.Timestamp(leg_end_date)

    # Slice daily bars to the leg's date range (inclusive)
    leg_days = daily_df.loc[
        (daily_df.index >= start_ts) & (daily_df.index <= end_ts)
    ].copy()

    if leg_days.empty:
        return 0.0, 0.0, 0.0

    # Compute daily pct_change if not already present
    if 'pct_change' not in leg_days.columns:
        leg_days['pct_change'] = leg_days['close'].pct_change() * 100.0

    leg_length = len(leg_days)
    purple_score = 0.0
    red_penalty = 0.0

    for i in range(leg_length):
        row = leg_days.iloc[i]
        position_frac = i / max(leg_length - 1, 1)
        recency_weight = 1.0 - position_frac  # left side -> ~1, right side -> ~0

        pct = row.get('pct_change', 0.0)
        vol = row.get('volume', 0)

        if pd.isna(pct) or pd.isna(vol):
            continue

        if pct >= PURPLE_DOT_MOVE_PCT and vol >= PURPLE_DOT_MIN_VOLUME:
            purple_score += recency_weight

        if pct <= -PURPLE_DOT_MOVE_PCT and vol >= PURPLE_DOT_MIN_VOLUME:
            red_penalty += recency_weight

    leg_dot_score = purple_score - RED_DOT_PENALTY_MULTIPLIER * red_penalty
    return round(purple_score, 4), round(red_penalty, 4), round(leg_dot_score, 4)


def count_dots_in_window(
    daily_df: pd.DataFrame,
    start_date: str,
    end_date: str,
) -> Tuple[int, int]:
    """
    Raw (unweighted) count of qualifying purple/red dot days within an
    arbitrary date window -- used by stage2_checklist.py's count-based gates
    ("Speed Filter: >= 3 purple dots", "Supply Filter: zero red dots"), which
    need a simple count rather than compute_leg_dot_score's recency-weighted
    score. Same PURPLE_DOT_MOVE_PCT/PURPLE_DOT_MIN_VOLUME thresholds, no
    position weighting -- deliberately separate from compute_leg_dot_score
    rather than bolted onto it, since the two callers want different windows
    (rally leg vs. pullback-only) and different shapes of result.

    Returns (purple_count, red_count). (0, 0) if the window has no rows.
    """
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)

    window_days = daily_df.loc[
        (daily_df.index >= start_ts) & (daily_df.index <= end_ts)
    ].copy()

    if window_days.empty:
        return 0, 0

    if 'pct_change' not in window_days.columns:
        window_days['pct_change'] = window_days['close'].pct_change() * 100.0

    purple_count = 0
    red_count = 0
    for _, row in window_days.iterrows():
        pct = row.get('pct_change', 0.0)
        vol = row.get('volume', 0)
        if pd.isna(pct) or pd.isna(vol):
            continue
        if pct >= PURPLE_DOT_MOVE_PCT and vol >= PURPLE_DOT_MIN_VOLUME:
            purple_count += 1
        if pct <= -PURPLE_DOT_MOVE_PCT and vol >= PURPLE_DOT_MIN_VOLUME:
            red_count += 1

    return purple_count, red_count
