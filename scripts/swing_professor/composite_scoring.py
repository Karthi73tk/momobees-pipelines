"""
composite_scoring.py
====================
Component F — Composite ranking score for weekly watchlist candidates.

Combines multiple signal dimensions into a single comparable score per candidate.
All weights are starting-point defaults — NOT tuned. They are designed to be
swept via backtesting against historical weekly_stock_stages + daily bar data.
Do not present these as authoritative.

Scoring formula:
    composite_score =
          W_QUALITY   * quality_tier_score
        + W_DEPTH_FIT * depth_tier_score
        + W_YOUNG     * (1 if is_young_stage2 else 0)
        + W_IN_SCAN   * (1 if in_momentum_scan else 0)
        + W_DOT       * normalize(leg_dot_score)     # skip if None
        + W_EXPANSION * expansion_number_score
        + W_MOMENTUM  * normalize(momentum_score)    # skip if None

Normalization for leg_dot_score and momentum_score is min-max across the
current week's candidate set. Missing optional signals contribute 0 to the
weighted sum against a fixed theoretical maximum denominator, preventing missing
data from artificially inflating scores.
"""

from typing import Dict, List, Optional

# ── Scoring weights (starting-point defaults — NOT tuned) ─────────────────────
# Put all weights in one config block. These are starting points for
# backtesting, not derived from domain rules — say so in code comments.
SCORING_CONFIG = {
    "W_QUALITY": 3,
    "W_DEPTH_FIT": 2,
    "W_YOUNG": 2,
    "W_IN_SCAN": 1,
    "W_DOT": 2,
    "W_EXPANSION": 1,
    "W_MOMENTUM": 1,
    "W_VOLUME_RATIO": 1,                # Untuned default starting weight for weekly breakout volume ratio
    "RED_DOT_PENALTY_MULTIPLIER": 1.5,  # also referenced here for documentation
}

# ── Quality / depth mappings ──────────────────────────────────────────────────
QUALITY_TIER_SCORES = {"CLEAN": 2, "OK": 1}
DEPTH_TIER_SCORES = {"ideal": 2, "acceptable": 1, "outside_band": 0}


def expansion_number_score(expansion_number: Optional[int]) -> float:
    """Expansion number → score. Legs 1-2 score highest (fresher trend, less
    exhaustion risk), tapering for 4+.
    
    Suggested curve (NOT derived from domain rules — untuned placeholder
    designed for backtesting sweeps):
        max(0, 3 - max(0, expansion_number - 2) * 0.75)
    
    Gives: leg 1 → 3.0, leg 2 → 3.0, leg 3 → 2.25, leg 4 → 1.5,
           leg 5 → 0.75, leg 6+ → 0.0
    """
    if expansion_number is None:
        return 0.0
    return max(0.0, 3.0 - max(0, expansion_number - 2) * 0.75)


def normalize_scores(values: List[Optional[float]]) -> List[Optional[float]]:
    """Min-max normalize a list of values. None values pass through as None
    and are excluded from the min/max computation."""
    valid = [v for v in values if v is not None]
    if not valid:
        return values
    lo, hi = min(valid), max(valid)
    if hi == lo:
        # All values are the same — normalize to 1.0 (avoid 0/0)
        return [1.0 if v is not None else None for v in values]
    return [
        (v - lo) / (hi - lo) if v is not None else None
        for v in values
    ]


def get_max_possible_score(config: Optional[Dict] = None) -> float:
    """Compute the theoretical maximum possible weighted score for normalization.
    
    Fixed constant across all candidates regardless of data availability.
    Missing signals (including base quality / depth terms for pending candidates)
    contribute 0 to the numerator against this full-scale fixed denominator.
    """
    cfg = config or SCORING_CONFIG
    return (
        cfg["W_QUALITY"] * 2.0                 # CLEAN = 2
        + cfg["W_DEPTH_FIT"] * 2.0             # ideal = 2
        + cfg["W_YOUNG"] * 1.0                 # True = 1
        + cfg["W_IN_SCAN"] * 1.0               # True = 1
        + cfg["W_DOT"] * 1.0                   # max normalized = 1.0
        + cfg["W_EXPANSION"] * 3.0             # leg 1-2 = 3.0
        + cfg["W_MOMENTUM"] * 1.0              # max normalized = 1.0
        + cfg.get("W_VOLUME_RATIO", 1) * 1.0   # max normalized = 1.0
    )


def compute_composite_score(
    quality_flag: Optional[str],
    depth_tier: Optional[str],
    is_young_stage2: bool,
    in_momentum_scan: bool,
    leg_dot_score_normalized: Optional[float],
    expansion_number: Optional[int],
    momentum_score_normalized: Optional[float],
    volume_ratio_normalized: Optional[float] = None,
    config: Optional[Dict] = None,
) -> float:
    """Compute a multi-factor composite score for a single candidate.
    
    Parameters
    ----------
    quality_flag : Optional[str]
        'CLEAN', 'OK', or None (for pending young breakouts without a base yet).
    depth_tier : Optional[str]
        'ideal', 'acceptable', 'outside_band', or None
    is_young_stage2 : bool
        Whether the candidate entered via Component C (young Stage 2 screen)
    in_momentum_scan : bool
        Whether the candidate is in the momentum scan survivors
    leg_dot_score_normalized : Optional[float]
        Min-max normalized leg dot score, or None if daily bars unavailable (contributes 0)
    expansion_number : Optional[int]
        Which expansion leg (1, 2, 3, ...) or None if pending
    momentum_score_normalized : Optional[float]
        Min-max normalized momentum scanner score, or None if not in scan (contributes 0)
    volume_ratio_normalized : Optional[float]
        Min-max normalized weekly volume ratio from Component C, or None (contributes 0)
    config : Optional[Dict]
        Override SCORING_CONFIG if provided
    
    Returns
    -------
    float : composite score in [0.0, 100.0] (higher is better)
    """
    cfg = config or SCORING_CONFIG
    score = 0.0
    
    # Quality tier (0 if None for pending young breakouts)
    if quality_flag:
        score += cfg["W_QUALITY"] * QUALITY_TIER_SCORES.get(quality_flag, 0)
    
    # Depth fit (0 if None / outside_band)
    if depth_tier:
        score += cfg["W_DEPTH_FIT"] * DEPTH_TIER_SCORES.get(depth_tier, 0)
    
    # Young stage 2 flag
    score += cfg["W_YOUNG"] * (1 if is_young_stage2 else 0)
    
    # In momentum scan flag
    score += cfg["W_IN_SCAN"] * (1 if in_momentum_scan else 0)
    
    # Dot score (0 if daily bars not fetched)
    if leg_dot_score_normalized is not None:
        score += cfg["W_DOT"] * leg_dot_score_normalized
    
    # Expansion number score (0 if None)
    if expansion_number is not None:
        score += cfg["W_EXPANSION"] * expansion_number_score(expansion_number)
    
    # Momentum scanner score (0 if not in momentum scan)
    if momentum_score_normalized is not None:
        score += cfg["W_MOMENTUM"] * momentum_score_normalized

    # Weekly volume ratio score (0 if not available)
    if volume_ratio_normalized is not None:
        score += cfg.get("W_VOLUME_RATIO", 1) * volume_ratio_normalized
    
    # Normalize against the FIXED theoretical maximum across all dimensions
    max_possible = get_max_possible_score(cfg)
    if max_possible > 0:
        composite_score = (score / max_possible) * 100.0
    else:
        composite_score = 0.0

    return round(composite_score, 2)


def rank_candidates(candidates: List[dict], config: Optional[Dict] = None) -> List[dict]:
    """Score and rank a list of candidate dicts.
    
    Each candidate dict can have:
        quality_flag (Optional), depth_tier (Optional), is_young_stage2, in_momentum_scan,
        leg_dot_score (raw, or None), expansion_number (Optional),
        momentum_score (raw, or None), weekly_vol_ratio (raw, or None)
    
    Returns the same list with 'composite_score' and 'rank' added,
    sorted by composite_score descending.
    """
    cfg = config or SCORING_CONFIG
    
    # Normalize dot scores, momentum scores, and volume ratios across the candidate set
    raw_dot_scores = [c.get("leg_dot_score") for c in candidates]
    raw_momentum_scores = [c.get("momentum_score") for c in candidates]
    raw_volume_ratios = [c.get("weekly_vol_ratio") for c in candidates]
    
    norm_dot = normalize_scores(raw_dot_scores)
    norm_momentum = normalize_scores(raw_momentum_scores)
    norm_vol = normalize_scores(raw_volume_ratios)
    
    for i, c in enumerate(candidates):
        c["composite_score"] = compute_composite_score(
            quality_flag=c.get("quality_flag"),
            depth_tier=c.get("depth_tier", c.get("base_depth_tier")),
            is_young_stage2=c.get("is_young_stage2", False),
            in_momentum_scan=c.get("in_momentum_scan", False),
            leg_dot_score_normalized=norm_dot[i],
            expansion_number=c.get("expansion_number"),
            momentum_score_normalized=norm_momentum[i],
            volume_ratio_normalized=norm_vol[i],
            config=cfg,
        )
    
    candidates.sort(key=lambda c: c["composite_score"], reverse=True)
    for rank, c in enumerate(candidates, 1):
        c["rank"] = rank
    
    return candidates
