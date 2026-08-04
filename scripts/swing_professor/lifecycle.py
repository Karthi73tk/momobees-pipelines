"""
lifecycle.py
============
Component G — Watchlist lifecycle decision engine.

Pure function that takes a candidate's current watchlist state (or None for new
tickers) and this week's analysis result, and returns the lifecycle action to take.

Lifecycle table (from the architecture spec §3G):

| Current state       | This week's result                          | Action   |
|---------------------|---------------------------------------------|----------|
| Not on watchlist    | CLEAN or OK, in candidate_set               | ADD      |
| Not on watchlist    | FAULTY / hard-reject / no active base       | IGNORE   |
| ACTIVE              | still CLEAN or OK                           | KEEP     |
| ACTIVE              | FAULTY or hard-reject (base broke)          | REMOVE   |
| ACTIVE              | no_active_base (broke out to new high)      | PROMOTE  |
| ACTIVE              | ticker dropped out of universe              | DELIST   |

The function does NOT mutate any state — the caller (the orchestrator) is
responsible for applying the action via WatchlistStore.
"""

from typing import Optional

# Valid lifecycle actions
ACTION_ADD = "ADD"
ACTION_KEEP = "KEEP"
ACTION_REMOVE = "REMOVE"
ACTION_PROMOTE = "PROMOTE"
ACTION_DELIST = "DELIST"
ACTION_IGNORE = "IGNORE"


def decide_lifecycle(
    current_status: Optional[str],
    quality_flag: Optional[str],
    has_active_base: bool,
    is_hard_reject: bool,
    in_universe: bool,
    is_young_stage2: bool = False,
    current_stage: Optional[int] = None,
    weeks_pending: int = 0,
    max_pending_weeks: int = 8,
    was_pending: bool = False,
) -> str:
    """
    Determine the lifecycle action for a ticker this week.

    Parameters
    ----------
    current_status : Optional[str]
        The ticker's current status in the watchlist store.
        None if not on the watchlist. 'ACTIVE' if currently active.
        Any other status (REMOVED_*, PROMOTED_*) means the ticker was
        previously removed/promoted and is treated as "not on watchlist"
        for lifecycle purposes (it would need to re-qualify as ADD).
    quality_flag : Optional[str]
        This week's quality flag from Component D: 'CLEAN', 'OK', 'FAULTY',
        or None if no base evaluation was produced (e.g. pending young breakout).
    has_active_base : bool
        True if Component D found an active base to evaluate this week.
        False if the result was 'no_active_base' or 'epoch_too_short_on_weekly_bars'.
    is_hard_reject : bool
        True if Component D returned a hard reject (base > 30% deep or
        > 16 weeks old).
    in_universe : bool
        True if the ticker is still in the active NSE universe.
        False if delisted/inactive.
    is_young_stage2 : bool
        True if the ticker is a young Stage 1->2 breakout (from Component C).
    current_stage : Optional[int]
        The latest weekly stage (1, 2, 3, or 4), or None if unverified/data gap.
    weeks_pending : int
        Number of weeks this ticker has been on the watchlist in a pending state.
    max_pending_weeks : int
        Maximum weeks a pending young ticker can stay without forming an evaluated base (default 8).
    was_pending : bool
        True if the existing record in the watchlist had quality_flag=None (pending base formation).

    Returns
    -------
    str : one of ACTION_ADD, ACTION_KEEP, ACTION_REMOVE, ACTION_PROMOTE,
          ACTION_DELIST, ACTION_IGNORE
    """
    is_active = current_status == "ACTIVE"
    is_on_watchlist = is_active  # only ACTIVE counts as "on watchlist"

    # ── ACTIVE ticker: check for removal/promotion/keep ───────────────────
    if is_on_watchlist:
        # Delisted/inactive → DELIST
        if not in_universe:
            return ACTION_DELIST

        # FAULTY or hard reject → base is broken → REMOVE
        if quality_flag == "FAULTY" or is_hard_reject:
            return ACTION_REMOVE

        # Still CLEAN or OK with active base → KEEP (graduates from pending if previously pending)
        if quality_flag in ("CLEAN", "OK") and has_active_base:
            return ACTION_KEEP

        # Pending young breakout on the watchlist (no evaluated base yet)
        if was_pending or (quality_flag is None and not has_active_base and is_young_stage2):
            # Stage loss: only Stage 1 or 4 loses Stage 2 (Stage 3 flattening is tolerated during basing)
            if current_stage in (1, 4):
                return ACTION_REMOVE
            # Aging cap: exceeded max pending weeks without forming a base
            if weeks_pending >= max_pending_weeks:
                return ACTION_REMOVE
            # Still healthy pending breakout
            return ACTION_KEEP

        # No active base (and had an established base previously) → breakout to new high → PROMOTE
        if not has_active_base:
            return ACTION_PROMOTE

        # Edge case fallback
        return ACTION_REMOVE

    # ── Not on watchlist: check for ADD ───────────────────────────────────
    # 1. Standard path: CLEAN or OK with an active base
    if quality_flag in ("CLEAN", "OK") and has_active_base and not is_hard_reject and in_universe:
        return ACTION_ADD

    # 2. Young breakout pending entry path: fresh Component C match (no base yet)
    if is_young_stage2 and not is_hard_reject and in_universe:
        return ACTION_ADD

    # Everything else: FAULTY, hard-reject, no active base on non-young, not in universe
    return ACTION_IGNORE
