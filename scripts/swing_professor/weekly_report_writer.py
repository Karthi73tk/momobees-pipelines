"""
weekly_report_writer.py
=======================
Weekly Watchlist Report Writer.

Generates a structured markdown report with four sections:
  1. Added (new setups clearing the Clean Base checklist this week)
  2. Kept (existing watchlist setups remaining in healthy, active bases)
  3. Promoted (setups breaking out to new highs without active bases)
  4. Removed (setups removed due to broken bases or delisting)

Each section is sorted by composite_score descending.
"""

import pathlib
from datetime import date
from typing import List, Optional


def generate_weekly_report(
    as_of_date: str,
    added_tickers: List[dict],
    kept_tickers: List[dict],
    promoted_tickers: List[dict],
    removed_tickers: List[dict],
    report_path: Optional[str] = None,
) -> str:
    """Generate a structured markdown weekly watchlist report with 4 sections."""
    def _score_key(x):
        s = x.get("composite_score")
        return (s is not None, float(s) if s is not None else -999.0)

    added_tickers = sorted(added_tickers, key=_score_key, reverse=True)
    kept_tickers = sorted(kept_tickers, key=_score_key, reverse=True)
    promoted_tickers = sorted(promoted_tickers, key=_score_key, reverse=True)
    removed_tickers = sorted(removed_tickers, key=_score_key, reverse=True)

    lines = []
    lines.append(f"# Weekly Swing-Trading Watchlist Report — {as_of_date}")
    lines.append("")
    lines.append(f"**Generated on:** {as_of_date}  ")
    lines.append(
        f"**Summary:** {len(added_tickers)} Added | {len(kept_tickers)} Kept | "
        f"{len(promoted_tickers)} Promoted (Breakout) | {len(removed_tickers)} Removed"
    )
    lines.append("")
    lines.append("---")
    lines.append("")

    # 1. Added
    lines.append(f"## 1. Added ({len(added_tickers)})")
    lines.append("New setups clearing the Clean Base checklist or qualified young Stage 1->2 breakouts this week.")
    lines.append("")
    if not added_tickers:
        lines.append("*No new candidates added this week.*")
    else:
        lines.append("| Ticker | Score | Quality | Depth Fit | Leg # | Base Depth | Duration | Young St2 | Mom Scan | Dot Score |")
        lines.append("|---|---|---|---|---|---|---|---|---|---|")
        for c in added_tickers:
            score = f"{c.get('composite_score', 0):.1f}" if c.get('composite_score') is not None else "—"
            weeks_in_st2 = c.get("weeks_in_stage2")
            if c.get("quality_flag") is None and c.get("is_young_stage2"):
                q = f"PENDING (Wk {weeks_in_st2})" if weeks_in_st2 is not None else "PENDING (Young)"
                d_fit = "Forming"
                leg = "1 (Breakout)"
                depth = "Forming"
                dur = f"{weeks_in_st2}w" if weeks_in_st2 is not None else "Forming"
            else:
                q = c.get("quality_flag", "N/A")
                d_fit = c.get("depth_tier", c.get("base_depth_tier", "N/A"))
                leg = c.get("expansion_number", "N/A")
                depth = f"{c.get('base_depth_pct', 0):.1f}%" if c.get('base_depth_pct') is not None else "N/A"
                dur = f"{c.get('base_duration_weeks', 0):.0f}w" if c.get('base_duration_weeks') is not None else "N/A"
            young = "Yes" if c.get("is_young_stage2") else "No"
            scan = "Yes" if c.get("in_momentum_scan") else "No"
            dot = f"{c.get('leg_dot_score'):.2f}" if c.get("leg_dot_score") is not None else "N/A"
            lines.append(f"| **{c['ticker']}** | {score} | {q} | {d_fit} | {leg} | {depth} | {dur} | {young} | {scan} | {dot} |")
    lines.append("")

    # 2. Kept
    lines.append(f"## 2. Kept ({len(kept_tickers)})")
    lines.append("Existing watchlist setups that remain in healthy, active bases or forming breakouts.")
    lines.append("")
    if not kept_tickers:
        lines.append("*No existing candidates kept this week.*")
    else:
        lines.append("| Ticker | Score (This Wk / Prev) | Quality | Leg # | Base Depth | Duration | First Added |")
        lines.append("|---|---|---|---|---|---|---|")
        for c in kept_tickers:
            curr_score = f"{c.get('composite_score', 0):.1f}" if c.get('composite_score') is not None else "—"
            history = c.get("score_history", [])
            prev_score = f"{history[-2]['composite_score']:.1f}" if len(history) >= 2 and history[-2].get('composite_score') is not None else "—"
            if c.get("quality_flag") is None and c.get("is_young_stage2"):
                q = "PENDING (Forming)"
                leg = "1 (Breakout)"
                depth = "Forming"
                dur = "Forming"
            else:
                q = c.get("quality_flag", "N/A")
                leg = c.get("expansion_number", "N/A")
                depth = f"{c.get('base_depth_pct', 0):.1f}%" if c.get('base_depth_pct') is not None else "N/A"
                dur = f"{c.get('base_duration_weeks', 0):.0f}w" if c.get('base_duration_weeks') is not None else "N/A"
            first_added = c.get("first_added_date", "N/A")
            extra = c.get("extra_data", {}) or {}
            if c.get("stage_unverified") or extra.get("stage_unverified"):
                q = f"{q} *(stage unverified)*"
            lines.append(f"| **{c['ticker']}** | {curr_score} (prev: {prev_score}) | {q} | {leg} | {depth} | {dur} | {first_added} |")
    lines.append("")

    # 3. Promoted
    lines.append(f"## 3. Promoted — Breakout Graduations ({len(promoted_tickers)})")
    lines.append("Setups that broke out to new highs with no active base pullback (candidates for daily trigger layer).")
    lines.append("")
    if not promoted_tickers:
        lines.append("*No setups promoted this week.*")
    else:
        lines.append("| Ticker | Previous Score | First Added | Basing Duration Before Breakout | Status |")
        lines.append("|---|---|---|---|---|")
        for c in promoted_tickers:
            score = f"{c.get('composite_score', 0):.1f}" if c.get('composite_score') is not None else "—"
            first_added = c.get("first_added_date", "N/A")
            dur = f"{c.get('base_duration_weeks', 0):.0f}w" if c.get('base_duration_weeks') is not None else "N/A"
            lines.append(f"| **{c['ticker']}** | {score} | {first_added} | {dur} | `PROMOTED_BREAKOUT` |")
    lines.append("")

    # 4. Removed
    lines.append(f"## 4. Removed ({len(removed_tickers)})")
    lines.append("Setups removed due to broken base structure, hard ceiling violations, or delisting.")
    lines.append("")
    if not removed_tickers:
        lines.append("*No setups removed this week.*")
    else:
        lines.append("| Ticker | Removal Reason | First Added | Weeks on List | Final Quality |")
        lines.append("|---|---|---|---|---|")
        for c in removed_tickers:
            reason = c.get("removal_reason", "base_broken")
            first_added = c.get("first_added_date", "N/A")
            weeks_on = "N/A"
            if first_added != "N/A":
                try:
                    weeks_on = f"{(date.fromisoformat(as_of_date) - date.fromisoformat(first_added)).days // 7}w"
                except Exception:
                    weeks_on = "N/A"
            q = c.get("quality_flag", "FAULTY")
            lines.append(f"| **{c['ticker']}** | `{reason}` | {first_added} | {weeks_on} | {q} |")
    lines.append("")

    report_md = "\n".join(lines)

    if report_path:
        path = pathlib.Path(report_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(report_md)

    return report_md
