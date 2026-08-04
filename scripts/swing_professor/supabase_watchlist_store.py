"""
supabase_watchlist_store.py
============================
Supabase/Postgres-backed persistence layer for the weekly swing-trading
watchlist. Implements the same interface as watchlist_store.WatchlistStore
(get_active, upsert, mark_removed, mark_promoted, get_ticker, get_all, close)
so weekly_watchlist_pipeline.py needs zero changes at its call sites -- only
which store implementation gets constructed.

Writes to swing_professor.watchlist (see sql/0039_swing_professor_schema.sql).
Uses the same supabase.schema(...).table(...) convention as the rest of this
codebase (get_supabase_client() in stage_analysis_pipeline_w.py), not the
.from_() form -- that's a JS-client idiom, the Python client uses .table().

Every real column on that table is mapped 1:1 from the record dict's
top-level keys. stage_unverified, vcp_ratio, prior_low_date and last_high_date
are also accepted from record["extra_data"] as a fallback, since
weekly_watchlist_pipeline.py's SQLite-era code still buries them there for
some candidates -- the store adapts to the existing caller contract on those
four fields rather than requiring an orchestrator change that could regress
the already-validated SQLite path.
"""

import logging
from datetime import date
from typing import List, Optional

from supabase import Client

log = logging.getLogger("supabase_watchlist_store")

SCHEMA = "swing_professor"
TABLE = "watchlist"

# Columns copied 1:1 from the incoming record dict's top level, when present.
_DIRECT_COLUMNS = [
    "quality_flag", "depth_tier", "base_depth_pct", "base_duration_weeks",
    "expansion_number", "base_category", "ma_respect_status",
    "ma_respect_status_daily_50dma", "checklist_passed", "slope_10w",
    "slope_20w", "purple_dot_count",
    "is_young_stage2", "weeks_in_stage2", "weekly_vol_ratio",
    "in_momentum_scan", "momentum_score", "leg_dot_score", "composite_score",
]

# Columns also accepted from record["extra_data"] as a fallback (see module
# docstring) -- checked there only if absent from the top level.
_EXTRA_DATA_FALLBACK_COLUMNS = [
    "vcp_ratio", "prior_low_date", "last_high_date", "stage_unverified",
]

_STATUS_MAP = {
    "base_broken": "REMOVED_BASE_BROKEN",
    "delisted": "REMOVED_DELISTED",
}


class SupabaseWatchlistStore:
    """Supabase-backed watchlist persistence layer -- same interface as WatchlistStore."""

    def __init__(self, client: Client):
        self.client = client
        self._table = client.schema(SCHEMA).table(TABLE)

    def get_active(self) -> List[dict]:
        resp = self._table.select("*").eq("status", "ACTIVE").execute()
        return resp.data or []

    def get_all(self) -> List[dict]:
        resp = self._table.select("*").execute()
        return resp.data or []

    def get_ticker(self, ticker: str) -> Optional[dict]:
        resp = self._table.select("*").eq("ticker", ticker).execute()
        return resp.data[0] if resp.data else None

    def _build_row(self, record: dict) -> dict:
        extra = record.get("extra_data") or {}
        row = {col: record.get(col) for col in _DIRECT_COLUMNS}
        for col in _EXTRA_DATA_FALLBACK_COLUMNS:
            row[col] = record.get(col, extra.get(col))
        row["is_young_stage2"] = bool(row.get("is_young_stage2") or False)
        row["in_momentum_scan"] = bool(row.get("in_momentum_scan") or False)
        row["stage_unverified"] = bool(row.get("stage_unverified") or False)
        row["extra_data"] = extra
        return row

    def upsert(self, ticker: str, record: dict, as_of_date: Optional[str] = None) -> str:
        """Insert or update a ticker's watchlist entry.

        Returns the action taken: 'ADD' (new ticker) or 'KEEP' (existing, updated).
        """
        today = as_of_date or date.today().isoformat()
        existing = self.get_ticker(ticker)

        history_entry = {
            "date": today,
            "composite_score": record.get("composite_score"),
            "quality_flag": record.get("quality_flag"),
        }

        row = self._build_row(record)
        row["ticker"] = ticker
        row["status"] = "ACTIVE"
        row["last_updated"] = today
        row["removed_date"] = None
        row["removal_reason"] = None

        if existing is None:
            row["first_added_date"] = today
            row["score_history"] = [history_entry]
            action = "ADD"
        else:
            # first_added_date must be preserved (not just omitted) -- Postgres
            # validates NOT NULL constraints against the INSERT VALUES clause
            # even when the statement is headed for ON CONFLICT DO UPDATE, so a
            # missing column here fails the whole upsert, not just skips a field.
            row["first_added_date"] = existing.get("first_added_date")
            score_history = existing.get("score_history") or []
            score_history.append(history_entry)
            row["score_history"] = score_history
            action = "KEEP"

        self._table.upsert(row, on_conflict="ticker").execute()
        return action

    def mark_removed(self, ticker: str, reason: str, as_of_date: Optional[str] = None):
        """Mark an ACTIVE ticker as removed.

        Sets status to the REMOVED_* value matching `reason` (see the CHECK
        constraint on swing_professor.watchlist.status for the valid set).
        """
        today = as_of_date or date.today().isoformat()
        status = _STATUS_MAP.get(reason, f"REMOVED_{reason.upper()}")
        self._table.update({
            "status": status,
            "removed_date": today,
            "removal_reason": reason,
            "last_updated": today,
        }).eq("ticker", ticker).execute()

    def mark_promoted(self, ticker: str, as_of_date: Optional[str] = None):
        """Mark an ACTIVE ticker as promoted (broke out to new high, no base)."""
        today = as_of_date or date.today().isoformat()
        self._table.update({
            "status": "PROMOTED_BREAKOUT",
            "removed_date": today,
            "removal_reason": "breakout",
            "last_updated": today,
        }).eq("ticker", ticker).execute()

    def close(self):
        """No persistent connection to close -- present for interface parity."""
        pass
