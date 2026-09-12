-- ============================================================================
-- SAR 002 — banded-ratchet position sizing
-- Adds the sizing columns the ratchet needs. Safe to run on an existing `sar`
-- schema (idempotent). Run once in the Supabase SQL editor, then re-seed:
--     python scripts/sar_pipeline.py --seed
--     python scripts/sar_pipeline.py --eod
--
-- Sizing: start 2 lots on ₹10L; +1 lot per +₹3L above the reference (compounding
-- up); −1 lot per −₹1.25L·lots drawdown from the reference (2 lots tolerate ₹2.5L
-- before the first cut); floor 1 lot. The reference moves with each add/cut.
-- ============================================================================

alter table sar.config add column if not exists start_lots     int     not null default 2;
alter table sar.config add column if not exists add_step_inr    numeric not null default 300000;
alter table sar.config add column if not exists dd_per_lot_inr  numeric not null default 125000;

-- ladder reference equity carried on the single-row book
alter table sar.book   add column if not exists ladder_ref      numeric default 1000000;

-- refresh the config note (capital_per_lot is retained but no longer drives sizing)
update sar.config set notes =
    '13/34 SAR, NIFTY 75-min. Forward test from 2026 signals; start 2 lots on ₹10L, '
    '+1 lot per +₹3L, −1 lot per −₹1.25L·lots drawdown (banded ratchet, floor 1 lot).'
where id = 1;

grant all privileges on all tables in schema sar to anon, authenticated, service_role;
