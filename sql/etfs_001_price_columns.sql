-- etfs_001_price_columns.sql
--
-- Applied 2026-10-24 (migration "etfs_price_columns").
--
-- ETFs such as GOLDBEES are never in universe.nse_universe (that table is NSE
-- common stocks only — data_sync_engine_nse_all_d.py filters type=stock), so
-- the web app's ST2 GO_CASH plan had no gold price. scripts/sync_etf_prices_d.py
-- now writes each active ETF's latest NSE daily close here.
--
-- Additive only: nullable columns, existing rows and policies unchanged
-- (anon/authenticated SELECT, service_role ALL already in place).

ALTER TABLE universe.etfs
  ADD COLUMN IF NOT EXISTS price      numeric,
  ADD COLUMN IF NOT EXISTS price_date date,
  ADD COLUMN IF NOT EXISTS updated_at timestamptz;
