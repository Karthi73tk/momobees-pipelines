-- create_indices_tables.sql
--
-- New tables for data_sync_engine_indices.py.
-- Review and apply manually in Supabase — not applied automatically.

-- ── universe.indices ─────────────────────────────────────────────────────────
create table if not exists universe.indices (
    ticker      text primary key,
    name        text,
    exchange    text not null default 'NSE',
    is_active   boolean not null default true,
    updated_at  timestamptz not null default now()
);

-- ── screener.indices_metrics ─────────────────────────────────────────────────
create table if not exists screener.indices_metrics (
    ticker          text primary key references universe.indices (ticker),
    price           numeric,
    prev_close      numeric,
    pct_change_1d   numeric,
    ema_200         numeric,
    above_ema200    boolean,
    tv_synced_at    timestamptz,
    updated_at      timestamptz not null default now()
);

create index if not exists idx_indices_metrics_tv_synced_at
    on screener.indices_metrics (tv_synced_at);
