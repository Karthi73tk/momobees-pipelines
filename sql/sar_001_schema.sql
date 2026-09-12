-- ============================================================================
-- 13/34 EMA Stop-and-Reverse (NIFTY, 75-min) — schema + tables
-- Run this ONCE in the Supabase SQL editor.
--
-- After running, also add 'sar' to the API-exposed schemas:
--   Supabase Dashboard → Project Settings → API → "Exposed schemas" → add: sar
-- (the pipeline writes via PostgREST with the service-role key, which requires
--  the schema to be exposed — same as the existing 'bluesky' / 'universe' schemas.)
--
-- Strategy identity (see MOMOBEES_INTEGRATION handoff):
--   • NIFTY only, 75-min candles (5 bars/session: 09:15 10:30 11:45 13:00 14:15)
--   • EMA_fast = EMA(13, close), EMA_slow = EMA(34, close), warm-up 34 bars
--   • Always-in-market trend follower (LONG / SHORT / FLAT), stop-and-reverse:
--       13 EMA is the trailing stop; a new entry needs close beyond BOTH EMAs.
--   • Fill at the CLOSE of the signal bar. No independent stop-loss by design.
--   • Live signal is built from the NIFTY CONTINUOUS FUTURE (tvDatafeed 15-min
--     → reconstructed 75-min). Historic 2026 seed is on index spot.
--   • Book: ₹10L capital, ₹5L per lot, lot_size 65, lots = floor(equity/₹5L)
--     recomputed at each new entry (compounded). ₹P&L = net_points·65·lots.
-- ============================================================================

-- Drop the earlier 'ema_sar' name if it was created before the rename to 'sar'.
-- (Safe: the book had not been seeded yet. Remove this line if you never ran the
--  old ema_sar_001_schema.sql.)
drop schema if exists ema_sar cascade;

create schema if not exists sar;

-- ── 1. Single-row strategy config ────────────────────────────────────────────
create table if not exists sar.config (
    id               int         primary key default 1,
    symbol           text        not null default 'NIFTY',
    feed_symbol      text        not null default 'NIFTY1!',  -- tvDatafeed continuous future
    exchange         text        not null default 'NSE',
    interval_min     int         not null default 15,          -- base bars we reconstruct 75-min from
    tf_min           int         not null default 75,
    ema_fast         int         not null default 13,
    ema_slow         int         not null default 34,
    start_capital    numeric     not null default 1000000,     -- ₹10L
    start_lots       int         not null default 2,           -- start 2 lots on ₹10L
    add_step_inr     numeric     not null default 300000,      -- +₹3L profit → +1 lot (compounding up)
    dd_per_lot_inr   numeric     not null default 125000,      -- −₹1.25L·lots drawdown → −1 lot (floor 1)
    lot_size         int         not null default 65,          -- current NIFTY F&O lot
    started_on       date        not null default '2026-01-01',
    notes            text,
    constraint sar_config_singleton check (id = 1)
);

insert into sar.config (id, notes) values
    (1, '13/34 SAR, NIFTY 75-min. Forward test from 2026 signals; start 2 lots on ₹10L, '
        '+1 lot per +₹3L, −1 lot per −₹1.25L·lots drawdown (banded ratchet, floor 1 lot).')
on conflict (id) do nothing;

-- ── 2. The live book (single row) — current position + running equity ────────
create table if not exists sar.book (
    id               int         primary key default 1,
    state            text        not null default 'FLAT',   -- 'LONG' | 'SHORT' | 'FLAT'
    entry_ts         timestamptz,                            -- open position entry bar close
    entry_price      numeric,                                -- entry mark (spot for seed, future live)
    lots             int         default 2,                  -- lots on the open position
    ladder_ref       numeric     default 1000000,            -- banded-ratchet reference equity (₹)
    last_price       numeric,                                -- latest 75-min close mark
    last_processed_ts timestamptz,                           -- newest 75-min bar folded into the book
    equity           numeric     not null default 1000000,   -- realised equity (₹); open P&L excluded
    net_points_cum   numeric     not null default 0,         -- cumulative realised net points
    ema_fast         numeric,                                -- most recent EMA13 (for the stop line)
    ema_slow         numeric,                                -- most recent EMA34
    updated_at       timestamptz not null default now(),
    constraint sar_book_singleton check (id = 1)
);

insert into sar.book (id) values (1) on conflict (id) do nothing;

-- ── 3. Closed trades (realised) ──────────────────────────────────────────────
create table if not exists sar.trades (
    id               bigint generated always as identity primary key,
    s_no             int,                                    -- 1-based chronological index
    symbol           text        not null default 'NIFTY',
    entry_ts         timestamptz not null,
    direction        text        not null,                  -- 'Long' | 'Short'
    entry_price      numeric     not null,
    exit_ts          timestamptz not null,
    exit_price       numeric     not null,
    exit_reason      text,                                   -- 'Trail-Flat' | 'SAR-Reverse' | 'End-of-Data'
    gross_points     numeric,
    cost_points      numeric,
    net_points       numeric,
    bars_held        int,
    lots             int,                                    -- lots at entry (compounded)
    lot_size         int         default 65,
    pnl_inr          numeric,                                -- net_points · lot_size · lots
    win_lose         text,                                   -- 'Win' | 'Lose' (on net)
    cum_net_points   numeric,
    equity_after     numeric,                                -- book equity after this trade
    source           text        not null default 'live',   -- 'seed' | 'live'
    year             int,
    created_at       timestamptz not null default now(),
    unique (source, entry_ts, exit_ts, direction)
);
create index if not exists idx_sar_trades_exit on sar.trades (exit_ts desc);

-- ── 4. Equity curve (one row per 75-min bar we advance the book through) ──────
create table if not exists sar.equity_curve (
    as_of            timestamptz primary key,                -- 75-min bar close ts
    equity           numeric     not null,                   -- realised equity at this bar
    net_points_cum   numeric,
    state            text,                                   -- position AT this bar
    close            numeric,                                -- 75-min close mark
    is_eod           boolean     not null default false,     -- true once folded by the EOD run
    source           text        default 'live',
    updated_at       timestamptz not null default now()
);

-- ── 5. 75-min candle + EMA cache (for the dashboard chart / stop line) ────────
create table if not exists sar.candles (
    ts               timestamptz primary key,
    open             numeric,
    high             numeric,
    low              numeric,
    close            numeric,
    ema_fast         numeric,
    ema_slow         numeric,
    source           text        default 'future',          -- 'spot_seed' | 'future'
    updated_at       timestamptz not null default now()
);
create index if not exists idx_sar_candles_ts on sar.candles (ts desc);

-- ── 6. Run log — every pipeline invocation leaves a heartbeat ────────────────
create table if not exists sar.runs (
    id               bigint generated always as identity primary key,
    run_ts           timestamptz not null default now(),
    mode             text,                                   -- 'eod' | 'intraday' | 'seed'
    last_bar_ts      timestamptz,                            -- newest completed 75-min bar seen
    bars_processed   int         default 0,                  -- new bars folded this run
    state            text,                                   -- book state after the run
    close            numeric,
    ema_fast         numeric,
    ema_slow         numeric,
    dist_to_stop_pts numeric,                                -- close − EMA13 (signed; how far from the trail)
    action           text,                                   -- human summary of any transition
    status           text        default 'ok'
);
create index if not exists idx_sar_runs_ts on sar.runs (run_ts desc);

-- ── 7. Grants (PostgREST needs these for the exposed schema) ──────────────────
grant usage on schema sar to anon, authenticated, service_role;
grant all privileges on all tables    in schema sar to anon, authenticated, service_role;
grant all privileges on all sequences in schema sar to anon, authenticated, service_role;
alter default privileges in schema sar grant all on tables    to anon, authenticated, service_role;
alter default privileges in schema sar grant all on sequences to anon, authenticated, service_role;
