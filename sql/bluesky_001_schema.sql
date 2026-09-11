-- ============================================================================
-- Blue Sky Breakout — schema + tables
-- Run this ONCE in the Supabase SQL editor.
--
-- After running, also add 'bluesky' to the API-exposed schemas:
--   Supabase Dashboard → Project Settings → API → "Exposed schemas" → add: bluesky
-- (the pipeline writes via PostgREST with the service-role key, which requires the
--  schema to be exposed — same as the existing 'universe' / 'stage' schemas.)
-- ============================================================================

create schema if not exists bluesky;

-- ── 1. Daily candidate list (armed-and-ready, appended each EOD run) ──────────
-- One row per (run_date, ticker). Re-running the same evening upserts in place.
create table if not exists bluesky.daily_candidates (
    run_date            date        not null,   -- the EOD session this scan reflects
    ticker              text        not null,
    company             text,
    status              text        not null,   -- 'ARMED' = within 20% below pivot, place buy-stop next day
                                                --  'TRIGGERED' = closed at/above pivot today (new all-time high)
    rs                  int,                    -- IBD-blend relative-strength rank 1-99
    close               numeric,                -- last close
    pivot               numeric,                -- all-time high = the trigger/ceiling
    trigger_price       numeric,                -- buy-stop level (= pivot)
    dist_to_pivot_pct   numeric,                -- (pivot/close - 1)*100  (>0 = still below trigger)
    stop_pivot          numeric,                -- pivot * 0.92  (8% below pivot)
    mcap_cr             numeric,                -- market cap, ₹ crore
    turnover_cr         numeric,                -- 20d avg daily traded value, ₹ crore
    above_sma50         boolean,
    ath_date            date,                   -- when the all-time high was set
    screened_at         timestamptz not null default now(),  -- LAST UPDATED timestamp
    primary key (run_date, ticker)
);
create index if not exists idx_bluesky_candidates_date   on bluesky.daily_candidates (run_date desc);
create index if not exists idx_bluesky_candidates_status on bluesky.daily_candidates (run_date desc, status);
create index if not exists idx_bluesky_candidates_rs     on bluesky.daily_candidates (run_date desc, rs desc);

-- Small run-log so we always know when the last successful scan finished.
create table if not exists bluesky.screener_runs (
    run_date            date        primary key,
    universe_scanned    int,
    eligible_liquid     int,        -- passed mcap + turnover floor
    candidates_total    int,        -- passed full funnel (written to daily_candidates)
    armed               int,
    triggered           int,
    finished_at         timestamptz not null default now(),
    status              text        default 'ok'
);

-- ── 2. Virtual portfolio — two variants, ₹10,00,000 each ─────────────────────
create table if not exists bluesky.variants (
    variant_id          text        primary key,        -- 'A', 'B'
    name                text        not null,
    start_capital       numeric     not null default 1000000,
    risk_pct            numeric     not null default 1.5,   -- % of equity risked per trade
    stop_pct            numeric     not null default 8,     -- hard stop distance %
    stop_ref            text        not null default 'entry', -- 'entry' | 'pivot' (where the 8% is measured from)
    entry_mode          text        not null default 'at_pivot',
    max_positions       int         not null default 10,
    max_position_frac   numeric     not null default 30,    -- cap on a single position, % of equity
    notes               text,
    created_at          timestamptz default now()
);

-- Open positions (live book) per variant.
create table if not exists bluesky.positions (
    id                  bigint generated always as identity primary key,
    variant_id          text        not null references bluesky.variants(variant_id),
    ticker              text        not null,
    entry_date          date        not null,
    entry_price         numeric     not null,
    shares              int         not null,
    stop_price          numeric     not null,
    pivot               numeric,
    be_locked           boolean     default false,          -- breakeven lock engaged
    last_price          numeric,
    opened_at           timestamptz default now(),
    updated_at          timestamptz default now(),
    unique (variant_id, ticker)
);
create index if not exists idx_bluesky_positions_variant on bluesky.positions (variant_id);

-- Closed trades (realised) per variant.
create table if not exists bluesky.trades (
    id                  bigint generated always as identity primary key,
    variant_id          text        not null references bluesky.variants(variant_id),
    ticker              text        not null,
    entry_date          date,
    entry_price         numeric,
    shares              int,
    exit_date           date,
    exit_price          numeric,
    return_pct          numeric,
    pnl                 numeric,
    exit_reason         text,                               -- 'stop_8pct' | 'below_50d' | 'still_open'
    created_at          timestamptz default now()
);
create index if not exists idx_bluesky_trades_variant on bluesky.trades (variant_id, exit_date desc);

-- Daily NAV / equity curve per variant.
create table if not exists bluesky.equity_curve (
    variant_id          text        not null references bluesky.variants(variant_id),
    as_of               date        not null,
    cash                numeric,
    positions_value     numeric,
    equity              numeric,
    n_open              int,
    n_passed            int,                                -- candidates passed up on capacity that day
    updated_at          timestamptz default now(),
    primary key (variant_id, as_of)
);

-- ── 3. Seed the two variants (₹10L each) ─────────────────────────────────────
insert into bluesky.variants (variant_id, name, risk_pct, stop_ref, notes) values
    ('A', 'Blue Sky — 8% stop from ENTRY', 1.5, 'entry',
          'Closing -8% stop measured from the fill price.'),
    ('B', 'Blue Sky — 8% stop from PIVOT', 1.5, 'pivot',
          'Closing -8% stop measured from the pivot (≈ -10% from a 1-2% extended fill); shakeout-resistant.')
on conflict (variant_id) do nothing;

-- ── 4. Grants (PostgREST needs these for the exposed schema) ──────────────────
grant usage on schema bluesky to anon, authenticated, service_role;
grant all privileges on all tables    in schema bluesky to anon, authenticated, service_role;
grant all privileges on all sequences in schema bluesky to anon, authenticated, service_role;
alter default privileges in schema bluesky grant all on tables    to anon, authenticated, service_role;
alter default privileges in schema bluesky grant all on sequences to anon, authenticated, service_role;
