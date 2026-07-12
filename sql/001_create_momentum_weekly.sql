-- =========================================================================
-- strategies.momentum_weekly
-- Weekly-rebalanced N500 momentum ranking.
-- Ranking = performance_52w_pct / volatility_52w_pct (same "sharpe_return"
-- style formula as strategies.momentum), no ATH / EMA / RS / market filters.
-- =========================================================================

create table strategies.momentum_weekly (
  ticker                text        not null,
  rank                  integer     not null,
  asof_date             date        not null,          -- trading date the calc is anchored to
  week_start_date       date        null,               -- Monday of the ISO week this snapshot belongs to
  exchange              text        not null default 'NSE'::text,

  close                 numeric     null,

  -- Inputs to the ranking formula
  performance_52w_pct   numeric     null,               -- ROC(252 trading days) * 100
  volatility_52w_pct    numeric     null,               -- std(close)/mean(close) over the same 252-bar window * 100
  momentum_score        numeric     null,               -- performance_52w_pct / volatility_52w_pct  (ranking key)

  data_bars             integer     null,               -- number of daily closes actually used
  rank_criteria         text        null,               -- e.g. 'momentum_score'

  day_of_week           text        null,               -- e.g. 'MONDAY' .. 'FRIDAY', derived from asof_date
  is_rebalance_basis_day boolean     not null default false, -- true only on Friday's close (the calc basis for the week's rebalance)
  rebalance_execution_date date      null,               -- the following Monday trades based on this row should execute on (set only when is_rebalance_basis_day = true)

  metadata               jsonb       not null default '{}'::jsonb,

  refreshed_at          timestamp with time zone not null default now(),
  created_at            timestamp with time zone not null default now(),
  updated_at            timestamp with time zone not null default now(),

  constraint momentum_weekly_pkey primary key (ticker)
) TABLESPACE pg_default;

create index IF not exists momentum_weekly_asof_date_idx
  on strategies.momentum_weekly using btree (asof_date) TABLESPACE pg_default;

create index IF not exists momentum_weekly_rank_idx
  on strategies.momentum_weekly using btree (rank) TABLESPACE pg_default;

create index IF not exists momentum_weekly_week_start_idx
  on strategies.momentum_weekly using btree (week_start_date) TABLESPACE pg_default;

-- Reuses the same trigger function already defined for strategies.momentum
create trigger set_momentum_weekly_updated_at BEFORE
update on strategies.momentum_weekly for EACH row
execute FUNCTION strategies.set_updated_at ();


-- =========================================================================
-- strategies.momentum_weekly_history
-- Append-only, one row per ticker per asof_date. `momentum_weekly` above is
-- overwritten every day the pipeline runs (delete-all-insert), so it only
-- ever shows "where things stand today". This table keeps every day's
-- snapshot so you can look back at Monday/Wednesday standings vs. Friday's
-- closing (rebalance) snapshot within the same week.
-- =========================================================================

create table strategies.momentum_weekly_history (
  ticker                text        not null,
  asof_date             date        not null,
  rank                  integer     not null,
  week_start_date       date        null,
  exchange              text        not null default 'NSE'::text,

  close                 numeric     null,
  performance_52w_pct   numeric     null,
  volatility_52w_pct    numeric     null,
  momentum_score        numeric     null,

  data_bars             integer     null,
  rank_criteria         text        null,

  day_of_week           text        null,
  is_rebalance_basis_day boolean     not null default false,
  rebalance_execution_date date      null,

  metadata               jsonb       not null default '{}'::jsonb,
  created_at            timestamp with time zone not null default now(),

  constraint momentum_weekly_history_pkey primary key (ticker, asof_date)
) TABLESPACE pg_default;

create index IF not exists momentum_weekly_history_asof_date_idx
  on strategies.momentum_weekly_history using btree (asof_date) TABLESPACE pg_default;

create index IF not exists momentum_weekly_history_rank_idx
  on strategies.momentum_weekly_history using btree (asof_date, rank) TABLESPACE pg_default;

create index IF not exists momentum_weekly_history_rebalance_idx
  on strategies.momentum_weekly_history using btree (asof_date) TABLESPACE pg_default
  where is_rebalance_basis_day;
