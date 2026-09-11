-- ============================================================================
-- Blue Sky — intraday poller coordination
-- Adds an is_eod flag so the intraday poller (live snapshots, is_eod=false) and
-- the EOD runner (authoritative daily close, is_eod=true) share one book without
-- stepping on each other. Run once in the Supabase SQL editor.
-- ============================================================================
alter table bluesky.equity_curve
    add column if not exists is_eod boolean not null default false;

comment on column bluesky.equity_curve.is_eod is
    'true once the EOD runner finalised this date; the intraday poller writes false snapshots.';

-- existing rows were all written by the EOD runner -> mark them finalised
update bluesky.equity_curve set is_eod = true where is_eod = false;
