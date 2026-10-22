-- security_001_lockdown_and_sip_lab.sql
--
-- 1. Lock down writes. The MoMoBees app writes only to public.* user tables
--    (portfolios, holdings, transactions, nav_history, ...) and execution.*
--    (via the service-role client). Every market-data schema is filled by
--    momobees-pipelines with the service-role key, which these REVOKEs do not
--    touch. Before this, the public anon key could INSERT/UPDATE/DELETE
--    nf.*, bnf.*, public.cnx500_monthly_returns and sip_lab.market_regimes
--    (no RLS on them), and table grants elsewhere were held back only by RLS.
--
-- 2. Expose sip_lab read-only, so the ETF / SIP Lab page stops failing with
--    PGRST106. Done after step 1, since sip_lab.market_regimes had anon
--    write grants and no RLS.

begin;

-- ── 1a. Revoke table writes from the API roles on market-data schemas ──────
do $$
declare s text;
begin
  foreach s in array array['universe','screener','strategies','market_pulse','stage',
                           'rrg','pivot','swing_professor','bluesky','sar','model',
                           'nf','bnf','sip_lab','ipo']
  loop
    if exists (select 1 from pg_namespace where nspname = s) then
      execute format('revoke insert, update, delete, truncate on all tables in schema %I from anon, authenticated', s);
      -- tables the pipelines create later must not pick the grants back up
      execute format('alter default privileges for role postgres in schema %I revoke insert, update, delete, truncate on tables from anon, authenticated', s);
    end if;
  end loop;
end $$;

-- public holds the user tables the app writes, so only the market tables here
revoke insert, update, delete, truncate on
  public.cnx500_monthly_returns, public.nifty_monthly_returns,
  public.backtest_monthly_returns, public.stock_universe,
  public.ng_allocation_quarterly, public.ng_ratio_monthly, public.ng_signals_monthly
from anon, authenticated;

-- ── 1b. RLS on the tables that had none (advisor: rls_disabled_in_public) ──
-- Read stays exactly as it is today (anon + authenticated); writes are
-- service-role only, which bypasses RLS.
do $$
declare t text;
begin
  foreach t in array array[
    'strategies.momentum_weekly','strategies.momentum_weekly_history','strategies.momentum_weekly_regime',
    'strategies.st2_backtest_dashboard','strategies.st2_backtest_equity_curve','strategies.st2_backtest_trades',
    'strategies.momentum_weekly_backtest_dashboard','strategies.momentum_weekly_backtest_equity_curve',
    'strategies.momentum_weekly_backtest_benchmark_curve','strategies.momentum_weekly_backtest_trades',
    'universe.indices','screener.indices_metrics','public.cnx500_monthly_returns',
    'sip_lab.market_regimes',
    'nf.daily','nf.min_15','nf.min_60','nf.min_75','bnf.daily','bnf.min_15','bnf.min_60','bnf.min_75']
  loop
    execute format('alter table %s enable row level security', t);
    execute format('drop policy if exists "read" on %s', t);
    execute format('create policy "read" on %s for select to anon, authenticated using (true)', t);
  end loop;
end $$;

-- ── 2. Expose sip_lab read-only ────────────────────────────────────────────
grant usage on schema sip_lab to anon, authenticated;
grant select on all tables in schema sip_lab to anon, authenticated;
-- the view ran as its owner (advisor: security_definer_view); run it as the caller
alter view sip_lab.market_regimes_v set (security_invoker = true);

-- same list as today, plus sip_lab
alter role authenticator set pgrst.db_schemas =
  'public, universe, model, strategies, screener, stage, rrg, market_pulse, pivot, swing_professor, nf, bnf, ipo, execution, graphql_public, bluesky, sar, sip_lab';

commit;

notify pgrst, 'reload config';
notify pgrst, 'reload schema';
