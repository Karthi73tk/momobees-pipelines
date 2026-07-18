-- =========================================================================
-- strategies.momentum_weekly_top25_tracking
-- A view that takes last week's Top 25 stocks and shows where they rank
-- this week, along with their weekly price change percentage.
-- =========================================================================

CREATE OR REPLACE VIEW strategies.momentum_weekly_top25_tracking AS
WITH recent_dates AS (
  SELECT DISTINCT asof_date 
  FROM strategies.momentum_weekly_history 
  WHERE is_rebalance_basis_day = true 
  ORDER BY asof_date DESC 
  LIMIT 2
),
this_week AS (
  SELECT ticker, rank, momentum_score, close
  FROM strategies.momentum_weekly_history 
  WHERE asof_date = (SELECT asof_date FROM recent_dates ORDER BY asof_date DESC LIMIT 1)
),
last_week AS (
  SELECT ticker, rank, momentum_score, close
  FROM strategies.momentum_weekly_history 
  WHERE asof_date = (SELECT asof_date FROM recent_dates ORDER BY asof_date ASC LIMIT 1)
)
SELECT 
  l.ticker,
  l.rank AS last_week_rank,
  t.rank AS this_week_rank,
  (l.rank - t.rank) AS rank_change,
  round(((t.close - l.close) / l.close) * 100, 2) AS weekly_price_change_pct,
  round(l.momentum_score, 4) AS last_week_score,
  round(t.momentum_score, 4) AS this_week_score
FROM last_week l
LEFT JOIN this_week t ON l.ticker = t.ticker
WHERE l.rank <= 25
ORDER BY l.rank ASC;
