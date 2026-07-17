-- =========================================================================
-- strategies.momentum_weekly_comparison
-- A view that automatically compares the rankings and scores of the 
-- two most recent rebalance basis days (Fridays).
-- =========================================================================

CREATE OR REPLACE VIEW strategies.momentum_weekly_comparison AS
WITH recent_dates AS (
  -- Get the two most recent Fridays where the ranking was saved
  SELECT DISTINCT asof_date 
  FROM strategies.momentum_weekly_history 
  WHERE is_rebalance_basis_day = true 
  ORDER BY asof_date DESC 
  LIMIT 2
),
this_week AS (
  SELECT ticker, rank, momentum_score 
  FROM strategies.momentum_weekly_history 
  WHERE asof_date = (SELECT asof_date FROM recent_dates ORDER BY asof_date DESC LIMIT 1)
),
last_week AS (
  SELECT ticker, rank, momentum_score 
  FROM strategies.momentum_weekly_history 
  WHERE asof_date = (SELECT asof_date FROM recent_dates ORDER BY asof_date ASC LIMIT 1)
)
SELECT 
  t.ticker,
  t.rank AS this_week_rank,
  l.rank AS last_week_rank,
  (l.rank - t.rank) AS rank_change,
  round(t.momentum_score, 4) AS this_week_score,
  round(l.momentum_score, 4) AS last_week_score
FROM this_week t
LEFT JOIN last_week l ON t.ticker = l.ticker;
