create table public.cnx500_monthly_returns (
  id bigint generated always as identity not null,
  month text not null,
  return_pct numeric not null,
  created_at timestamp with time zone null default now(),
  constraint cnx500_monthly_returns_pkey primary key (id),
  constraint cnx500_monthly_returns_month_key unique (month)
);

create view public.cnx500_equity_curve as
select
  month,
  return_pct,
  100::numeric * exp(sum(ln(1::numeric + return_pct)) over (order by month)) as equity_index
from cnx500_monthly_returns
order by month;
