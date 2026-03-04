with p as (
  select
    d,
    sum(close_value_gbp) as close_value_gbp,
    sum(external_flow_gbp) as external_flow_gbp
  from mart_account_daily
  group by d
),
base as (
  select
    d,
    close_value_gbp,
    external_flow_gbp,
    lag(close_value_gbp) over (order by d) as prev_value
  from p
),
perf as (
  select
    d,
    close_value_gbp,
    external_flow_gbp,
    coalesce(sum(external_flow_gbp) over (order by d rows between unbounded preceding and current row), 0) as net_deposited_external_to_date_gbp,
    case
      when prev_value is null then 0
      else close_value_gbp - prev_value - external_flow_gbp
    end as daily_pnl_flow_corrected_gbp,
    case
      when prev_value is null then 0
      when abs(prev_value + 0.5 * external_flow_gbp) < 1e-9 then 0
      else (close_value_gbp - prev_value - external_flow_gbp) / (prev_value + 0.5 * external_flow_gbp)
    end as daily_return_flow_corrected
  from base
)
select
  d,
  close_value_gbp,
  external_flow_gbp,
  net_deposited_external_to_date_gbp,
  daily_pnl_flow_corrected_gbp,
  daily_return_flow_corrected,
  exp(sum(ln(1 + daily_return_flow_corrected)) over (
    order by d rows between unbounded preceding and current row
  )) - 1 as cumulative_twr
from perf
order by d;
