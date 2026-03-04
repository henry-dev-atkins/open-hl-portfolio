with account_dates as (
  select
    v.account_id,
    v.d,
    v.close_value_gbp,
    coalesce(f.external_flow_gbp, 0) as external_flow_gbp,
    coalesce(f.internal_flow_gbp, 0) as internal_flow_gbp
  from stg_account_value_daily v
  left join stg_account_flow_daily f
    on v.account_id = f.account_id
   and v.d = f.d
   and v.source_run_id = f.source_run_id
  where v.source_run_id = $run_id
),
base as (
  select
    account_id,
    d,
    close_value_gbp,
    external_flow_gbp,
    internal_flow_gbp,
    lag(close_value_gbp) over (partition by account_id order by d) as prev_value
  from account_dates
),
perf as (
  select
    account_id,
    d,
    close_value_gbp,
    external_flow_gbp,
    internal_flow_gbp,
    coalesce(sum(external_flow_gbp) over (partition by account_id order by d rows between unbounded preceding and current row), 0) as net_deposited_external_to_date_gbp,
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
  account_id,
  d,
  close_value_gbp,
  external_flow_gbp,
  internal_flow_gbp,
  net_deposited_external_to_date_gbp,
  daily_pnl_flow_corrected_gbp,
  daily_return_flow_corrected,
  exp(sum(ln(1 + daily_return_flow_corrected)) over (
    partition by account_id order by d rows between unbounded preceding and current row
  )) - 1 as cumulative_twr,
  close_value_gbp - net_deposited_external_to_date_gbp as cumulative_gain_vs_external_deposits_gbp
from perf
order by account_id, d;
