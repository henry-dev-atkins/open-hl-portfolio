create table if not exists raw_import_runs (
  run_id varchar primary key,
  imported_at timestamp not null,
  source_path varchar not null,
  hl_export_date date,
  notes varchar
);

create table if not exists raw_source_files (
  run_id varchar not null,
  source_file varchar not null,
  file_sha256 varchar not null,
  file_size_bytes bigint not null,
  detected_kind varchar not null,
  primary key (run_id, source_file)
);

create table if not exists raw_transactions (
  run_id varchar not null,
  source_file varchar not null,
  row_num integer not null,
  account_name_raw varchar not null,
  account_id varchar,
  trade_date date,
  settle_date date,
  txn_type_raw varchar not null,
  description_raw varchar,
  amount_gbp decimal(18, 2),
  currency varchar,
  instrument_name varchar,
  isin varchar,
  sedol varchar,
  units decimal(24, 10),
  price decimal(24, 10),
  balance_after_gbp decimal(18, 2),
  primary key (run_id, source_file, row_num)
);

create table if not exists raw_valuations (
  run_id varchar not null,
  source_file varchar not null,
  row_num integer not null,
  account_name_raw varchar not null,
  account_id varchar,
  valuation_date date not null,
  total_value_gbp decimal(18, 2) not null,
  cash_value_gbp decimal(18, 2),
  invested_value_gbp decimal(18, 2),
  primary key (run_id, source_file, row_num)
);

create table if not exists raw_holdings_snapshot (
  run_id varchar not null,
  source_file varchar not null,
  row_num integer not null,
  account_name_raw varchar not null,
  account_id varchar,
  as_of_date date not null,
  asset_name varchar not null,
  market_value_gbp decimal(18, 2) not null,
  isin varchar,
  sedol varchar,
  units decimal(24, 10),
  currency varchar,
  primary key (run_id, source_file, row_num)
);

create table if not exists dim_account (
  account_id varchar primary key,
  account_name varchar not null,
  account_type varchar,
  tax_wrapper varchar,
  opened_date date,
  closed_date date
);

create table if not exists stg_transactions (
  account_id varchar not null,
  event_date date not null,
  txn_type varchar not null,
  amount_gbp decimal(18, 2) not null,
  flow_class varchar not null,
  source_run_id varchar not null,
  source_file varchar not null,
  source_row_num integer not null
);

create table if not exists stg_account_value_daily (
  account_id varchar not null,
  d date not null,
  close_value_gbp decimal(18, 2) not null,
  source_run_id varchar not null,
  primary key (account_id, d, source_run_id)
);

create table if not exists stg_account_flow_daily (
  account_id varchar not null,
  d date not null,
  external_flow_gbp decimal(18, 2) not null,
  internal_flow_gbp decimal(18, 2) not null,
  source_run_id varchar not null,
  primary key (account_id, d, source_run_id)
);

create table if not exists stg_asset_checkpoint (
  account_id varchar not null,
  asset_id varchar not null,
  asset_name_canonical varchar not null,
  d date not null,
  value_gbp decimal(18, 2) not null,
  isin varchar,
  sedol varchar,
  source_run_id varchar not null,
  source_file varchar,
  source_row_num integer,
  primary key (account_id, asset_id, d, source_run_id)
);

create table if not exists stg_account_cost_daily (
  account_id varchar not null,
  d date not null,
  fee_gbp decimal(18, 2) not null,
  tax_gbp decimal(18, 2) not null,
  cash_interest_gbp decimal(18, 2) not null,
  source_run_id varchar not null,
  primary key (account_id, d, source_run_id)
);

create table if not exists stg_account_cash_daily (
  account_id varchar not null,
  d date not null,
  cash_balance_gbp decimal(18, 2) not null,
  source_quality varchar not null,
  source_run_id varchar not null,
  primary key (account_id, d, source_run_id)
);

create table if not exists mart_account_daily (
  account_id varchar not null,
  d date not null,
  close_value_gbp decimal(18, 2) not null,
  external_flow_gbp decimal(18, 2) not null,
  internal_flow_gbp decimal(18, 2) not null,
  net_deposited_external_to_date_gbp decimal(18, 2) not null,
  daily_pnl_flow_corrected_gbp decimal(18, 2) not null,
  daily_return_flow_corrected decimal(18, 8) not null,
  cumulative_twr decimal(18, 8) not null,
  cumulative_gain_vs_external_deposits_gbp decimal(18, 2) not null,
  primary key (account_id, d)
);

create table if not exists mart_portfolio_daily (
  d date primary key,
  close_value_gbp decimal(18, 2) not null,
  external_flow_gbp decimal(18, 2) not null,
  net_deposited_external_to_date_gbp decimal(18, 2) not null,
  daily_pnl_flow_corrected_gbp decimal(18, 2) not null,
  daily_return_flow_corrected decimal(18, 8) not null,
  cumulative_twr decimal(18, 8) not null
);

create table if not exists mart_asset_daily (
  account_id varchar not null,
  asset_id varchar not null,
  d date not null,
  value_gbp decimal(18, 2) not null,
  daily_return decimal(18, 8),
  weight decimal(18, 10),
  interpolation_method varchar not null,
  source_run_id varchar,
  primary key (account_id, asset_id, d)
);

create table if not exists mart_cost_drag_account_daily (
  account_id varchar not null,
  d date not null,
  fee_drag_gbp decimal(18, 2) not null,
  tax_drag_gbp decimal(18, 2) not null,
  idle_cash_drag_gbp decimal(18, 2) not null,
  total_drag_gbp decimal(18, 2) not null,
  cash_balance_prev_gbp decimal(18, 2),
  cash_benchmark_daily_rate decimal(18, 8) not null,
  cash_source_quality varchar,
  source_run_id varchar,
  primary key (account_id, d)
);

create table if not exists mart_cost_drag_portfolio_daily (
  d date primary key,
  fee_drag_gbp decimal(18, 2) not null,
  tax_drag_gbp decimal(18, 2) not null,
  idle_cash_drag_gbp decimal(18, 2) not null,
  total_drag_gbp decimal(18, 2) not null,
  cash_balance_prev_gbp decimal(18, 2),
  cash_benchmark_daily_rate decimal(18, 8) not null,
  source_run_id varchar
);

create table if not exists mart_attribution_daily (
  d date primary key,
  benchmark_return decimal(18, 8),
  allocation_effect decimal(18, 8),
  selection_effect decimal(18, 8),
  interaction_effect decimal(18, 8),
  residual_effect decimal(18, 8),
  portfolio_return decimal(18, 8),
  cumulative_benchmark_return decimal(18, 8),
  cumulative_allocation_effect decimal(18, 8),
  cumulative_selection_effect decimal(18, 8),
  cumulative_interaction_effect decimal(18, 8),
  cumulative_residual_effect decimal(18, 8),
  source_run_id varchar
);

create table if not exists mart_concentration_daily (
  scope_type varchar not null,
  scope_id varchar not null,
  d date not null,
  max_single_weight decimal(18, 10),
  top5_weight decimal(18, 10),
  top10_weight decimal(18, 10),
  hhi decimal(18, 10),
  effective_n decimal(18, 4),
  breach_single varchar,
  breach_top5 varchar,
  breach_top10 varchar,
  breach_hhi varchar,
  source_run_id varchar,
  primary key (scope_type, scope_id, d)
);

create table if not exists dim_asset (
  asset_id varchar primary key,
  asset_name_canonical varchar not null,
  isin varchar,
  sedol varchar,
  first_seen_date date,
  last_seen_date date,
  source_priority varchar
);

create table if not exists asset_ticker_mapping (
  asset_id varchar not null,
  provider varchar not null,
  ticker varchar,
  currency varchar,
  confidence_score decimal(6, 2),
  match_status varchar not null,
  mapping_source varchar,
  is_locked boolean default false,
  matched_at timestamp,
  notes varchar,
  yf_quote_url varchar,
  yf_history_url varchar,
  hl_security_url varchar,
  hl_search_url varchar,
  hl_link_source varchar,
  primary key (asset_id, provider)
);

create table if not exists asset_ticker_candidates (
  asset_id varchar not null,
  provider varchar not null,
  query varchar,
  ticker varchar,
  longname varchar,
  exchange varchar,
  quote_type varchar,
  currency varchar,
  score decimal(6, 2),
  rank integer,
  evaluated_at timestamp,
  yf_quote_url varchar,
  hl_search_url varchar
);

create table if not exists raw_market_price_daily (
  provider varchar not null,
  ticker varchar not null,
  d date not null,
  open decimal(24, 10),
  high decimal(24, 10),
  low decimal(24, 10),
  close decimal(24, 10),
  adj_close decimal(24, 10),
  volume bigint,
  currency varchar,
  fetched_at timestamp not null,
  primary key (provider, ticker, d)
);

alter table if exists asset_ticker_mapping add column if not exists yf_quote_url varchar;
alter table if exists asset_ticker_mapping add column if not exists yf_history_url varchar;
alter table if exists asset_ticker_mapping add column if not exists hl_security_url varchar;
alter table if exists asset_ticker_mapping add column if not exists hl_search_url varchar;
alter table if exists asset_ticker_mapping add column if not exists hl_link_source varchar;
alter table if exists asset_ticker_candidates add column if not exists yf_quote_url varchar;
alter table if exists asset_ticker_candidates add column if not exists hl_search_url varchar;
