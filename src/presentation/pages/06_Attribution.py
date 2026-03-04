from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from src.presentation.data_access import (
    load_account_daily,
    load_attribution_daily,
    load_benchmark_daily_returns,
)


st.title("Attribution")
st.caption("Allocation, selection, interaction, and residual decomposition of portfolio returns.")

attr = load_attribution_daily()
if attr.empty:
    st.warning("No attribution data available. Build marts after asset snapshots are loaded.")
    st.stop()

attr = attr.copy()
attr["d"] = pd.to_datetime(attr["d"], errors="coerce")
attr = attr[attr["d"].notna()].sort_values("d")
if attr.empty:
    st.warning("Attribution mart contains no valid dates.")
    st.stop()

bench = load_benchmark_daily_returns()
if not bench.empty:
    bench = bench.copy()
    bench["d"] = pd.to_datetime(bench["d"], errors="coerce")
    bench = bench[bench["d"].notna()].sort_values("d")
    merged = attr.merge(bench[["d", "benchmark_return"]], on="d", how="left", suffixes=("", "_market"))
    if "benchmark_return_market" in merged.columns:
        merged["benchmark_return_context"] = merged["benchmark_return_market"].fillna(merged["benchmark_return"])
    else:
        merged["benchmark_return_context"] = merged["benchmark_return"]
else:
    merged = attr.copy()
    merged["benchmark_return_context"] = merged["benchmark_return"]

k1, k2, k3, k4 = st.columns(4)
latest = merged.iloc[-1]
k1.metric("Portfolio Return (Daily)", f"{float(latest['portfolio_return']) * 100:.3f}%")
k2.metric("Benchmark Return (Daily)", f"{float(latest['benchmark_return_context']) * 100:.3f}%")
k3.metric("Allocation Effect (Daily)", f"{float(latest['allocation_effect']) * 100:.3f}%")
k4.metric("Selection Effect (Daily)", f"{float(latest['selection_effect']) * 100:.3f}%")

cum_cols = [
    "cumulative_benchmark_return",
    "cumulative_allocation_effect",
    "cumulative_selection_effect",
    "cumulative_interaction_effect",
    "cumulative_residual_effect",
]
cum_long = merged[["d", *cum_cols]].melt("d", var_name="component", value_name="value")
cum_fig = px.line(
    cum_long,
    x="d",
    y="value",
    color="component",
    title="Cumulative Attribution Decomposition",
)
cum_fig.update_yaxes(tickformat=".2%")
st.plotly_chart(cum_fig, use_container_width=True)

monthly = merged.copy()
monthly["month"] = monthly["d"].dt.to_period("M").dt.to_timestamp()
monthly = (
    monthly.groupby("month", as_index=False)[
        ["allocation_effect", "selection_effect", "interaction_effect", "residual_effect"]
    ]
    .sum()
    .sort_values("month")
)
monthly_long = monthly.melt("month", var_name="component", value_name="value")
monthly_fig = px.bar(
    monthly_long,
    x="month",
    y="value",
    color="component",
    title="Monthly Attribution Effects",
)
monthly_fig.update_yaxes(tickformat=".2%")
st.plotly_chart(monthly_fig, use_container_width=True)

accounts = load_account_daily()
if accounts.empty:
    st.info("No account-level data available for drilldown.")
    st.stop()

accounts = accounts.copy()
accounts["d"] = pd.to_datetime(accounts["d"], errors="coerce")
accounts = accounts[accounts["d"].notna()].sort_values(["account_id", "d"])
accounts["prev_close"] = accounts.groupby("account_id")["close_value_gbp"].shift(1)
total_prev = accounts.groupby("d", as_index=False)["prev_close"].sum().rename(columns={"prev_close": "portfolio_prev"})
accounts = accounts.merge(total_prev, on="d", how="left")
accounts["weight_prev"] = accounts["prev_close"] / accounts["portfolio_prev"]
accounts["weight_prev"] = accounts["weight_prev"].fillna(0.0)
accounts["daily_contribution"] = accounts["weight_prev"] * accounts["daily_return_flow_corrected"]

selected_account = st.selectbox("Account Drilldown", sorted(accounts["account_id"].astype(str).unique().tolist()))
account_view = accounts[accounts["account_id"].astype(str) == selected_account].copy()
if account_view.empty:
    st.info("No rows for selected account.")
    st.stop()
account_view["month"] = account_view["d"].dt.to_period("M").dt.to_timestamp()
account_monthly = (
    account_view.groupby("month", as_index=False)[["daily_contribution", "daily_return_flow_corrected"]]
    .sum()
    .sort_values("month")
)

drill_fig = px.bar(
    account_monthly,
    x="month",
    y="daily_contribution",
    title=f"{selected_account}: Monthly Contribution to Portfolio Return",
)
drill_fig.update_yaxes(tickformat=".2%")
st.plotly_chart(drill_fig, use_container_width=True)

st.dataframe(
    merged[
        [
            "d",
            "portfolio_return",
            "benchmark_return_context",
            "allocation_effect",
            "selection_effect",
            "interaction_effect",
            "residual_effect",
        ]
    ].tail(180),
    use_container_width=True,
)
