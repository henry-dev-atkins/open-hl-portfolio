from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from src.presentation.data_access import (
    load_account_daily,
    load_cost_drag_account_daily,
    load_cost_drag_portfolio_daily,
)


st.title("Cost Drag")
st.caption("Daily fee, tax, and idle-cash drag decomposition with source-quality annotations.")

portfolio_drag = load_cost_drag_portfolio_daily()
account_drag = load_cost_drag_account_daily()
account_daily = load_account_daily()

if portfolio_drag.empty:
    st.warning("No cost-drag mart data available.")
    st.stop()

portfolio_drag = portfolio_drag.copy()
portfolio_drag["d"] = pd.to_datetime(portfolio_drag["d"], errors="coerce")
portfolio_drag = portfolio_drag[portfolio_drag["d"].notna()].sort_values("d")
if portfolio_drag.empty:
    st.warning("No valid cost-drag dates.")
    st.stop()

latest = portfolio_drag.iloc[-1]
k1, k2, k3, k4 = st.columns(4)
k1.metric("Fee Drag (Daily)", f"GBP {float(latest['fee_drag_gbp']):,.2f}")
k2.metric("Tax Drag (Daily)", f"GBP {float(latest['tax_drag_gbp']):,.2f}")
k3.metric("Idle Cash Drag (Daily)", f"GBP {float(latest['idle_cash_drag_gbp']):,.2f}")
k4.metric("Total Drag (Daily)", f"GBP {float(latest['total_drag_gbp']):,.2f}")

long_drag = portfolio_drag.melt(
    "d",
    value_vars=["fee_drag_gbp", "tax_drag_gbp", "idle_cash_drag_gbp"],
    var_name="component",
    value_name="value_gbp",
)
drag_fig = px.area(long_drag, x="d", y="value_gbp", color="component", title="Portfolio Drag Components")
st.plotly_chart(drag_fig, use_container_width=True)

cum = portfolio_drag.copy()
cum["cum_fee_drag"] = cum["fee_drag_gbp"].cumsum()
cum["cum_tax_drag"] = cum["tax_drag_gbp"].cumsum()
cum["cum_idle_cash_drag"] = cum["idle_cash_drag_gbp"].cumsum()
cum["cum_total_drag"] = cum["total_drag_gbp"].cumsum()
cum_long = cum.melt(
    "d",
    value_vars=["cum_fee_drag", "cum_tax_drag", "cum_idle_cash_drag", "cum_total_drag"],
    var_name="component",
    value_name="value_gbp",
)
cum_fig = px.line(cum_long, x="d", y="value_gbp", color="component", title="Cumulative Drag")
st.plotly_chart(cum_fig, use_container_width=True)

if not account_daily.empty:
    acct = account_daily.copy()
    acct["d"] = pd.to_datetime(acct["d"], errors="coerce")
    acct = acct[acct["d"].notna()].copy()
    acct = acct.groupby("d", as_index=False)["close_value_gbp"].sum().rename(columns={"close_value_gbp": "portfolio_value_gbp"})
    bps = portfolio_drag.merge(acct, on="d", how="left")
    bps["drag_bps"] = bps["total_drag_gbp"] / bps["portfolio_value_gbp"].replace(0, pd.NA) * 10000.0
    bps_fig = px.line(bps, x="d", y="drag_bps", title="Total Drag (bps of Portfolio Value)")
    st.plotly_chart(bps_fig, use_container_width=True)

if account_drag.empty:
    st.caption("No account-level drag rows available.")
    st.stop()

account_drag = account_drag.copy()
account_drag["d"] = pd.to_datetime(account_drag["d"], errors="coerce")
account_drag = account_drag[account_drag["d"].notna()].sort_values(["account_id", "d"])
account_options = sorted(account_drag["account_id"].astype(str).unique().tolist())
selected_account = st.selectbox("Account", account_options)
view = account_drag[account_drag["account_id"].astype(str) == selected_account].copy()
if view.empty:
    st.caption("No rows for selected account.")
    st.stop()

quality_counts = view["cash_source_quality"].fillna("unknown").value_counts()
st.caption("Cash source quality: " + ", ".join([f"{k}={v}" for k, v in quality_counts.items()]))

account_fig = px.line(
    view,
    x="d",
    y=["fee_drag_gbp", "tax_drag_gbp", "idle_cash_drag_gbp", "total_drag_gbp"],
    title=f"{selected_account}: Drag Components",
)
st.plotly_chart(account_fig, use_container_width=True)

st.dataframe(
    view[
        [
            "d",
            "fee_drag_gbp",
            "tax_drag_gbp",
            "idle_cash_drag_gbp",
            "total_drag_gbp",
            "cash_balance_prev_gbp",
            "cash_benchmark_daily_rate",
            "cash_source_quality",
        ]
    ].tail(180),
    use_container_width=True,
)
