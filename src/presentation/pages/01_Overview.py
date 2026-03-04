from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from src.presentation.data_access import load_portfolio_daily


st.title("Overview")

portfolio_df = load_portfolio_daily()
if portfolio_df.empty:
    st.warning("No portfolio mart data available.")
    st.stop()

frame = portfolio_df.copy()
frame["d"] = pd.to_datetime(frame["d"])
latest = frame.sort_values("d").tail(1).iloc[0]
gain = float(latest["close_value_gbp"] - latest["net_deposited_external_to_date_gbp"])

k1, k2, k3, k4 = st.columns(4)
k1.metric("Portfolio Value", f"GBP {latest['close_value_gbp']:,.2f}")
k2.metric("Net Deposits", f"GBP {latest['net_deposited_external_to_date_gbp']:,.2f}")
k3.metric("Gain vs Deposits", f"GBP {gain:,.2f}")
k4.metric("Cumulative TWR", f"{latest['cumulative_twr'] * 100:.2f}%")

trend_df = frame[
    ["d", "close_value_gbp", "net_deposited_external_to_date_gbp", "daily_pnl_flow_corrected_gbp"]
].melt(
    id_vars="d",
    value_vars=["close_value_gbp", "net_deposited_external_to_date_gbp"],
    var_name="series",
    value_name="value",
)
fig = px.line(trend_df, x="d", y="value", color="series", title="Portfolio Value vs Net External Deposits")
st.plotly_chart(fig, use_container_width=True)

bar = px.bar(
    frame,
    x="d",
    y="daily_pnl_flow_corrected_gbp",
    title="Daily Flow-Corrected P/L (GBP)",
)
st.plotly_chart(bar, use_container_width=True)

st.dataframe(
    frame.tail(30)[
        [
            "d",
            "close_value_gbp",
            "external_flow_gbp",
            "daily_pnl_flow_corrected_gbp",
            "daily_return_flow_corrected",
            "cumulative_twr",
        ]
    ],
    use_container_width=True,
)
