from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from src.presentation.data_access import load_account_daily, load_accounts


st.title("Accounts")

account_df = load_account_daily()
dim_df = load_accounts()

if account_df.empty:
    st.warning("No account mart data available.")
    st.stop()

account_df["d"] = pd.to_datetime(account_df["d"])
account_ids = sorted(account_df["account_id"].unique().tolist())

label_map = {r["account_id"]: r["account_name"] for _, r in dim_df.iterrows()} if not dim_df.empty else {}
options = [f"{aid} - {label_map.get(aid, aid)}" for aid in account_ids]
selected_label = st.selectbox("Account", options=options)
selected_account = selected_label.split(" - ", 1)[0]

f = account_df[account_df["account_id"] == selected_account].copy().sort_values("d")
latest = f.tail(1).iloc[0]

k1, k2, k3, k4 = st.columns(4)
k1.metric("Account Value", f"GBP {latest['close_value_gbp']:,.2f}")
k2.metric("Net Deposits", f"GBP {latest['net_deposited_external_to_date_gbp']:,.2f}")
k3.metric("Gain vs Deposits", f"GBP {latest['cumulative_gain_vs_external_deposits_gbp']:,.2f}")
k4.metric("Cumulative TWR", f"{latest['cumulative_twr'] * 100:.2f}%")

value_fig = px.line(
    f,
    x="d",
    y=["close_value_gbp", "net_deposited_external_to_date_gbp"],
    title=f"{selected_account}: Value vs Net Deposits",
)
st.plotly_chart(value_fig, use_container_width=True)

ret_fig = px.line(
    f,
    x="d",
    y="daily_return_flow_corrected",
    title=f"{selected_account}: Daily Flow-Corrected Return",
)
ret_fig.update_yaxes(tickformat=".2%")
st.plotly_chart(ret_fig, use_container_width=True)

st.dataframe(
    f[
        [
            "d",
            "close_value_gbp",
            "external_flow_gbp",
            "internal_flow_gbp",
            "daily_pnl_flow_corrected_gbp",
            "daily_return_flow_corrected",
            "cumulative_twr",
        ]
    ].tail(90),
    use_container_width=True,
)
