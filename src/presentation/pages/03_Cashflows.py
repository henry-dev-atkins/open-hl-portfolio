from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from src.presentation.data_access import load_account_daily


st.title("Cashflows")

account_df = load_account_daily()
if account_df.empty:
    st.warning("No account mart data available.")
    st.stop()

frame = account_df.copy()
frame["d"] = pd.to_datetime(frame["d"])

account_filter = st.multiselect(
    "Accounts",
    options=sorted(frame["account_id"].unique().tolist()),
    default=sorted(frame["account_id"].unique().tolist()),
)

filtered = frame[frame["account_id"].isin(account_filter)].copy()
if filtered.empty:
    st.info("No rows for selected filters.")
    st.stop()

daily_flows = filtered.groupby(["d", "account_id"], as_index=False)[["external_flow_gbp", "internal_flow_gbp"]].sum()

external_fig = px.bar(
    daily_flows,
    x="d",
    y="external_flow_gbp",
    color="account_id",
    title="Daily External Flows (Deposits/Withdrawals)",
)
st.plotly_chart(external_fig, use_container_width=True)

internal_fig = px.bar(
    daily_flows,
    x="d",
    y="internal_flow_gbp",
    color="account_id",
    title="Daily Internal Transfers",
)
st.plotly_chart(internal_fig, use_container_width=True)

monthly = daily_flows.copy()
monthly["month"] = monthly["d"].dt.to_period("M").dt.to_timestamp()
monthly = monthly.groupby(["month", "account_id"], as_index=False)[["external_flow_gbp", "internal_flow_gbp"]].sum()

monthly_fig = px.bar(
    monthly,
    x="month",
    y="external_flow_gbp",
    color="account_id",
    title="Monthly Net External Flow by Account",
)
st.plotly_chart(monthly_fig, use_container_width=True)

st.dataframe(monthly.sort_values(["month", "account_id"]), use_container_width=True)
