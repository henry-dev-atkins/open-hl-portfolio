from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from src.metrics.rolling import compute_drawdown_episodes, compute_rolling_returns, compute_worst_trailing_windows
from src.presentation.data_access import load_account_daily, load_concentration_daily, load_portfolio_daily


st.title("Rolling & Concentration")
st.caption("Rolling 6M/1Y returns, worst trailing 1Y windows, recovery cycles, and concentration risk.")

portfolio = load_portfolio_daily()
accounts = load_account_daily()
conc = load_concentration_daily()

if portfolio.empty:
    st.warning("No portfolio mart data available.")
    st.stop()

portfolio = portfolio.copy()
portfolio["d"] = pd.to_datetime(portfolio["d"], errors="coerce")
portfolio = portfolio[portfolio["d"].notna()].sort_values("d")
if portfolio.empty:
    st.warning("No valid portfolio dates available.")
    st.stop()

scope = st.selectbox("Scope", ["Portfolio", "Account"])

if scope == "Account":
    if accounts.empty:
        st.warning("No account-level rows available.")
        st.stop()
    options = sorted(accounts["account_id"].astype(str).unique().tolist())
    selected = st.selectbox("Account", options=options)
    frame = accounts[accounts["account_id"].astype(str) == selected].copy()
else:
    selected = "PORTFOLIO"
    frame = portfolio.copy()

frame["d"] = pd.to_datetime(frame["d"], errors="coerce")
frame = frame[frame["d"].notna()].sort_values("d")
rolling = compute_rolling_returns(frame)
worst = compute_worst_trailing_windows(frame, top_n=10)
episodes = compute_drawdown_episodes(frame)

if rolling.empty:
    st.warning("Not enough data to compute rolling windows.")
    st.stop()

roll_long = rolling.melt("d", var_name="window", value_name="value")
roll_fig = px.line(roll_long, x="d", y="value", color="window", title=f"{selected}: Rolling Returns")
roll_fig.update_yaxes(tickformat=".2%")
st.plotly_chart(roll_fig, use_container_width=True)

if not worst.empty:
    worst_fig = px.bar(
        worst,
        x="window_end",
        y="trailing_1y_return",
        title=f"{selected}: Worst Trailing 1Y Windows",
    )
    worst_fig.update_yaxes(tickformat=".2%")
    st.plotly_chart(worst_fig, use_container_width=True)
else:
    st.caption("No full 1Y window available yet.")

if not episodes.empty:
    k1, k2, k3 = st.columns(3)
    k1.metric("Drawdown Episodes", f"{len(episodes)}")
    k2.metric("Worst Drawdown", f"{episodes['max_drawdown'].min() * 100:.2f}%")
    recovered = episodes[episodes["is_recovered"]]
    longest = int(recovered["days_to_recover"].max()) if not recovered.empty else 0
    k3.metric("Longest Recovery (days)", f"{longest}")
    st.dataframe(episodes.sort_values("max_drawdown").head(20), use_container_width=True)

if conc.empty:
    st.caption("No concentration mart data available.")
    st.stop()

conc = conc.copy()
conc["d"] = pd.to_datetime(conc["d"], errors="coerce")
conc = conc[conc["d"].notna()].sort_values("d")
if scope == "Portfolio":
    conc_view = conc[(conc["scope_type"] == "portfolio") & (conc["scope_id"] == "PORTFOLIO")].copy()
else:
    conc_view = conc[(conc["scope_type"] == "account") & (conc["scope_id"].astype(str) == selected)].copy()

if conc_view.empty:
    st.caption("No concentration rows for selected scope.")
    st.stop()

conc_long = conc_view.melt(
    ["d"],
    value_vars=["max_single_weight", "top5_weight", "top10_weight", "hhi"],
    var_name="metric",
    value_name="value",
)
conc_fig = px.line(conc_long, x="d", y="value", color="metric", title=f"{selected}: Concentration Metrics")
conc_fig.update_yaxes(tickformat=".2%")
st.plotly_chart(conc_fig, use_container_width=True)

breaches = conc_view[
    conc_view[["breach_single", "breach_top5", "breach_top10", "breach_hhi"]].notna().any(axis=1)
].copy()
if breaches.empty:
    st.caption("No concentration breaches detected.")
else:
    st.subheader("Breach Log")
    st.dataframe(
        breaches[
            [
                "d",
                "max_single_weight",
                "top5_weight",
                "top10_weight",
                "hhi",
                "breach_single",
                "breach_top5",
                "breach_top10",
                "breach_hhi",
            ]
        ].tail(120),
        use_container_width=True,
    )
