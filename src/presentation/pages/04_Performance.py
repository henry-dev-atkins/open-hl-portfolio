from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

from src.metrics.performance import compute_trading_metrics
from src.presentation.data_access import load_portfolio_daily

METRIC_LABELS = {
    "sharpe_ratio": "Sharpe Ratio",
    "sortino_ratio": "Sortino Ratio",
    "omega_ratio": "Omega Ratio",
    "payoff_ratio": "Payoff Ratio",
    "profit_factor": "Profit Factor",
    "kelly_criterion": "Kelly Criterion",
    "expected_return": "Expected Return",
    "avg_loss": "Avg Loss",
    "avg_return": "Avg Return",
    "avg_win": "Avg Win",
    "implied_volatility": "Implied Volatility",
    "information_ratio": "Information Ratio",
    "volatility": "Volatility",
    "win_loss_ratio": "Win/Loss Ratio",
    "win_rate": "Win Rate",
}

PERCENT_METRICS = {
    "expected_return",
    "avg_loss",
    "avg_return",
    "avg_win",
    "implied_volatility",
    "volatility",
    "win_rate",
}

METRIC_ORDER = [
    "sharpe_ratio",
    "sortino_ratio",
    "omega_ratio",
    "payoff_ratio",
    "profit_factor",
    "kelly_criterion",
    "expected_return",
    "avg_loss",
    "avg_return",
    "avg_win",
    "implied_volatility",
    "information_ratio",
    "volatility",
    "win_loss_ratio",
    "win_rate",
]


st.title("Performance")

portfolio_df = load_portfolio_daily()
if portfolio_df.empty:
    st.warning("No portfolio mart data available.")
    st.stop()

frame = portfolio_df.copy()
frame["d"] = pd.to_datetime(frame["d"])
frame = frame.sort_values("d")
frame["drawdown"] = frame["close_value_gbp"] / frame["close_value_gbp"].cummax() - 1.0
trading_metrics = compute_trading_metrics(frame["daily_return_flow_corrected"])

metric_rows: list[dict[str, str]] = []
for metric_name in METRIC_ORDER:
    value = trading_metrics.get(metric_name, np.nan)
    if pd.isna(value):
        display_value = "n/a"
    elif np.isposinf(value):
        display_value = "inf"
    elif np.isneginf(value):
        display_value = "-inf"
    elif metric_name in PERCENT_METRICS:
        display_value = f"{value * 100:.2f}%"
    else:
        display_value = f"{value:.4f}"
    metric_rows.append(
        {
            "Metric": METRIC_LABELS.get(metric_name, metric_name),
            "Value": display_value,
        }
    )

st.subheader("Trading Metrics")
st.caption("All metrics are computed from transfer-corrected daily returns.")
st.dataframe(pd.DataFrame(metric_rows), hide_index=True, use_container_width=True)

cum_fig = px.line(frame, x="d", y="cumulative_twr", title="Cumulative Flow-Corrected Return (TWR)")
cum_fig.update_yaxes(tickformat=".2%")
st.plotly_chart(cum_fig, use_container_width=True)

draw_fig = px.area(frame, x="d", y="drawdown", title="Portfolio Drawdown")
draw_fig.update_yaxes(tickformat=".2%")
st.plotly_chart(draw_fig, use_container_width=True)

daily_fig = px.bar(frame, x="d", y="daily_return_flow_corrected", title="Daily Flow-Corrected Return")
daily_fig.update_yaxes(tickformat=".2%")
st.plotly_chart(daily_fig, use_container_width=True)

hist = px.histogram(frame, x="daily_return_flow_corrected", nbins=50, title="Daily Return Distribution")
hist.update_xaxes(tickformat=".2%")
st.plotly_chart(hist, use_container_width=True)

st.dataframe(
    frame[
        [
            "d",
            "close_value_gbp",
            "external_flow_gbp",
            "daily_pnl_flow_corrected_gbp",
            "daily_return_flow_corrected",
            "cumulative_twr",
            "drawdown",
        ]
    ].tail(120),
    use_container_width=True,
)
