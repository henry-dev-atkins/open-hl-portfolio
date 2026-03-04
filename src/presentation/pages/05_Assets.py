from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.presentation.asset_series import build_yfinance_shaped_asset_series
from src.presentation.data_access import (
    list_asset_value_runs,
    load_price_history_for_ticker,
    load_asset_ticker_mapping,
    load_asset_value_checkpoints,
    load_unresolved_assets,
    load_unresolved_ticker_candidates,
)
from src.prices.asset_identity import canonicalize_asset_name


def _fmt_link(label: str, url: str | None) -> str:
    if url:
        return f"[{label}]({url})"
    return f"{label}: unavailable"


st.title("Assets")
st.caption("Per-asset valuation checkpoints with ticker-mapping validation links.")

run_dates = list_asset_value_runs()
if not run_dates:
    st.warning(
        "No asset valuation CSV found. Run the investment report parser first to generate "
        "`investment_reports_asset_values_<run-date>.csv`."
    )
    st.stop()

selected_run = st.selectbox("Report Run Date", options=run_dates, index=len(run_dates) - 1)
asset_df = load_asset_value_checkpoints(run_date=selected_run)
mapping_df = load_asset_ticker_mapping()
unresolved_assets_df = load_unresolved_assets(run_date=selected_run)
unresolved_candidates_df = load_unresolved_ticker_candidates(run_date=selected_run)

if asset_df.empty:
    st.warning(f"No asset valuation rows found for run date {selected_run}.")
    st.stop()

asset_df["account_name"] = asset_df["account_name"].astype(str).str.strip()
asset_df["asset_name"] = asset_df["asset_name"].astype(str).str.strip()

account_options = sorted(asset_df["account_name"].unique().tolist())
selected_account = st.selectbox("Account", options=account_options)

account_df = asset_df[asset_df["account_name"] == selected_account].copy()
if account_df.empty:
    st.info("No assets for selected account.")
    st.stop()

asset_options = sorted(account_df["asset_name"].unique().tolist())
selected_asset = st.selectbox("Asset", options=asset_options)
selected_asset_canonical = canonicalize_asset_name(selected_asset)

series = (
    account_df[account_df["asset_name"] == selected_asset]
    .sort_values("report_date")
    .drop_duplicates(subset=["report_date"], keep="last")
    .copy()
)
if series.empty:
    st.info("No checkpoints for selected asset.")
    st.stop()

series["period_change_gbp"] = series["value_gbp"].diff()
series["period_return"] = series["value_gbp"].pct_change()
series["cumulative_return"] = series["value_gbp"] / series["value_gbp"].iloc[0] - 1.0

latest = series.iloc[-1]
start = series.iloc[0]
change_gbp = float(latest["value_gbp"] - start["value_gbp"])
total_return = float(latest["cumulative_return"])

k1, k2, k3, k4 = st.columns(4)
k1.metric("Latest Value", f"GBP {latest['value_gbp']:,.2f}")
k2.metric("Start Value", f"GBP {start['value_gbp']:,.2f}")
k3.metric("Change", f"GBP {change_gbp:,.2f}")
k4.metric("Return", f"{total_return * 100:.2f}%")

mapping_row = pd.Series(dtype=object)
if not mapping_df.empty:
    matching = mapping_df[
        mapping_df["asset_name_canonical"].astype(str) == selected_asset_canonical
    ].copy()
    if not matching.empty:
        matching["resolved_sort"] = (matching["match_status"].astype(str) == "resolved").astype(int)
        matching["locked_sort"] = matching["is_locked"].fillna(False).astype(int)
        matching["confidence_sort"] = pd.to_numeric(matching["confidence_score"], errors="coerce").fillna(0.0)
        matching = matching.sort_values(
            ["resolved_sort", "locked_sort", "confidence_sort"],
            ascending=[False, False, False],
        )
        mapping_row = matching.iloc[0]

st.subheader("Validation Links")
if not mapping_row.empty:
    st.write(_fmt_link("Yahoo Quote", mapping_row.get("yf_quote_url")))
    st.write(_fmt_link("Yahoo History", mapping_row.get("yf_history_url")))
    st.write(_fmt_link("HL Security Page", mapping_row.get("hl_security_url")))
    st.write(_fmt_link("HL Search", mapping_row.get("hl_search_url")))

    mk1, mk2, mk3 = st.columns(3)
    mk1.metric("Ticker", str(mapping_row.get("ticker") or "n/a"))
    mk2.metric("Match Status", str(mapping_row.get("match_status") or "n/a"))
    conf = mapping_row.get("confidence_score")
    mk3.metric("Confidence", f"{float(conf):.2f}" if pd.notna(conf) else "n/a")
else:
    st.info("No ticker mapping row for this asset yet. Run `src.prices.resolve_asset_tickers` first.")

ticker = str(mapping_row.get("ticker") or "").strip() if not mapping_row.empty else ""
provider = str(mapping_row.get("provider") or "yfinance").strip() if not mapping_row.empty else "yfinance"
price_df = load_price_history_for_ticker(ticker=ticker, provider=provider) if ticker else pd.DataFrame()
modeled_series = build_yfinance_shaped_asset_series(checkpoints=series, price_history=price_df)

can_model = not modeled_series.empty
use_modeled = st.toggle(
    "Use yfinance-shaped daily interpolation",
    value=can_model,
    disabled=not can_model,
)

if use_modeled and can_model:
    value_fig = go.Figure()
    value_fig.add_trace(
        go.Scatter(
            x=modeled_series["d"],
            y=modeled_series["value_gbp"],
            mode="lines",
            name="Daily modeled value",
        )
    )
    value_fig.add_trace(
        go.Scatter(
            x=series["report_date"],
            y=series["value_gbp"],
            mode="markers",
            name="Report checkpoints",
            marker={"size": 7},
        )
    )
    value_fig.update_layout(
        title=f"{selected_asset}: Value Over Time",
        xaxis_title="date",
        yaxis_title="value_gbp",
    )
else:
    value_fig = px.line(
        series,
        x="report_date",
        y="value_gbp",
        markers=True,
        title=f"{selected_asset}: Value Over Time",
    )

st.plotly_chart(value_fig, use_container_width=True)

period_fig = px.bar(
    series.dropna(subset=["period_return"]),
    x="report_date",
    y="period_return",
    title=f"{selected_asset}: Period Return Between Reports",
)
period_fig.update_yaxes(tickformat=".2%")
st.plotly_chart(period_fig, use_container_width=True)

st.subheader("Account Asset Comparison")
latest_by_asset = (
    account_df.sort_values("report_date")
    .groupby("asset_name", as_index=False)
    .tail(1)
    .sort_values("value_gbp", ascending=False)
)
top_n = st.slider("Top Assets (by latest value)", min_value=3, max_value=20, value=8)
top_assets = latest_by_asset["asset_name"].head(top_n).tolist()
comparison = account_df[account_df["asset_name"].isin(top_assets)].copy()

comparison = comparison.sort_values(["asset_name", "report_date"])
comparison["value_index"] = comparison.groupby("asset_name")["value_gbp"].transform(
    lambda s: (s / s.iloc[0]) * 100.0 if s.iloc[0] else pd.NA
)
comparison_fig = px.line(
    comparison,
    x="report_date",
    y="value_index",
    color="asset_name",
    title=f"{selected_account}: Indexed Asset Performance (Start=100)",
)
st.plotly_chart(comparison_fig, use_container_width=True)

st.subheader("Unresolved Queue")
if not unresolved_assets_df.empty:
    unresolved_for_asset = unresolved_assets_df[
        unresolved_assets_df["asset_name_canonical"].astype(str) == selected_asset_canonical
    ]
    if unresolved_for_asset.empty:
        st.caption("No unresolved entry for selected asset.")
    else:
        st.dataframe(unresolved_for_asset, use_container_width=True)
else:
    st.caption("No unresolved asset CSV found for this run date.")

if not unresolved_candidates_df.empty:
    cands_for_asset = unresolved_candidates_df[
        unresolved_candidates_df["asset_name_canonical"].astype(str) == selected_asset_canonical
    ]
    if not cands_for_asset.empty:
        st.dataframe(cands_for_asset.head(20), use_container_width=True)

st.dataframe(
    series[["report_date", "value_gbp", "period_change_gbp", "period_return", "cumulative_return"]],
    use_container_width=True,
)
