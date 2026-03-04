from __future__ import annotations

import streamlit as st

from src.presentation.data_access import load_account_daily, load_portfolio_daily


st.set_page_config(
    page_title="HL Portfolio Analytics",
    page_icon="GBP",
    layout="wide",
)

st.title("HL Portfolio Analytics")
st.caption("Deposit-corrected daily analytics built from HL exports.")

portfolio_df = load_portfolio_daily()
account_df = load_account_daily()

if portfolio_df.empty and account_df.empty:
    st.warning("No mart data found. Run scripts/run_all.ps1 after placing raw exports in data/raw/<date>/")
else:
    last_date = None
    if not portfolio_df.empty:
        last_date = portfolio_df["d"].max()
    st.success(
        f"Data loaded. Portfolio rows: {len(portfolio_df):,}. "
        f"Account rows: {len(account_df):,}. "
        f"As-of: {last_date if last_date else 'n/a'}."
    )

st.markdown(
    """
Use the sidebar to open:
- `Overview`: portfolio-level KPIs and trend lines
- `Accounts`: per-account deep dive
- `Cashflows`: external/internal cash movements over time
- `Performance`: corrected daily return and drawdown views
- `Assets`: per-asset valuation/performance checkpoints from investment reports
- `Attribution`: account allocation vs security selection decomposition
- `Rolling & Concentration`: rolling returns, worst windows, recovery stats, concentration risk
- `Cost Drag`: fee/tax/idle-cash drag decomposition
"""
)
