from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

from src.common.config import load_yaml
from src.common.paths import PROJECT_ROOT, get_db_path


def _connect_read_only(db_path: str | Path | None = None) -> duckdb.DuckDBPyConnection | None:
    db_file = Path(get_db_path(str(db_path) if db_path is not None else None))
    if not db_file.exists():
        return None
    try:
        return duckdb.connect(str(db_file), read_only=True)
    except Exception:  # noqa: BLE001
        return None


@st.cache_resource
def get_connection(db_path: str | None = None) -> duckdb.DuckDBPyConnection | None:
    return _connect_read_only(db_path)


@st.cache_data(ttl=30)
def load_portfolio_daily(db_path: str | None = None) -> pd.DataFrame:
    conn = get_connection(db_path)
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute(
            """
            select *
            from mart_portfolio_daily
            order by d
            """
        ).df()
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


@st.cache_data(ttl=30)
def load_account_daily(db_path: str | None = None) -> pd.DataFrame:
    conn = get_connection(db_path)
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute(
            """
            select *
            from mart_account_daily
            order by account_id, d
            """
        ).df()
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


@st.cache_data(ttl=30)
def load_accounts(db_path: str | None = None) -> pd.DataFrame:
    conn = get_connection(db_path)
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute(
            """
            select *
            from dim_account
            order by account_id
            """
        ).df()
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


@st.cache_data(ttl=30)
def list_asset_value_runs() -> list[str]:
    staging_dir = PROJECT_ROOT / "data" / "staging"
    files = sorted(staging_dir.glob("investment_reports_asset_values_*.csv"))
    run_dates: list[str] = []
    prefix = "investment_reports_asset_values_"
    for file in files:
        stem = file.stem
        if not stem.startswith(prefix):
            continue
        run_dates.append(stem.replace(prefix, "", 1))
    return sorted(set(run_dates))


@st.cache_data(ttl=30)
def load_asset_value_checkpoints(run_date: str | None = None) -> pd.DataFrame:
    runs = list_asset_value_runs()
    if not runs:
        return pd.DataFrame()
    selected_run = run_date or runs[-1]

    csv_path = PROJECT_ROOT / "data" / "staging" / f"investment_reports_asset_values_{selected_run}.csv"
    if not csv_path.exists():
        return pd.DataFrame()

    frame = pd.read_csv(csv_path)
    if frame.empty:
        return frame

    frame["report_date"] = pd.to_datetime(
        frame["report_date"].astype(str),
        errors="coerce",
        dayfirst=True,
        format="mixed",
    )
    frame = frame[frame["report_date"].notna()].copy()
    frame["value_gbp"] = pd.to_numeric(frame["value_gbp"], errors="coerce")
    frame = frame[frame["value_gbp"].notna()].copy()
    frame["run_date"] = selected_run
    return frame


@st.cache_data(ttl=30)
def load_asset_ticker_mapping(db_path: str | None = None) -> pd.DataFrame:
    conn = get_connection(db_path)
    if conn is None:
        return pd.DataFrame()
    try:
        frame = conn.execute(
            """
            select
              m.asset_id,
              a.asset_name_canonical,
              m.provider,
              m.ticker,
              m.currency,
              m.confidence_score,
              m.match_status,
              m.mapping_source,
              m.is_locked,
              m.yf_quote_url,
              m.yf_history_url,
              m.hl_security_url,
              m.hl_search_url,
              m.hl_link_source
            from asset_ticker_mapping m
            left join dim_asset a using (asset_id)
            order by a.asset_name_canonical, m.provider
            """
        ).df()
    except Exception:  # noqa: BLE001
        return pd.DataFrame()
    return frame


@st.cache_data(ttl=30)
def load_unresolved_assets(run_date: str) -> pd.DataFrame:
    csv_path = PROJECT_ROOT / "data" / "staging" / f"unresolved_assets_{run_date}.csv"
    if not csv_path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(csv_path)
    return frame if not frame.empty else pd.DataFrame(columns=frame.columns)


@st.cache_data(ttl=30)
def load_unresolved_ticker_candidates(run_date: str) -> pd.DataFrame:
    csv_path = PROJECT_ROOT / "data" / "staging" / f"unresolved_ticker_candidates_{run_date}.csv"
    if not csv_path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(csv_path)
    return frame if not frame.empty else pd.DataFrame(columns=frame.columns)


@st.cache_data(ttl=30)
def load_price_history_for_ticker(
    ticker: str,
    provider: str = "yfinance",
    db_path: str | None = None,
) -> pd.DataFrame:
    ticker_value = str(ticker or "").strip()
    if not ticker_value:
        return pd.DataFrame(columns=["d", "px"])
    conn = get_connection(db_path)
    if conn is None:
        return pd.DataFrame(columns=["d", "px"])
    try:
        frame = conn.execute(
            """
            select
              d,
              coalesce(adj_close, close) as px
            from raw_market_price_daily
            where provider = ?
              and ticker = ?
            order by d
            """,
            [provider, ticker_value],
        ).df()
    except Exception:  # noqa: BLE001
        return pd.DataFrame(columns=["d", "px"])
    return frame if not frame.empty else pd.DataFrame(columns=["d", "px"])


@st.cache_data(ttl=30)
def load_asset_daily(db_path: str | None = None) -> pd.DataFrame:
    conn = get_connection(db_path)
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute(
            """
            select *
            from mart_asset_daily
            order by account_id, asset_id, d
            """
        ).df()
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


@st.cache_data(ttl=30)
def load_attribution_daily(db_path: str | None = None) -> pd.DataFrame:
    conn = get_connection(db_path)
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute(
            """
            select *
            from mart_attribution_daily
            order by d
            """
        ).df()
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


@st.cache_data(ttl=30)
def load_concentration_daily(db_path: str | None = None) -> pd.DataFrame:
    conn = get_connection(db_path)
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute(
            """
            select *
            from mart_concentration_daily
            order by scope_type, scope_id, d
            """
        ).df()
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


@st.cache_data(ttl=30)
def load_cost_drag_account_daily(db_path: str | None = None) -> pd.DataFrame:
    conn = get_connection(db_path)
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute(
            """
            select *
            from mart_cost_drag_account_daily
            order by account_id, d
            """
        ).df()
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


@st.cache_data(ttl=30)
def load_cost_drag_portfolio_daily(db_path: str | None = None) -> pd.DataFrame:
    conn = get_connection(db_path)
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute(
            """
            select *
            from mart_cost_drag_portfolio_daily
            order by d
            """
        ).df()
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


@st.cache_data(ttl=30)
def load_benchmark_daily_returns(db_path: str | None = None) -> pd.DataFrame:
    cfg = load_yaml(PROJECT_ROOT / "config" / "benchmark.yml")
    provider = str(cfg.get("provider") or "yfinance").strip()
    ticker = str(cfg.get("ticker") or "").strip()
    if not ticker:
        return pd.DataFrame(columns=["d", "benchmark_px", "benchmark_return"])
    conn = get_connection(db_path)
    if conn is None:
        return pd.DataFrame(columns=["d", "benchmark_px", "benchmark_return"])
    try:
        frame = conn.execute(
            """
            select d, coalesce(adj_close, close) as benchmark_px
            from raw_market_price_daily
            where provider = ?
              and ticker = ?
            order by d
            """,
            [provider, ticker],
        ).df()
    except Exception:  # noqa: BLE001
        return pd.DataFrame(columns=["d", "benchmark_px", "benchmark_return"])
    if frame.empty:
        return pd.DataFrame(columns=["d", "benchmark_px", "benchmark_return"])
    frame["d"] = pd.to_datetime(frame["d"], errors="coerce")
    frame["benchmark_px"] = pd.to_numeric(frame["benchmark_px"], errors="coerce")
    frame = frame[frame["d"].notna() & frame["benchmark_px"].notna()].copy()
    if frame.empty:
        return pd.DataFrame(columns=["d", "benchmark_px", "benchmark_return"])
    frame = frame.sort_values("d").reset_index(drop=True)
    frame["benchmark_return"] = frame["benchmark_px"].pct_change().fillna(0.0)
    frame["d"] = frame["d"].dt.date
    return frame[["d", "benchmark_px", "benchmark_return"]]
