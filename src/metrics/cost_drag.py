from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from src.common.config import load_yaml
from src.common.paths import PROJECT_ROOT
from src.transform.account_resolution import load_account_rules, resolve_account_id


def _to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def _load_cost_config(path: str | Path | None = None) -> tuple[float, int]:
    cfg_path = Path(path) if path else (PROJECT_ROOT / "config" / "cost_drag.yml")
    cfg = load_yaml(cfg_path)
    annual = float(cfg.get("cash_benchmark_rate_annual", 0.0))
    periods = int(cfg.get("periods_per_year", 365))
    periods = max(periods, 1)
    return annual, periods


def build_stg_account_cost_daily(conn, run_id: str) -> pd.DataFrame:
    tx = conn.execute(
        """
        select account_id, event_date, txn_type, flow_class, amount_gbp
        from stg_transactions
        where source_run_id = ?
        """,
        [run_id],
    ).df()
    if tx.empty:
        out = pd.DataFrame(columns=["account_id", "d", "fee_gbp", "tax_gbp", "cash_interest_gbp", "source_run_id"])
        conn.execute("delete from stg_account_cost_daily where source_run_id = ?", [run_id])
        return out

    tx = tx.copy()
    tx["event_date"] = _to_date(tx["event_date"])
    tx["amount_gbp"] = pd.to_numeric(tx["amount_gbp"], errors="coerce").fillna(0.0)
    tx["txn_type"] = tx["txn_type"].astype(str).str.lower()
    tx["flow_class"] = tx["flow_class"].astype(str).str.lower()
    tx = tx[tx["event_date"].notna()].copy()

    tx["fee_component"] = np.where(tx["flow_class"].eq("fee"), tx["amount_gbp"], 0.0)
    tx["tax_component"] = np.where(
        tx["flow_class"].eq("tax") | tx["txn_type"].eq("tax"),
        tx["amount_gbp"],
        0.0,
    )
    tx["cash_interest_component"] = np.where(
        tx["txn_type"].eq("interest") | tx["flow_class"].eq("cash_interest"),
        tx["amount_gbp"],
        0.0,
    )
    out = (
        tx.groupby(["account_id", "event_date"], as_index=False)[
            ["fee_component", "tax_component", "cash_interest_component"]
        ]
        .sum()
        .rename(
            columns={
                "event_date": "d",
                "fee_component": "fee_gbp",
                "tax_component": "tax_gbp",
                "cash_interest_component": "cash_interest_gbp",
            }
        )
    )
    out["source_run_id"] = run_id
    out = out.sort_values(["account_id", "d"]).reset_index(drop=True)

    conn.execute("delete from stg_account_cost_daily where source_run_id = ?", [run_id])
    if not out.empty:
        conn.register("tmp_stg_account_cost_daily", out)
        conn.execute("insert into stg_account_cost_daily select * from tmp_stg_account_cost_daily")
        conn.unregister("tmp_stg_account_cost_daily")
    return out


def _load_cash_from_raw_valuations(conn, run_id: str) -> pd.DataFrame:
    raw = conn.execute(
        """
        select account_id, account_name_raw, valuation_date, cash_value_gbp
        from raw_valuations
        where run_id = ?
        """,
        [run_id],
    ).df()
    if raw.empty:
        return pd.DataFrame(columns=["account_id", "d", "cash_balance_gbp", "source_quality"])

    account_rules = load_account_rules()
    raw = raw.copy()
    raw["account_id"] = raw.apply(
        lambda r: resolve_account_id(r.get("account_id"), r.get("account_name_raw"), account_rules),
        axis=1,
    )
    raw["d"] = _to_date(raw["valuation_date"])
    raw["cash_value_gbp"] = pd.to_numeric(raw["cash_value_gbp"], errors="coerce")
    raw = raw[raw["account_id"].notna() & raw["d"].notna() & raw["cash_value_gbp"].notna()].copy()
    if raw.empty:
        return pd.DataFrame(columns=["account_id", "d", "cash_balance_gbp", "source_quality"])
    out = (
        raw.groupby(["account_id", "d"], as_index=False)["cash_value_gbp"]
        .sum()
        .rename(columns={"cash_value_gbp": "cash_balance_gbp"})
    )
    out["source_quality"] = "observed"
    return out


def build_report_cash_proxy(capital_tx_df: pd.DataFrame) -> pd.DataFrame:
    if capital_tx_df.empty:
        return pd.DataFrame(columns=["account_id", "d", "cash_balance_gbp", "source_quality"])
    df = capital_tx_df.copy()
    if "account_id" not in df.columns:
        account_rules = load_account_rules()
        df["account_id"] = df["account_name"].apply(lambda n: resolve_account_id(None, str(n), account_rules))
    df["event_date"] = _to_date(df["event_date"])
    df["balance_after_gbp"] = pd.to_numeric(df["balance_after_gbp"], errors="coerce")
    df["subledger"] = df.get("subledger", "unknown").astype(str).str.lower()
    df = df[df["account_id"].notna() & df["event_date"].notna() & df["balance_after_gbp"].notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=["account_id", "d", "cash_balance_gbp", "source_quality"])

    ledger_last = (
        df.sort_values(["account_id", "subledger", "event_date"])
        .groupby(["account_id", "subledger", "event_date"], as_index=False)
        .tail(1)
    )
    out = (
        ledger_last.groupby(["account_id", "event_date"], as_index=False)["balance_after_gbp"]
        .sum()
        .rename(columns={"event_date": "d", "balance_after_gbp": "cash_balance_gbp"})
    )
    out["source_quality"] = "proxy"
    return out


def build_stg_account_cash_daily(
    conn,
    run_id: str,
    report_cash_proxy_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    observed = _load_cash_from_raw_valuations(conn=conn, run_id=run_id)
    proxy = report_cash_proxy_df.copy() if report_cash_proxy_df is not None else pd.DataFrame()
    if not proxy.empty:
        proxy["d"] = _to_date(proxy["d"])
        proxy["cash_balance_gbp"] = pd.to_numeric(proxy["cash_balance_gbp"], errors="coerce")
        proxy = proxy[proxy["account_id"].notna() & proxy["d"].notna() & proxy["cash_balance_gbp"].notna()].copy()

    if observed.empty and proxy.empty:
        out = pd.DataFrame(columns=["account_id", "d", "cash_balance_gbp", "source_quality", "source_run_id"])
    elif observed.empty:
        out = proxy.copy()
    elif proxy.empty:
        out = observed.copy()
    else:
        out = observed.merge(
            proxy.rename(
                columns={
                    "cash_balance_gbp": "cash_balance_gbp_proxy",
                    "source_quality": "source_quality_proxy",
                }
            ),
            on=["account_id", "d"],
            how="outer",
        )
        out["cash_balance_gbp"] = out["cash_balance_gbp"].fillna(out["cash_balance_gbp_proxy"])
        out["source_quality"] = out["source_quality"].fillna(out["source_quality_proxy"]).fillna("proxy")
        out = out[["account_id", "d", "cash_balance_gbp", "source_quality"]]

    if out.empty:
        conn.execute("delete from stg_account_cash_daily where source_run_id = ?", [run_id])
        return pd.DataFrame(columns=["account_id", "d", "cash_balance_gbp", "source_quality", "source_run_id"])

    out["source_run_id"] = run_id
    out = out.sort_values(["account_id", "d"]).reset_index(drop=True)
    conn.execute("delete from stg_account_cash_daily where source_run_id = ?", [run_id])
    conn.register("tmp_stg_account_cash_daily", out)
    conn.execute("insert into stg_account_cash_daily select * from tmp_stg_account_cash_daily")
    conn.unregister("tmp_stg_account_cash_daily")
    return out


def compute_cost_drag_tables(
    account_daily_df: pd.DataFrame,
    account_cost_df: pd.DataFrame,
    account_cash_df: pd.DataFrame,
    *,
    annual_cash_rate: float,
    periods_per_year: int,
    source_run_id: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if account_daily_df.empty:
        return (
            pd.DataFrame(
                columns=[
                    "account_id",
                    "d",
                    "fee_drag_gbp",
                    "tax_drag_gbp",
                    "idle_cash_drag_gbp",
                    "total_drag_gbp",
                    "cash_balance_prev_gbp",
                    "cash_benchmark_daily_rate",
                    "cash_source_quality",
                    "source_run_id",
                ]
            ),
            pd.DataFrame(
                columns=[
                    "d",
                    "fee_drag_gbp",
                    "tax_drag_gbp",
                    "idle_cash_drag_gbp",
                    "total_drag_gbp",
                    "cash_balance_prev_gbp",
                    "cash_benchmark_daily_rate",
                    "source_run_id",
                ]
            ),
        )

    base = account_daily_df[["account_id", "d"]].copy()
    base["d"] = _to_date(base["d"])
    costs = account_cost_df.copy()
    costs["d"] = _to_date(costs["d"])
    for col in ("fee_gbp", "tax_gbp", "cash_interest_gbp"):
        if col in costs.columns:
            costs[col] = pd.to_numeric(costs[col], errors="coerce").fillna(0.0)
    cash = account_cash_df.copy()
    cash["d"] = _to_date(cash["d"])
    cash["cash_balance_gbp"] = pd.to_numeric(cash.get("cash_balance_gbp"), errors="coerce")

    base = base.merge(costs[["account_id", "d", "fee_gbp", "tax_gbp", "cash_interest_gbp"]], on=["account_id", "d"], how="left")
    base = base.merge(cash[["account_id", "d", "cash_balance_gbp", "source_quality"]], on=["account_id", "d"], how="left")
    base[["fee_gbp", "tax_gbp", "cash_interest_gbp"]] = base[["fee_gbp", "tax_gbp", "cash_interest_gbp"]].fillna(0.0)
    base["cash_balance_gbp"] = base["cash_balance_gbp"].fillna(0.0)
    base = base.sort_values(["account_id", "d"]).reset_index(drop=True)
    base["cash_balance_prev_gbp"] = base.groupby("account_id")["cash_balance_gbp"].shift(1).fillna(0.0)

    daily_rate = float((1.0 + annual_cash_rate) ** (1.0 / max(int(periods_per_year), 1)) - 1.0)
    base["fee_drag_gbp"] = np.abs(np.minimum(base["fee_gbp"], 0.0))
    base["tax_drag_gbp"] = np.abs(np.minimum(base["tax_gbp"], 0.0))
    base["cash_opportunity_gbp"] = np.maximum(base["cash_balance_prev_gbp"], 0.0) * daily_rate
    base["cash_interest_credit_gbp"] = np.maximum(base["cash_interest_gbp"], 0.0)
    base["idle_cash_drag_gbp"] = np.maximum(base["cash_opportunity_gbp"] - base["cash_interest_credit_gbp"], 0.0)
    base["total_drag_gbp"] = base["fee_drag_gbp"] + base["tax_drag_gbp"] + base["idle_cash_drag_gbp"]
    base["cash_benchmark_daily_rate"] = daily_rate
    base["cash_source_quality"] = base["source_quality"].fillna("unknown")
    base["source_run_id"] = source_run_id

    account_out = base[
        [
            "account_id",
            "d",
            "fee_drag_gbp",
            "tax_drag_gbp",
            "idle_cash_drag_gbp",
            "total_drag_gbp",
            "cash_balance_prev_gbp",
            "cash_benchmark_daily_rate",
            "cash_source_quality",
            "source_run_id",
        ]
    ].copy()
    portfolio_out = (
        account_out.groupby("d", as_index=False)[
            ["fee_drag_gbp", "tax_drag_gbp", "idle_cash_drag_gbp", "total_drag_gbp", "cash_balance_prev_gbp"]
        ]
        .sum()
        .sort_values("d")
    )
    portfolio_out["cash_benchmark_daily_rate"] = daily_rate
    portfolio_out["source_run_id"] = source_run_id
    return account_out, portfolio_out


def build_cost_drag_marts(
    conn,
    run_id: str,
    account_daily_df: pd.DataFrame,
    report_cash_proxy_df: pd.DataFrame | None = None,
    config_path: str | Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    annual_rate, periods = _load_cost_config(path=config_path)
    cost_df = build_stg_account_cost_daily(conn=conn, run_id=run_id)
    cash_df = build_stg_account_cash_daily(conn=conn, run_id=run_id, report_cash_proxy_df=report_cash_proxy_df)
    account_out, portfolio_out = compute_cost_drag_tables(
        account_daily_df=account_daily_df,
        account_cost_df=cost_df,
        account_cash_df=cash_df,
        annual_cash_rate=annual_rate,
        periods_per_year=periods,
        source_run_id=run_id,
    )
    conn.execute("delete from mart_cost_drag_account_daily")
    if not account_out.empty:
        conn.register("tmp_mart_cost_drag_account_daily", account_out)
        conn.execute("insert into mart_cost_drag_account_daily select * from tmp_mart_cost_drag_account_daily")
        conn.unregister("tmp_mart_cost_drag_account_daily")

    conn.execute("delete from mart_cost_drag_portfolio_daily")
    if not portfolio_out.empty:
        conn.register("tmp_mart_cost_drag_portfolio_daily", portfolio_out)
        conn.execute("insert into mart_cost_drag_portfolio_daily select * from tmp_mart_cost_drag_portfolio_daily")
        conn.unregister("tmp_mart_cost_drag_portfolio_daily")

    return account_out, portfolio_out
