from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from src.common.db import connect_db, ensure_schema
from src.common.paths import PROJECT_ROOT, get_db_path
from src.marts.build_asset_daily import (
    build_asset_daily_from_checkpoints,
    load_checkpoints_from_report_csv,
    stage_asset_checkpoints,
)
from src.metrics.attribution import compute_attribution_daily
from src.metrics.concentration import compute_concentration_daily
from src.metrics.cost_drag import build_cost_drag_marts, build_report_cash_proxy
from src.metrics.performance import compute_account_daily_metrics, compute_portfolio_daily_metrics
from src.marts.report_value_gap_fill import build_gap_filled_account_values
from src.transform.account_resolution import build_dim_account_df, load_account_rules, resolve_account_id


EXTERNAL_CLASSES = {"external_in", "external_out"}
INTERNAL_CLASSES = {"internal_in", "internal_out"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build marts from HL investment report PDF-derived datasets.")
    parser.add_argument("--run-date", required=True, help="Run date folder, e.g. 2026-02-13")
    parser.add_argument("--db-path", default=None, help="DuckDB path")
    parser.add_argument("--overview-csv", default=None, help="Path to investment_reports_overview CSV")
    parser.add_argument("--capital-tx-csv", default=None, help="Path to investment_reports_capital_txns CSV")
    parser.add_argument("--asset-values-csv", default=None, help="Path to investment_reports_asset_values CSV")
    parser.add_argument("--price-provider", default="yfinance", help="Price provider label in DB")
    parser.add_argument(
        "--disable-price-gap-fill",
        action="store_true",
        help="Disable yfinance-based valuation gap-fill and use report checkpoints only.",
    )
    return parser.parse_args()


def _coerce_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series.astype(str), dayfirst=True, errors="coerce", format="mixed").dt.date


def _normalize_flow_amount(amount: float, flow_class: str) -> float:
    if pd.isna(amount):
        return 0.0
    value = float(amount)
    if flow_class in {"external_in", "internal_in"}:
        return abs(value)
    if flow_class in {"external_out", "internal_out"}:
        return -abs(value)
    return value


def _resolve_accounts(df: pd.DataFrame, account_col: str, account_rules) -> pd.DataFrame:
    out = df.copy()
    out["account_name_raw"] = out[account_col].astype(str).str.strip()
    out["account_id"] = out["account_name_raw"].apply(
        lambda name: resolve_account_id(None, str(name), account_rules)
    )
    return out


def _load_overview_values(overview_csv: Path, account_rules) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not overview_csv.exists():
        raise FileNotFoundError(f"Overview CSV not found: {overview_csv}")
    overview = pd.read_csv(overview_csv)
    if overview.empty:
        raise ValueError(f"Overview CSV is empty: {overview_csv}")

    overview = overview.copy()
    overview["account_name"] = overview["account_name"].astype(str).str.strip()
    overview = overview[overview["account_name"].str.upper() != "TOTAL"].copy()
    overview["report_date"] = _coerce_date(overview["report_date"])
    overview["previous_period_date"] = _coerce_date(overview["previous_period_date"])
    overview["value_current"] = pd.to_numeric(overview["value_current"], errors="coerce")
    overview["value_previous"] = pd.to_numeric(overview["value_previous"], errors="coerce")
    overview = _resolve_accounts(overview, account_col="account_name", account_rules=account_rules)

    current_df = overview[["account_id", "report_date", "value_current", "report_label"]].rename(
        columns={"report_date": "d", "value_current": "close_value_gbp", "report_label": "source_label"}
    )
    current_df["asof_report_date"] = current_df["d"]
    current_df["source_priority"] = 2

    previous_df = overview[
        ["account_id", "previous_period_date", "value_previous", "report_label", "report_date"]
    ].rename(
        columns={
            "previous_period_date": "d",
            "value_previous": "close_value_gbp",
            "report_label": "source_label",
            "report_date": "asof_report_date",
        }
    )
    previous_df["source_priority"] = 1

    combined = pd.concat([current_df, previous_df], ignore_index=True)
    combined = combined[combined["d"].notna() & combined["close_value_gbp"].notna()].copy()
    combined = combined.sort_values(["account_id", "d", "asof_report_date", "source_priority"])
    combined = combined.drop_duplicates(subset=["account_id", "d"], keep="last")
    value_df = combined[["account_id", "d", "close_value_gbp"]].sort_values(["account_id", "d"]).reset_index(drop=True)

    account_dim_source = overview[["account_id", "account_name_raw"]].drop_duplicates().reset_index(drop=True)
    return value_df, account_dim_source


def _load_capital_transactions(
    capital_tx_csv: Path,
    account_rules,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not capital_tx_csv.exists():
        empty_flow = pd.DataFrame(columns=["account_id", "d", "external_flow_gbp", "internal_flow_gbp"])
        empty_stg = pd.DataFrame(
            columns=[
                "account_id",
                "event_date",
                "txn_type",
                "amount_gbp",
                "flow_class",
                "source_run_id",
                "source_file",
                "source_row_num",
            ]
        )
        empty_dim = pd.DataFrame(columns=["account_id", "account_name_raw"])
        empty_cash = pd.DataFrame(columns=["account_id", "d", "cash_balance_gbp", "source_quality"])
        return empty_flow, empty_stg, empty_dim, empty_cash

    tx = pd.read_csv(capital_tx_csv)
    if tx.empty:
        empty_flow = pd.DataFrame(columns=["account_id", "d", "external_flow_gbp", "internal_flow_gbp"])
        empty_stg = pd.DataFrame(
            columns=[
                "account_id",
                "event_date",
                "txn_type",
                "amount_gbp",
                "flow_class",
                "source_run_id",
                "source_file",
                "source_row_num",
            ]
        )
        empty_dim = pd.DataFrame(columns=["account_id", "account_name_raw"])
        empty_cash = pd.DataFrame(columns=["account_id", "d", "cash_balance_gbp", "source_quality"])
        return empty_flow, empty_stg, empty_dim, empty_cash

    tx = tx.copy()
    tx = _resolve_accounts(tx, account_col="account_name", account_rules=account_rules)
    tx["event_date"] = _coerce_date(tx["event_date"])
    tx["amount_gbp"] = pd.to_numeric(tx["amount_gbp"], errors="coerce").fillna(0.0)
    tx["flow_class"] = tx["flow_class"].fillna("other").astype(str)
    tx["txn_type"] = tx["txn_type"].fillna("other").astype(str)
    tx["subledger"] = tx["subledger"].fillna("unknown").astype(str) if "subledger" in tx.columns else "unknown"
    tx["balance_after_gbp"] = (
        pd.to_numeric(tx["balance_after_gbp"], errors="coerce")
        if "balance_after_gbp" in tx.columns
        else pd.Series([np.nan] * len(tx))
    )
    tx = tx[tx["event_date"].notna()].copy()
    tx["amount_gbp"] = tx.apply(lambda r: _normalize_flow_amount(r["amount_gbp"], r["flow_class"]), axis=1)

    tx["external_component"] = tx.apply(
        lambda r: float(r["amount_gbp"]) if r["flow_class"] in EXTERNAL_CLASSES else 0.0,
        axis=1,
    )
    tx["internal_component"] = tx.apply(
        lambda r: float(r["amount_gbp"]) if r["flow_class"] in INTERNAL_CLASSES else 0.0,
        axis=1,
    )
    flow_df = (
        tx.groupby(["account_id", "event_date"], as_index=False)[["external_component", "internal_component"]]
        .sum()
        .rename(
            columns={
                "event_date": "d",
                "external_component": "external_flow_gbp",
                "internal_component": "internal_flow_gbp",
            }
        )
        .sort_values(["account_id", "d"])
        .reset_index(drop=True)
    )

    stg_tx = tx[
        [
            "account_id",
            "event_date",
            "txn_type",
            "amount_gbp",
            "flow_class",
            "source_pdf",
            "source_row_num",
        ]
    ].rename(columns={"source_pdf": "source_file"})
    stg_tx["source_run_id"] = ""
    stg_tx = stg_tx[
        [
            "account_id",
            "event_date",
            "txn_type",
            "amount_gbp",
            "flow_class",
            "source_run_id",
            "source_file",
            "source_row_num",
        ]
    ]

    dim_source = tx[["account_id", "account_name_raw"]].drop_duplicates().reset_index(drop=True)
    cash_proxy = build_report_cash_proxy(
        tx[["account_id", "event_date", "balance_after_gbp", "subledger"]]
    )
    return flow_df, stg_tx, dim_source, cash_proxy


def build_report_marts(
    conn,
    run_date: str,
    overview_csv: Path,
    capital_tx_csv: Path,
    asset_values_csv: Path | None = None,
    price_provider: str = "yfinance",
    disable_price_gap_fill: bool = False,
) -> tuple[int, int, str]:
    account_rules = load_account_rules()
    run_id = f"REPORTS_{run_date}"
    run_date_dt = pd.to_datetime(run_date, errors="coerce")

    value_df, dim_overview = _load_overview_values(overview_csv=overview_csv, account_rules=account_rules)
    flow_df, stg_tx_df, dim_tx, report_cash_proxy_df = _load_capital_transactions(
        capital_tx_csv=capital_tx_csv,
        account_rules=account_rules,
    )
    if value_df.empty:
        raise ValueError("No account valuation checkpoints were parsed from investment reports.")

    value_df_for_mart = value_df.copy()
    if disable_price_gap_fill:
        print("Price gap-fill disabled; using report checkpoints only.")
    else:
        if asset_values_csv is None or not asset_values_csv.exists():
            print(
                "Price gap-fill skipped: asset values CSV missing. "
                "Using report checkpoints only."
            )
        else:
            resolved_count = int(
                conn.execute(
                    """
                    select count(*)
                    from asset_ticker_mapping
                    where provider = ?
                      and match_status = 'resolved'
                      and coalesce(trim(ticker), '') <> ''
                    """,
                    [price_provider],
                ).fetchone()[0]
            )
            price_count = int(
                conn.execute(
                    """
                    select count(*)
                    from raw_market_price_daily
                    where provider = ?
                    """,
                    [price_provider],
                ).fetchone()[0]
            )
            if resolved_count == 0 or price_count == 0:
                print(
                    "Price gap-fill skipped: missing resolved mappings and/or market prices. "
                    "Using report checkpoints only."
                )
            else:
                gap_df, diagnostics = build_gap_filled_account_values(
                    conn=conn,
                    value_df=value_df,
                    flow_df=flow_df[["account_id", "d", "external_flow_gbp", "internal_flow_gbp"]],
                    asset_values_csv=asset_values_csv,
                    provider=price_provider,
                )
                if gap_df.empty:
                    print("Price gap-fill produced no rows; using report checkpoints only.")
                else:
                    value_df_for_mart = gap_df
                    print(
                        "Price gap-fill applied "
                        + " ".join([f"{k}={v}" for k, v in diagnostics.items()])
                    )

    dim_source = pd.concat([dim_overview, dim_tx], ignore_index=True).drop_duplicates()
    dim_df = build_dim_account_df(dim_source, account_rules)

    conn.execute("delete from raw_import_runs where run_id = ?", [run_id])
    conn.execute(
        """
        insert into raw_import_runs (run_id, imported_at, source_path, hl_export_date, notes)
        values (?, ?, ?, ?, ?)
        """,
        [
            run_id,
            datetime.now(UTC),
            str(overview_csv.resolve()),
            run_date_dt.date() if pd.notna(run_date_dt) else None,
            "Generated from HL investment report PDFs",
        ],
    )

    stg_value = value_df_for_mart.copy()
    stg_value["source_run_id"] = run_id
    conn.execute("delete from stg_account_value_daily where source_run_id = ?", [run_id])
    conn.register("tmp_report_value", stg_value)
    conn.execute("insert into stg_account_value_daily select * from tmp_report_value")
    conn.unregister("tmp_report_value")

    stg_flow = flow_df.copy()
    stg_flow["source_run_id"] = run_id
    conn.execute("delete from stg_account_flow_daily where source_run_id = ?", [run_id])
    if not stg_flow.empty:
        conn.register("tmp_report_flow", stg_flow)
        conn.execute("insert into stg_account_flow_daily select * from tmp_report_flow")
        conn.unregister("tmp_report_flow")

    conn.execute("delete from stg_transactions where source_run_id = ?", [run_id])
    if not stg_tx_df.empty:
        stg_tx_df = stg_tx_df.copy()
        stg_tx_df["source_run_id"] = run_id
        conn.register("tmp_report_stg_tx", stg_tx_df)
        conn.execute("insert into stg_transactions select * from tmp_report_stg_tx")
        conn.unregister("tmp_report_stg_tx")

    if not dim_df.empty:
        account_ids = dim_df["account_id"].astype(str).tolist()
        placeholders = ",".join(["?"] * len(account_ids))
        conn.execute(f"delete from dim_account where account_id in ({placeholders})", account_ids)  # noqa: S608
        conn.register("tmp_report_dim", dim_df)
        conn.execute("insert into dim_account select * from tmp_report_dim")
        conn.unregister("tmp_report_dim")

    account_daily = compute_account_daily_metrics(
        value_df=value_df_for_mart,
        flow_df=flow_df[["account_id", "d", "external_flow_gbp", "internal_flow_gbp"]],
    )
    portfolio_daily = compute_portfolio_daily_metrics(account_daily_df=account_daily)

    conn.execute("delete from mart_account_daily")
    if not account_daily.empty:
        conn.register("tmp_report_mart_account", account_daily)
        conn.execute("insert into mart_account_daily select * from tmp_report_mart_account")
        conn.unregister("tmp_report_mart_account")

    conn.execute("delete from mart_portfolio_daily")
    if not portfolio_daily.empty:
        conn.register("tmp_report_mart_portfolio", portfolio_daily)
        conn.execute("insert into mart_portfolio_daily select * from tmp_report_mart_portfolio")
        conn.unregister("tmp_report_mart_portfolio")

    checkpoint_df = pd.DataFrame()
    if asset_values_csv is not None and asset_values_csv.exists():
        checkpoint_df = load_checkpoints_from_report_csv(asset_values_csv=asset_values_csv, run_id=run_id)
        if not checkpoint_df.empty:
            stage_asset_checkpoints(conn=conn, checkpoint_df=checkpoint_df, run_id=run_id)

    asset_daily = build_asset_daily_from_checkpoints(
        conn=conn,
        checkpoint_df=checkpoint_df,
        account_value_df=account_daily[["account_id", "d", "close_value_gbp"]],
        source_run_id=run_id,
        price_provider=price_provider,
    )

    attribution = compute_attribution_daily(
        asset_daily_df=asset_daily,
        account_daily_df=account_daily,
        portfolio_daily_df=portfolio_daily,
    )
    conn.execute("delete from mart_attribution_daily")
    if not attribution.empty:
        attribution = attribution.copy()
        attribution["source_run_id"] = run_id
        conn.register("tmp_report_mart_attribution", attribution)
        conn.execute("insert into mart_attribution_daily select * from tmp_report_mart_attribution")
        conn.unregister("tmp_report_mart_attribution")

    concentration = compute_concentration_daily(asset_daily_df=asset_daily)
    conn.execute("delete from mart_concentration_daily")
    if not concentration.empty:
        concentration = concentration.copy()
        concentration["source_run_id"] = run_id
        conn.register("tmp_report_mart_concentration", concentration)
        conn.execute("insert into mart_concentration_daily select * from tmp_report_mart_concentration")
        conn.unregister("tmp_report_mart_concentration")

    build_cost_drag_marts(
        conn=conn,
        run_id=run_id,
        account_daily_df=account_daily,
        report_cash_proxy_df=report_cash_proxy_df,
    )

    return len(account_daily), len(portfolio_daily), run_id


def main() -> None:
    args = parse_args()
    db_path = get_db_path(args.db_path)
    overview_csv = (
        Path(args.overview_csv)
        if args.overview_csv
        else (PROJECT_ROOT / "data" / "staging" / f"investment_reports_overview_{args.run_date}.csv")
    )
    capital_tx_csv = (
        Path(args.capital_tx_csv)
        if args.capital_tx_csv
        else (PROJECT_ROOT / "data" / "staging" / f"investment_reports_capital_txns_{args.run_date}.csv")
    )
    asset_values_csv = (
        Path(args.asset_values_csv)
        if args.asset_values_csv
        else (PROJECT_ROOT / "data" / "staging" / f"investment_reports_asset_values_{args.run_date}.csv")
    )

    conn = connect_db(db_path)
    ensure_schema(conn)

    account_rows, portfolio_rows, run_id = build_report_marts(
        conn=conn,
        run_date=args.run_date,
        overview_csv=overview_csv,
        capital_tx_csv=capital_tx_csv,
        asset_values_csv=asset_values_csv,
        price_provider=args.price_provider,
        disable_price_gap_fill=args.disable_price_gap_fill,
    )
    print(
        f"Report marts build complete run_id={run_id} "
        f"mart_account_daily={account_rows} mart_portfolio_daily={portfolio_rows}"
    )


if __name__ == "__main__":
    main()
