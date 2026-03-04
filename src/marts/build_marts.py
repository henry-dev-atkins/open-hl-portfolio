from __future__ import annotations

import argparse

from src.common.db import connect_db, ensure_schema, fetch_latest_run_id
from src.common.paths import get_db_path
from src.marts.build_asset_daily import build_asset_daily_from_checkpoints, load_staged_checkpoints
from src.metrics.attribution import compute_attribution_daily
from src.metrics.concentration import compute_concentration_daily
from src.metrics.cost_drag import build_cost_drag_marts
from src.metrics.performance import compute_account_daily_metrics, compute_portfolio_daily_metrics
from src.transform.build_daily_flows import build_daily_flows
from src.transform.clean_holdings import clean_holdings
from src.transform.clean_transactions import clean_transactions
from src.transform.clean_valuations import clean_valuations


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build staging and marts for HL portfolio analytics.")
    parser.add_argument("--db-path", default=None, help="DuckDB file path")
    parser.add_argument("--run-id", default=None, help="Import run id; defaults to latest")
    return parser.parse_args()


def build_marts(conn, run_id: str) -> dict[str, int]:
    clean_transactions(conn=conn, run_id=run_id)
    clean_valuations(conn=conn, run_id=run_id)
    clean_holdings(conn=conn, run_id=run_id)
    build_daily_flows(conn=conn, run_id=run_id)

    value_df = conn.execute(
        """
        select account_id, d, close_value_gbp
        from stg_account_value_daily
        where source_run_id = ?
        order by account_id, d
        """,
        [run_id],
    ).df()

    flow_df = conn.execute(
        """
        select account_id, d, external_flow_gbp, internal_flow_gbp
        from stg_account_flow_daily
        where source_run_id = ?
        order by account_id, d
        """,
        [run_id],
    ).df()

    account_daily = compute_account_daily_metrics(value_df=value_df, flow_df=flow_df)
    portfolio_daily = compute_portfolio_daily_metrics(account_daily_df=account_daily)

    conn.execute("delete from mart_account_daily")
    if not account_daily.empty:
        conn.register("tmp_mart_account_daily", account_daily)
        conn.execute("insert into mart_account_daily select * from tmp_mart_account_daily")
        conn.unregister("tmp_mart_account_daily")

    conn.execute("delete from mart_portfolio_daily")
    if not portfolio_daily.empty:
        conn.register("tmp_mart_portfolio_daily", portfolio_daily)
        conn.execute("insert into mart_portfolio_daily select * from tmp_mart_portfolio_daily")
        conn.unregister("tmp_mart_portfolio_daily")

    checkpoint_df = load_staged_checkpoints(conn=conn, run_id=run_id)
    asset_daily = build_asset_daily_from_checkpoints(
        conn=conn,
        checkpoint_df=checkpoint_df,
        account_value_df=account_daily[["account_id", "d", "close_value_gbp"]],
        source_run_id=run_id,
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
        conn.register("tmp_mart_attribution_daily", attribution)
        conn.execute("insert into mart_attribution_daily select * from tmp_mart_attribution_daily")
        conn.unregister("tmp_mart_attribution_daily")

    concentration = compute_concentration_daily(asset_daily_df=asset_daily)
    conn.execute("delete from mart_concentration_daily")
    if not concentration.empty:
        concentration = concentration.copy()
        concentration["source_run_id"] = run_id
        conn.register("tmp_mart_concentration_daily", concentration)
        conn.execute("insert into mart_concentration_daily select * from tmp_mart_concentration_daily")
        conn.unregister("tmp_mart_concentration_daily")

    account_drag, portfolio_drag = build_cost_drag_marts(
        conn=conn,
        run_id=run_id,
        account_daily_df=account_daily,
    )

    return {
        "mart_account_daily": len(account_daily),
        "mart_portfolio_daily": len(portfolio_daily),
        "mart_asset_daily": len(asset_daily),
        "mart_attribution_daily": len(attribution),
        "mart_concentration_daily": len(concentration),
        "mart_cost_drag_account_daily": len(account_drag),
        "mart_cost_drag_portfolio_daily": len(portfolio_drag),
    }


def main() -> None:
    args = parse_args()
    db_path = get_db_path(args.db_path)
    conn = connect_db(db_path)
    ensure_schema(conn)

    run_id = args.run_id or fetch_latest_run_id(conn)
    if not run_id:
        raise ValueError("No import runs found. Run src.ingest.run_import first.")

    counts = build_marts(conn=conn, run_id=run_id)
    count_text = " ".join([f"{k}={v}" for k, v in counts.items()])
    print(f"Marts build complete run_id={run_id} {count_text}")


if __name__ == "__main__":
    main()
