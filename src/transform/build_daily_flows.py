from __future__ import annotations

import argparse

import pandas as pd

from src.common.db import connect_db, ensure_schema, fetch_latest_run_id
from src.common.paths import get_db_path


EXTERNAL_CLASSES = {"external_in", "external_out"}
INTERNAL_CLASSES = {"internal_in", "internal_out"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build daily external/internal flow table by account.")
    parser.add_argument("--db-path", default=None, help="DuckDB file path")
    parser.add_argument("--run-id", default=None, help="Import run id; defaults to latest")
    return parser.parse_args()


def build_daily_flows(conn, run_id: str) -> pd.DataFrame:
    conn.execute("delete from stg_account_flow_daily where source_run_id = ?", [run_id])

    tx_df = conn.execute(
        """
        select *
        from stg_transactions
        where source_run_id = ?
        """,
        [run_id],
    ).df()
    if tx_df.empty:
        return pd.DataFrame(columns=["account_id", "d", "external_flow_gbp", "internal_flow_gbp", "source_run_id"])

    tx_df["external_component"] = tx_df.apply(
        lambda r: float(r["amount_gbp"]) if r["flow_class"] in EXTERNAL_CLASSES else 0.0, axis=1
    )
    tx_df["internal_component"] = tx_df.apply(
        lambda r: float(r["amount_gbp"]) if r["flow_class"] in INTERNAL_CLASSES else 0.0, axis=1
    )

    flow_df = (
        tx_df.groupby(["account_id", "event_date"], as_index=False)[["external_component", "internal_component"]]
        .sum()
        .rename(
            columns={
                "event_date": "d",
                "external_component": "external_flow_gbp",
                "internal_component": "internal_flow_gbp",
            }
        )
    )
    flow_df["source_run_id"] = run_id
    flow_df = flow_df.sort_values(["account_id", "d"]).reset_index(drop=True)

    if not flow_df.empty:
        conn.register("tmp_stg_flow", flow_df)
        conn.execute("insert into stg_account_flow_daily select * from tmp_stg_flow")
        conn.unregister("tmp_stg_flow")

    return flow_df


def main() -> None:
    args = parse_args()
    db_path = get_db_path(args.db_path)
    conn = connect_db(db_path)
    ensure_schema(conn)

    run_id = args.run_id or fetch_latest_run_id(conn)
    if not run_id:
        raise ValueError("No import runs found. Run src.ingest.run_import first.")

    flow_df = build_daily_flows(conn=conn, run_id=run_id)
    print(f"stg_account_flow_daily rows={len(flow_df)} run_id={run_id}")


if __name__ == "__main__":
    main()
