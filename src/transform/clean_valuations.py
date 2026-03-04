from __future__ import annotations

import argparse

import pandas as pd

from src.common.db import connect_db, ensure_schema, fetch_latest_run_id
from src.common.paths import get_db_path
from src.transform.account_resolution import build_dim_account_df, load_account_rules, resolve_account_id


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean raw HL valuations into daily account values.")
    parser.add_argument("--db-path", default=None, help="DuckDB file path")
    parser.add_argument("--run-id", default=None, help="Import run id; defaults to latest")
    return parser.parse_args()


def clean_valuations(conn, run_id: str) -> pd.DataFrame:
    conn.execute("delete from stg_account_value_daily where source_run_id = ?", [run_id])

    raw_df = conn.execute(
        """
        select *
        from raw_valuations
        where run_id = ?
        """,
        [run_id],
    ).df()
    if raw_df.empty:
        return pd.DataFrame(columns=["account_id", "d", "close_value_gbp", "source_run_id"])

    account_rules = load_account_rules()
    raw_df["account_id_resolved"] = raw_df.apply(
        lambda r: resolve_account_id(r.get("account_id"), r.get("account_name_raw"), account_rules),
        axis=1,
    )

    stg_df = (
        raw_df.groupby(["account_id_resolved", "valuation_date"], as_index=False)["total_value_gbp"]
        .sum()
        .rename(
            columns={
                "account_id_resolved": "account_id",
                "valuation_date": "d",
                "total_value_gbp": "close_value_gbp",
            }
        )
    )
    stg_df["source_run_id"] = run_id
    stg_df = stg_df.sort_values(["account_id", "d"]).reset_index(drop=True)

    raw_accounts = raw_df[["account_id_resolved", "account_name_raw"]].rename(
        columns={"account_id_resolved": "account_id"}
    )
    dim_df = build_dim_account_df(raw_accounts, account_rules)

    if not stg_df.empty:
        conn.register("tmp_stg_value", stg_df)
        conn.execute("insert into stg_account_value_daily select * from tmp_stg_value")
        conn.unregister("tmp_stg_value")

    if not dim_df.empty:
        account_ids = dim_df["account_id"].astype(str).tolist()
        placeholders = ",".join(["?"] * len(account_ids))
        conn.execute(f"delete from dim_account where account_id in ({placeholders})", account_ids)  # noqa: S608
        conn.register("tmp_dim_account", dim_df)
        conn.execute("insert into dim_account select * from tmp_dim_account")
        conn.unregister("tmp_dim_account")

    return stg_df


def main() -> None:
    args = parse_args()
    db_path = get_db_path(args.db_path)
    conn = connect_db(db_path)
    ensure_schema(conn)

    run_id = args.run_id or fetch_latest_run_id(conn)
    if not run_id:
        raise ValueError("No import runs found. Run src.ingest.run_import first.")

    stg_df = clean_valuations(conn=conn, run_id=run_id)
    print(f"stg_account_value_daily rows={len(stg_df)} run_id={run_id}")


if __name__ == "__main__":
    main()
