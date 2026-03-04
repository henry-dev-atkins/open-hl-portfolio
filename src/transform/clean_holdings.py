from __future__ import annotations

import argparse

import pandas as pd

from src.common.db import connect_db, ensure_schema, fetch_latest_run_id
from src.common.paths import get_db_path
from src.prices.asset_identity import canonicalize_asset_name, make_asset_id
from src.transform.account_resolution import load_account_rules, resolve_account_id


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean holdings snapshots into staged asset checkpoints.")
    parser.add_argument("--db-path", default=None, help="DuckDB file path")
    parser.add_argument("--run-id", default=None, help="Import run id; defaults to latest")
    return parser.parse_args()


def _first_non_blank(series: pd.Series) -> str | None:
    for value in series.tolist():
        text = str(value or "").strip()
        if text:
            return text
    return None


def _merge_dim_asset(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    if existing_df.empty:
        merged = new_df.copy()
    else:
        merged = new_df.merge(
            existing_df[
                [
                    "asset_id",
                    "asset_name_canonical",
                    "isin",
                    "sedol",
                    "first_seen_date",
                    "last_seen_date",
                    "source_priority",
                ]
            ].rename(
                columns={
                    "asset_name_canonical": "asset_name_canonical_existing",
                    "isin": "isin_existing",
                    "sedol": "sedol_existing",
                    "first_seen_date": "first_seen_date_existing",
                    "last_seen_date": "last_seen_date_existing",
                    "source_priority": "source_priority_existing",
                }
            ),
            on="asset_id",
            how="left",
        )

        merged["first_seen_date"] = pd.to_datetime(merged["first_seen_date"], errors="coerce")
        merged["last_seen_date"] = pd.to_datetime(merged["last_seen_date"], errors="coerce")
        merged["first_seen_date_existing"] = pd.to_datetime(merged["first_seen_date_existing"], errors="coerce")
        merged["last_seen_date_existing"] = pd.to_datetime(merged["last_seen_date_existing"], errors="coerce")
        merged["first_seen_date"] = merged[["first_seen_date", "first_seen_date_existing"]].min(axis=1)
        merged["last_seen_date"] = merged[["last_seen_date", "last_seen_date_existing"]].max(axis=1)

        merged["asset_name_canonical"] = (
            merged["asset_name_canonical"].fillna("").astype(str).str.strip()
        )
        merged.loc[
            merged["asset_name_canonical"].eq(""),
            "asset_name_canonical",
        ] = merged["asset_name_canonical_existing"].fillna("").astype(str)

        merged["isin"] = merged["isin"].fillna("").astype(str).str.strip()
        merged.loc[merged["isin"].eq(""), "isin"] = merged["isin_existing"].fillna("").astype(str)
        merged["sedol"] = merged["sedol"].fillna("").astype(str).str.strip()
        merged.loc[merged["sedol"].eq(""), "sedol"] = merged["sedol_existing"].fillna("").astype(str)
        merged["source_priority"] = (
            merged["source_priority_existing"]
            .fillna(merged["source_priority"])
            .fillna("holdings_snapshot")
        )

    merged["first_seen_date"] = pd.to_datetime(merged["first_seen_date"], errors="coerce").dt.date
    merged["last_seen_date"] = pd.to_datetime(merged["last_seen_date"], errors="coerce").dt.date
    merged["isin"] = merged["isin"].replace({"": None, "nan": None, "None": None})
    merged["sedol"] = merged["sedol"].replace({"": None, "nan": None, "None": None})
    return merged[
        [
            "asset_id",
            "asset_name_canonical",
            "isin",
            "sedol",
            "first_seen_date",
            "last_seen_date",
            "source_priority",
        ]
    ]


def clean_holdings(conn, run_id: str) -> pd.DataFrame:
    conn.execute("delete from stg_asset_checkpoint where source_run_id = ?", [run_id])

    raw_df = conn.execute(
        """
        select *
        from raw_holdings_snapshot
        where run_id = ?
        """,
        [run_id],
    ).df()
    if raw_df.empty:
        return pd.DataFrame(
            columns=[
                "account_id",
                "asset_id",
                "asset_name_canonical",
                "d",
                "value_gbp",
                "isin",
                "sedol",
                "source_run_id",
                "source_file",
                "source_row_num",
            ]
        )

    account_rules = load_account_rules()
    raw_df["account_id_resolved"] = raw_df.apply(
        lambda r: resolve_account_id(r.get("account_id"), r.get("account_name_raw"), account_rules),
        axis=1,
    )
    raw_df["asset_name_canonical"] = raw_df["asset_name"].astype(str).apply(canonicalize_asset_name)
    raw_df = raw_df[raw_df["asset_name_canonical"].astype(str).str.len() > 0].copy()
    raw_df["asset_id"] = raw_df["asset_name_canonical"].apply(make_asset_id)
    raw_df["d"] = pd.to_datetime(raw_df["as_of_date"], errors="coerce").dt.date
    raw_df["market_value_gbp"] = pd.to_numeric(raw_df["market_value_gbp"], errors="coerce")
    raw_df = raw_df[raw_df["d"].notna() & raw_df["market_value_gbp"].notna()].copy()

    grouped = (
        raw_df.sort_values(["account_id_resolved", "asset_id", "d", "source_file", "row_num"])
        .groupby(["account_id_resolved", "asset_id", "asset_name_canonical", "d"], as_index=False)
        .agg(
            value_gbp=("market_value_gbp", "sum"),
            isin=("isin", _first_non_blank),
            sedol=("sedol", _first_non_blank),
            source_file=("source_file", "first"),
            source_row_num=("row_num", "min"),
        )
    )
    stg_df = grouped.rename(columns={"account_id_resolved": "account_id"})
    stg_df["source_run_id"] = run_id
    stg_df = stg_df[
        [
            "account_id",
            "asset_id",
            "asset_name_canonical",
            "d",
            "value_gbp",
            "isin",
            "sedol",
            "source_run_id",
            "source_file",
            "source_row_num",
        ]
    ].sort_values(["account_id", "asset_id", "d"])

    if not stg_df.empty:
        conn.register("tmp_stg_asset_checkpoint", stg_df)
        conn.execute("insert into stg_asset_checkpoint select * from tmp_stg_asset_checkpoint")
        conn.unregister("tmp_stg_asset_checkpoint")

    dim_new = (
        stg_df.groupby(["asset_id", "asset_name_canonical"], as_index=False)
        .agg(
            isin=("isin", _first_non_blank),
            sedol=("sedol", _first_non_blank),
            first_seen_date=("d", "min"),
            last_seen_date=("d", "max"),
        )
    )
    dim_new["source_priority"] = "holdings_snapshot"
    asset_ids = dim_new["asset_id"].astype(str).tolist()
    if asset_ids:
        placeholders = ",".join(["?"] * len(asset_ids))
        existing = conn.execute(
            f"select * from dim_asset where asset_id in ({placeholders})",  # noqa: S608
            asset_ids,
        ).df()
    else:
        existing = pd.DataFrame()
    dim_merged = _merge_dim_asset(existing_df=existing, new_df=dim_new)

    if asset_ids:
        placeholders = ",".join(["?"] * len(asset_ids))
        conn.execute(f"delete from dim_asset where asset_id in ({placeholders})", asset_ids)  # noqa: S608
    if not dim_merged.empty:
        conn.register("tmp_dim_asset_holdings", dim_merged)
        conn.execute("insert into dim_asset select * from tmp_dim_asset_holdings")
        conn.unregister("tmp_dim_asset_holdings")

    return stg_df.reset_index(drop=True)


def main() -> None:
    args = parse_args()
    db_path = get_db_path(args.db_path)
    conn = connect_db(db_path)
    ensure_schema(conn)

    run_id = args.run_id or fetch_latest_run_id(conn)
    if not run_id:
        raise ValueError("No import runs found. Run src.ingest.run_import first.")

    stg_df = clean_holdings(conn=conn, run_id=run_id)
    print(f"stg_asset_checkpoint rows={len(stg_df)} run_id={run_id}")


if __name__ == "__main__":
    main()
