from __future__ import annotations

import argparse
from typing import Any

import pandas as pd

from src.common.config import load_yaml
from src.common.db import connect_db, ensure_schema, fetch_latest_run_id
from src.common.paths import get_db_path
from src.transform.account_resolution import build_dim_account_df, load_account_rules, resolve_account_id


POSITIVE_FLOW_CLASSES = {"external_in", "internal_in", "dividend", "cash_interest"}
NEGATIVE_FLOW_CLASSES = {"external_out", "internal_out", "fee", "tax"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean and classify raw HL transactions.")
    parser.add_argument("--db-path", default=None, help="DuckDB file path")
    parser.add_argument("--run-id", default=None, help="Import run id; defaults to latest")
    return parser.parse_args()


def classify_transaction(row: pd.Series, rules_config: dict[str, Any]) -> tuple[str, str]:
    text = f"{row.get('txn_type_raw', '')} {row.get('description_raw', '')}".lower()
    for rule in rules_config.get("rules", []):
        needles = [str(x).lower() for x in rule.get("match_any", [])]
        if any(n in text for n in needles):
            return str(rule.get("txn_type", "other")), str(rule.get("flow_class", "other"))

    default_cfg = rules_config.get("default", {})
    return str(default_cfg.get("txn_type", "other")), str(default_cfg.get("flow_class", "other"))


def normalize_amount(amount: float | int | None, flow_class: str) -> float:
    if pd.isna(amount):
        return 0.0
    amount_f = float(amount)
    if flow_class in POSITIVE_FLOW_CLASSES:
        return abs(amount_f)
    if flow_class in NEGATIVE_FLOW_CLASSES:
        return -abs(amount_f)
    return amount_f


def clean_transactions(conn, run_id: str) -> pd.DataFrame:
    conn.execute("delete from stg_transactions where source_run_id = ?", [run_id])

    raw_df = conn.execute(
        """
        select *
        from raw_transactions
        where run_id = ?
        """,
        [run_id],
    ).df()
    if raw_df.empty:
        return pd.DataFrame(
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

    account_rules = load_account_rules()
    txn_cfg = load_yaml("config/txn_type_map.yml")

    raw_df["account_id_resolved"] = raw_df.apply(
        lambda r: resolve_account_id(r.get("account_id"), r.get("account_name_raw"), account_rules),
        axis=1,
    )
    raw_df["event_date"] = raw_df["settle_date"].fillna(raw_df["trade_date"])
    classified = raw_df.apply(lambda r: classify_transaction(r, txn_cfg), axis=1)
    raw_df["txn_type"] = classified.str[0]
    raw_df["flow_class"] = classified.str[1]
    raw_df["amount_norm"] = raw_df.apply(lambda r: normalize_amount(r.get("amount_gbp"), r["flow_class"]), axis=1)

    stg_df = raw_df[
        [
            "account_id_resolved",
            "event_date",
            "txn_type",
            "amount_norm",
            "flow_class",
            "run_id",
            "source_file",
            "row_num",
        ]
    ].rename(
        columns={
            "account_id_resolved": "account_id",
            "amount_norm": "amount_gbp",
            "run_id": "source_run_id",
            "row_num": "source_row_num",
        }
    )
    stg_df = stg_df[stg_df["event_date"].notna()].copy()
    stg_df = stg_df.sort_values(["account_id", "event_date", "source_file", "source_row_num"]).reset_index(drop=True)

    raw_accounts = raw_df[["account_id_resolved", "account_name_raw"]].rename(
        columns={"account_id_resolved": "account_id"}
    )
    dim_df = build_dim_account_df(raw_accounts, account_rules)

    if not stg_df.empty:
        conn.register("tmp_stg_transactions", stg_df)
        conn.execute("insert into stg_transactions select * from tmp_stg_transactions")
        conn.unregister("tmp_stg_transactions")

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

    stg_df = clean_transactions(conn=conn, run_id=run_id)
    print(f"stg_transactions rows={len(stg_df)} run_id={run_id}")


if __name__ == "__main__":
    main()
