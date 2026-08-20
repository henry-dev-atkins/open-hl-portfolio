import json

import pandas as pd

from src.common.db import connect_db, ensure_schema
from src.quality.reconciliation import build_quality_report


def test_quality_report_detects_issues(tmp_path) -> None:
    db_path = tmp_path / "quality_test.duckdb"
    conn = connect_db(db_path)
    ensure_schema(conn)

    run_id = "RUN1"
    conn.execute(
        """
        insert into raw_import_runs (run_id, imported_at, source_path, hl_export_date, notes)
        values (?, now(), 'tests', null, null)
        """,
        [run_id],
    )

    stg_tx = pd.DataFrame(
        [
            {
                "account_id": "ACC1",
                "event_date": pd.Timestamp("2026-01-01").date(),
                "txn_type": "other",
                "amount_gbp": 100.0,
                "flow_class": "other",
                "source_run_id": run_id,
                "source_file": "a.csv",
                "source_row_num": 1,
            },
            {
                "account_id": "ACC1",
                "event_date": pd.Timestamp("2026-01-01").date(),
                "txn_type": "other",
                "amount_gbp": 100.0,
                "flow_class": "other",
                "source_run_id": run_id,
                "source_file": "a.csv",
                "source_row_num": 2,
            },
        ]
    )
    conn.register("tmp_stg_tx", stg_tx)
    conn.execute("insert into stg_transactions select * from tmp_stg_tx")
    conn.unregister("tmp_stg_tx")

    stg_vals = pd.DataFrame(
        [
            {
                "account_id": "ACC1",
                "d": pd.Timestamp("2026-01-01").date(),
                "close_value_gbp": 1000.0,
                "source_run_id": run_id,
            },
            {
                "account_id": "ACC1",
                "d": pd.Timestamp("2026-01-20").date(),
                "close_value_gbp": 1100.0,
                "source_run_id": run_id,
            },
        ]
    )
    conn.register("tmp_stg_vals", stg_vals)
    conn.execute("insert into stg_account_value_daily select * from tmp_stg_vals")
    conn.unregister("tmp_stg_vals")

    mart = pd.DataFrame(
        [
            {
                "account_id": "ACC1",
                "d": pd.Timestamp("2026-01-01").date(),
                "close_value_gbp": 1000.0,
                "external_flow_gbp": 0.0,
                "internal_flow_gbp": 0.0,
                "net_deposited_external_to_date_gbp": 0.0,
                "daily_pnl_flow_corrected_gbp": 0.0,
                "daily_return_flow_corrected": 0.0,
                "cumulative_twr": 0.0,
                "cumulative_gain_vs_external_deposits_gbp": 1000.0,
            }
        ]
    )
    conn.register("tmp_mart", mart)
    conn.execute("insert into mart_account_daily select * from tmp_mart")
    conn.unregister("tmp_mart")

    report = build_quality_report(conn=conn, run_id=run_id)
    assert report["status"] == "warning"
    assert report["issue_count"] >= 2
    assert len(report["checks"]["duplicate_stg_transactions"]) == 1
    assert len(report["checks"]["unmapped_transaction_types"]) == 1
    assert len(report["checks"]["valuation_gaps_gt_7_days"]) == 1
    assert report["checks"]["duplicate_stg_transactions"][0]["event_date"] == "2026-01-01"
    json.dumps(report)


def test_quality_report_errors_on_asset_checkpoint_after_latest_account_value(tmp_path) -> None:
    db_path = tmp_path / "quality_missing_account_value.duckdb"
    conn = connect_db(db_path)
    ensure_schema(conn)

    run_id = "REPORTS_2026-08-19"
    conn.execute(
        """
        insert into stg_account_value_daily (account_id, d, close_value_gbp, source_run_id)
        values ('LISA', '2026-01-31', 55622.0, ?)
        """,
        [run_id],
    )
    conn.execute(
        """
        insert into stg_asset_checkpoint
          (account_id, asset_id, asset_name_canonical, d, value_gbp, source_run_id)
        values
          ('LISA', 'ASSET_A', 'blackrock frontiers investment tst ord 1p', '2026-04-30', 1405.0, ?)
        """,
        [run_id],
    )

    report = build_quality_report(conn=conn, run_id=run_id)

    assert report["status"] == "error"
    assert report["critical_issue_count"] == 1
    findings = report["checks"]["asset_checkpoints_after_latest_account_value"]
    assert findings[0]["account_id"] == "LISA"
    assert findings[0]["latest_account_value_date"] == "2026-01-31"
    json.dumps(report)


def test_quality_report_errors_on_suspicious_asset_heading_name(tmp_path) -> None:
    db_path = tmp_path / "quality_suspicious_asset.duckdb"
    conn = connect_db(db_path)
    ensure_schema(conn)

    run_id = "REPORTS_2026-08-19"
    conn.execute(
        """
        insert into stg_account_value_daily (account_id, d, close_value_gbp, source_run_id)
        values ('LISA', '2026-01-31', 55622.0, ?)
        """,
        [run_id],
    )
    conn.execute(
        """
        insert into stg_asset_checkpoint
          (account_id, asset_id, asset_name_canonical, d, value_gbp, source_run_id)
        values
          ('LISA', 'ASSET_BAD', 'lifetime isa 31 january 2026', '2026-01-31', 2170255.0, ?)
        """,
        [run_id],
    )

    report = build_quality_report(conn=conn, run_id=run_id)

    assert report["status"] == "error"
    assert report["critical_issue_count"] == 1
    findings = report["checks"]["suspicious_asset_heading_names"]
    assert findings[0]["asset_name_canonical"] == "lifetime isa 31 january 2026"
    json.dumps(report)
