from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.common.db import connect_db, ensure_schema, fetch_latest_run_id
from src.common.paths import PROJECT_ROOT, get_db_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run data quality checks for HL analytics pipeline.")
    parser.add_argument("--db-path", default=None, help="DuckDB file path")
    parser.add_argument("--run-id", default=None, help="Import run id; defaults to latest")
    parser.add_argument("--output-json", default=None, help="Optional output JSON path")
    return parser.parse_args()


def _valuation_gap_findings(values_df: pd.DataFrame) -> list[dict[str, object]]:
    findings: list[dict[str, object]] = []
    if values_df.empty:
        return findings

    frame = values_df.copy()
    frame["d"] = pd.to_datetime(frame["d"])
    frame = frame.sort_values(["account_id", "d"])
    frame["prev_d"] = frame.groupby("account_id")["d"].shift(1)
    frame["gap_days"] = (frame["d"] - frame["prev_d"]).dt.days
    gap_rows = frame[frame["gap_days"] > 7]
    for _, row in gap_rows.iterrows():
        findings.append(
            {
                "account_id": row["account_id"],
                "date": row["d"].date().isoformat(),
                "prev_date": row["prev_d"].date().isoformat(),
                "gap_days": int(row["gap_days"]),
            }
        )
    return findings


def build_quality_report(conn, run_id: str) -> dict[str, object]:
    duplicates = conn.execute(
        """
        select account_id, event_date, txn_type, amount_gbp, count(*) as row_count
        from stg_transactions
        where source_run_id = ?
        group by account_id, event_date, txn_type, amount_gbp
        having count(*) > 1
        order by row_count desc
        """,
        [run_id],
    ).df()

    unmapped = conn.execute(
        """
        select txn_type, count(*) as row_count
        from stg_transactions
        where source_run_id = ?
          and flow_class = 'other'
        group by txn_type
        order by row_count desc
        """,
        [run_id],
    ).df()

    values_df = conn.execute(
        """
        select account_id, d, close_value_gbp
        from stg_account_value_daily
        where source_run_id = ?
        order by account_id, d
        """,
        [run_id],
    ).df()
    valuation_gaps = _valuation_gap_findings(values_df)

    mart_consistency = conn.execute(
        """
        select count(*) as bad_rows
        from mart_account_daily
        where abs(
          cumulative_gain_vs_external_deposits_gbp
          - (close_value_gbp - net_deposited_external_to_date_gbp)
        ) > 0.01
        """
    ).fetchone()[0]

    issue_count = int(len(duplicates)) + int(len(unmapped)) + int(len(valuation_gaps)) + int(mart_consistency)
    status = "ok" if issue_count == 0 else "warning"

    return {
        "status": status,
        "run_id": run_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "issue_count": issue_count,
        "checks": {
            "duplicate_stg_transactions": duplicates.to_dict(orient="records"),
            "unmapped_transaction_types": unmapped.to_dict(orient="records"),
            "valuation_gaps_gt_7_days": valuation_gaps,
            "mart_gain_formula_mismatches": int(mart_consistency),
        },
    }


def main() -> None:
    args = parse_args()
    db_path = get_db_path(args.db_path)
    conn = connect_db(db_path)
    ensure_schema(conn)

    run_id = args.run_id or fetch_latest_run_id(conn)
    if not run_id:
        raise ValueError("No import runs found. Run src.ingest.run_import first.")

    report = build_quality_report(conn=conn, run_id=run_id)

    output_path = Path(args.output_json) if args.output_json else (
        PROJECT_ROOT / "data" / "marts" / f"quality_report_{run_id}.json"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Quality report status={report['status']} issues={report['issue_count']} path={output_path}")


if __name__ == "__main__":
    main()
