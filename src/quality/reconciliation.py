from __future__ import annotations

import argparse
import json
import re
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd

from src.common.db import connect_db, ensure_schema, fetch_latest_run_id
from src.common.paths import PROJECT_ROOT, get_db_path

ACCOUNT_DATE_HEADING_RE = re.compile(
    r"^(?:stocks\s*&?\s*shares\s+isa|lifetime\s+isa|sipp|loyalty\s+bonus\s+account|fund\s*&?\s*share\s+account)\s+"
    r"\d{1,2}\s+[a-z]+\s+\d{4}$",
    re.IGNORECASE,
)


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


def _json_safe_value(value: object) -> object:
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        if value.hour == 0 and value.minute == 0 and value.second == 0 and value.microsecond == 0:
            return value.date().isoformat()
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "item"):
        return _json_safe_value(value.item())  # type: ignore[no-any-return]
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _records_for_json(frame: pd.DataFrame) -> list[dict[str, object]]:
    return [
        {str(key): _json_safe_value(value) for key, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]


def _suspicious_asset_heading_findings(conn, run_id: str) -> list[dict[str, object]]:
    frame = conn.execute(
        """
        select account_id, asset_id, asset_name_canonical, d, value_gbp, source_file, source_row_num
        from stg_asset_checkpoint
        where source_run_id = ?
        order by account_id, d, asset_name_canonical
        """,
        [run_id],
    ).df()
    if frame.empty:
        return []

    mask = frame["asset_name_canonical"].astype(str).apply(
        lambda name: bool(ACCOUNT_DATE_HEADING_RE.match(name.strip()))
    )
    return _records_for_json(frame[mask])


def _asset_after_latest_account_value_findings(conn, run_id: str) -> list[dict[str, object]]:
    frame = conn.execute(
        """
        with latest_values as (
          select account_id, max(d) as latest_account_value_date
          from stg_account_value_daily
          where source_run_id = ?
          group by account_id
        )
        select
          a.account_id,
          v.latest_account_value_date,
          min(a.d) as first_asset_checkpoint_after_latest_value,
          max(a.d) as latest_asset_checkpoint_date,
          count(*) as asset_checkpoint_rows_after_latest_value
        from stg_asset_checkpoint a
        left join latest_values v using (account_id)
        where a.source_run_id = ?
          and (v.latest_account_value_date is null or a.d > v.latest_account_value_date)
        group by a.account_id, v.latest_account_value_date
        order by a.account_id
        """,
        [run_id, run_id],
    ).df()
    return _records_for_json(frame)


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

    suspicious_asset_headings = _suspicious_asset_heading_findings(conn=conn, run_id=run_id)
    asset_after_latest_account_value = _asset_after_latest_account_value_findings(conn=conn, run_id=run_id)
    critical_issue_count = len(suspicious_asset_headings) + len(asset_after_latest_account_value)

    issue_count = (
        len(duplicates)
        + len(unmapped)
        + len(valuation_gaps)
        + int(mart_consistency)
        + critical_issue_count
    )
    if critical_issue_count:
        status = "error"
    else:
        status = "ok" if issue_count == 0 else "warning"

    return {
        "status": status,
        "run_id": run_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "issue_count": issue_count,
        "critical_issue_count": critical_issue_count,
        "checks": {
            "duplicate_stg_transactions": _records_for_json(duplicates),
            "unmapped_transaction_types": _records_for_json(unmapped),
            "valuation_gaps_gt_7_days": valuation_gaps,
            "mart_gain_formula_mismatches": int(mart_consistency),
            "suspicious_asset_heading_names": suspicious_asset_headings,
            "asset_checkpoints_after_latest_account_value": asset_after_latest_account_value,
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
    if report["status"] == "error":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
