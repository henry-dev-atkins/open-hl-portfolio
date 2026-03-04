from __future__ import annotations

import argparse
import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.common.db import connect_db, ensure_schema
from src.common.paths import get_db_path
from src.ingest.hl_holdings_snapshot_parser import parse_holdings_snapshot_file
from src.ingest.hl_transactions_parser import parse_transactions_file
from src.ingest.hl_valuations_parser import parse_valuations_file


TRANSACTION_KEYWORDS = ("transaction", "cash", "statement", "activity")
VALUATION_KEYWORDS = ("valuation", "history", "portfolio_value", "account_value")
HOLDINGS_KEYWORDS = ("holding", "holdings", "position", "snapshot")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import HL export files into raw DuckDB tables.")
    parser.add_argument("--input-dir", required=True, help="Directory containing HL CSV exports for one run.")
    parser.add_argument("--db-path", default=None, help="DuckDB file path.")
    parser.add_argument("--run-id", default=None, help="Import run id. Defaults to UTC timestamp.")
    parser.add_argument("--notes", default=None, help="Optional note for this import run.")
    return parser.parse_args()


def detect_file_kind(path: Path) -> str:
    name = path.name.lower()
    has_txn = any(k in name for k in TRANSACTION_KEYWORDS)
    has_val = any(k in name for k in VALUATION_KEYWORDS)
    has_holdings = any(k in name for k in HOLDINGS_KEYWORDS)
    if has_holdings and not has_txn and not has_val:
        return "holdings"
    if has_holdings and has_val:
        return "holdings"
    if has_txn and not has_val:
        return "transactions"
    if has_val and not has_txn:
        return "valuations"
    return "unknown"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(8192), b""):
            h.update(block)
    return h.hexdigest()


def parse_folder(input_dir: Path, run_id: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    csv_files = sorted(input_dir.rglob("*.csv"))
    if not csv_files:
        raise ValueError(f"No CSV files found in {input_dir}")

    transactions: list[pd.DataFrame] = []
    valuations: list[pd.DataFrame] = []
    holdings: list[pd.DataFrame] = []
    source_rows: list[dict[str, object]] = []

    for file_path in csv_files:
        kind = detect_file_kind(file_path)
        parsed = False

        if kind in {"transactions", "unknown"}:
            try:
                tx_df = parse_transactions_file(file_path, run_id=run_id, source_file=file_path.name)
                transactions.append(tx_df)
                kind = "transactions"
                parsed = True
            except Exception:  # noqa: BLE001
                parsed = False

        if not parsed and kind in {"valuations", "unknown"}:
            try:
                val_df = parse_valuations_file(file_path, run_id=run_id, source_file=file_path.name)
                valuations.append(val_df)
                kind = "valuations"
                parsed = True
            except Exception:  # noqa: BLE001
                parsed = False

        if not parsed and kind in {"holdings", "unknown"}:
            try:
                holdings_df = parse_holdings_snapshot_file(file_path, run_id=run_id, source_file=file_path.name)
                holdings.append(holdings_df)
                kind = "holdings"
                parsed = True
            except Exception:  # noqa: BLE001
                parsed = False

        if not parsed:
            print(f"Skipping unrecognized CSV format: {file_path}")
            continue

        source_rows.append(
            {
                "run_id": run_id,
                "source_file": str(file_path.resolve()),
                "file_sha256": sha256_file(file_path),
                "file_size_bytes": file_path.stat().st_size,
                "detected_kind": kind,
            }
        )

    tx_all = pd.concat(transactions, ignore_index=True) if transactions else pd.DataFrame()
    val_all = pd.concat(valuations, ignore_index=True) if valuations else pd.DataFrame()
    holdings_all = pd.concat(holdings, ignore_index=True) if holdings else pd.DataFrame()
    src_df = pd.DataFrame(source_rows)

    if tx_all.empty and val_all.empty and holdings_all.empty:
        raise ValueError(f"No parsable transaction, valuation, or holdings rows found in {input_dir}")

    return tx_all, val_all, holdings_all, src_df


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")

    run_id = args.run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    db_path = get_db_path(args.db_path)

    tx_df, val_df, holdings_df, src_df = parse_folder(input_dir=input_dir, run_id=run_id)

    conn = connect_db(db_path)
    ensure_schema(conn)

    conn.execute("delete from raw_transactions where run_id = ?", [run_id])
    conn.execute("delete from raw_valuations where run_id = ?", [run_id])
    conn.execute("delete from raw_holdings_snapshot where run_id = ?", [run_id])
    conn.execute("delete from raw_source_files where run_id = ?", [run_id])
    conn.execute("delete from raw_import_runs where run_id = ?", [run_id])

    hl_export_date = None
    try:
        hl_export_date = datetime.strptime(input_dir.name, "%Y-%m-%d").date()
    except ValueError:
        hl_export_date = None

    conn.execute(
        """
        insert into raw_import_runs (run_id, imported_at, source_path, hl_export_date, notes)
        values (?, ?, ?, ?, ?)
        """,
        [run_id, datetime.now(UTC), str(input_dir.resolve()), hl_export_date, args.notes],
    )

    if not src_df.empty:
        conn.register("tmp_raw_source_files", src_df)
        conn.execute("insert into raw_source_files select * from tmp_raw_source_files")
        conn.unregister("tmp_raw_source_files")

    if not tx_df.empty:
        conn.register("tmp_raw_transactions", tx_df)
        conn.execute("insert into raw_transactions select * from tmp_raw_transactions")
        conn.unregister("tmp_raw_transactions")

    if not val_df.empty:
        conn.register("tmp_raw_valuations", val_df)
        conn.execute("insert into raw_valuations select * from tmp_raw_valuations")
        conn.unregister("tmp_raw_valuations")

    if not holdings_df.empty:
        conn.register("tmp_raw_holdings_snapshot", holdings_df)
        conn.execute("insert into raw_holdings_snapshot select * from tmp_raw_holdings_snapshot")
        conn.unregister("tmp_raw_holdings_snapshot")

    print(
        f"Import complete run_id={run_id} "
        f"transactions={len(tx_df)} valuations={len(val_df)} holdings={len(holdings_df)} "
        f"files={len(src_df)} db={db_path}"
    )


if __name__ == "__main__":
    main()
