from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.ingest.hl_holdings_snapshot_parser import parse_holdings_snapshot_file


def test_parse_holdings_snapshot_file_valid(tmp_path: Path) -> None:
    csv_path = tmp_path / "holdings_snapshot_2026-02-13.csv"
    pd.DataFrame(
        [
            {
                "account_name": "Stocks & Shares ISA",
                "as_of_date": "13/02/2026",
                "asset_name": "Fund A",
                "market_value_gbp": "1,250.00",
                "isin": "GB00TEST1234",
                "sedol": "B123456",
                "units": "12.5",
                "currency": "GBP",
            }
        ]
    ).to_csv(csv_path, index=False)

    out = parse_holdings_snapshot_file(path=csv_path, run_id="RUN_001")
    assert len(out) == 1
    row = out.iloc[0]
    assert row["run_id"] == "RUN_001"
    assert row["account_name_raw"] == "Stocks & Shares ISA"
    assert row["asset_name"] == "Fund A"
    assert float(row["market_value_gbp"]) == 1250.0


def test_parse_holdings_snapshot_file_missing_required_column(tmp_path: Path) -> None:
    csv_path = tmp_path / "holdings_snapshot_bad.csv"
    pd.DataFrame(
        [
            {
                "account_name": "Stocks & Shares ISA",
                "as_of_date": "13/02/2026",
                "market_value_gbp": "1250.00",
            }
        ]
    ).to_csv(csv_path, index=False)

    with pytest.raises(ValueError):
        parse_holdings_snapshot_file(path=csv_path, run_id="RUN_001")
