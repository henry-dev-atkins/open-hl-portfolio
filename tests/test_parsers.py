from pathlib import Path

from src.ingest.hl_transactions_parser import parse_transactions_file
from src.ingest.hl_valuations_parser import parse_valuations_file


def test_parse_transactions_fixture() -> None:
    fixture = Path("tests/fixtures/sample_transactions.csv")
    df = parse_transactions_file(fixture, run_id="RUN_TEST")
    assert len(df) == 3
    assert set(df.columns) == {
        "run_id",
        "source_file",
        "row_num",
        "account_name_raw",
        "account_id",
        "trade_date",
        "settle_date",
        "txn_type_raw",
        "description_raw",
        "amount_gbp",
        "currency",
        "instrument_name",
        "isin",
        "sedol",
        "units",
        "price",
        "balance_after_gbp",
    }
    assert float(df.iloc[0]["amount_gbp"]) == 1000.0


def test_parse_valuations_fixture() -> None:
    fixture = Path("tests/fixtures/sample_valuations.csv")
    df = parse_valuations_file(fixture, run_id="RUN_TEST")
    assert len(df) == 3
    assert set(df.columns) == {
        "run_id",
        "source_file",
        "row_num",
        "account_name_raw",
        "account_id",
        "valuation_date",
        "total_value_gbp",
        "cash_value_gbp",
        "invested_value_gbp",
    }
    assert float(df.iloc[-1]["total_value_gbp"]) == 1020.0
