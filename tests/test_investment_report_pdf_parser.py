from src.ingest.hl_investment_report_pdf_parser import (
    _extract_asset_value_rows,
    _extract_capital_transactions,
    _extract_overview_rows,
)


def test_extract_overview_rows_uses_last_block() -> None:
    text = """
    Contents
    Portfolio Overview 30 April 2025
    Market Review
    ...
    PORTFOLIO OVERVIEW 31 JULY 2025
    Account Value (Â£) as at
    31 July 2025
    Value (Â£) as at
    30 April 2025
    Change (Â£) since
    30 April 2025
    Stocks & Shares ISA 10,000 9,500 500
    Lifetime ISA 5,000 4,900 100
    TOTAL 15,000 14,400 600
    PAGE 2| SUMMER 2025
    """
    report_date, prev_date, rows = _extract_overview_rows(text)
    assert report_date == "31 JULY 2025"
    assert prev_date == "30 April 2025"
    assert len(rows) == 3
    assert rows[0]["account_name"] == "Stocks & Shares ISA"
    assert rows[0]["value_current"] == 10000.0


def test_extract_overview_rows_parses_parenthesized_negative_changes() -> None:
    text = """
    PORTFOLIO OVERVIEW 31 JULY 2026
    Account Value (Ł) as at
    31 July 2026
    Value (Ł) as at
    30 April 2026
    Change (Ł) since
    30 April 2026
    Stocks & Shares ISA 44,332 47,285 (2,953)
    SIPP 22,056 21,276 780
    Loyalty Bonus Account 0 0 0
    Lifetime ISA 36,319 42,441 (6,122)
    TOTAL 102,707 111,002 (8,295)
    PAGE 2| SUMMER 2026
    """
    _, _, rows = _extract_overview_rows(text)

    by_account = {row["account_name"]: row for row in rows}
    assert by_account["Stocks & Shares ISA"]["change_value"] == -2953.0
    assert by_account["Lifetime ISA"]["value_current"] == 36319.0
    assert by_account["Lifetime ISA"]["change_value"] == -6122.0
    assert by_account["TOTAL"]["change_value"] == -8295.0


def test_extract_capital_transactions_parses_line_formats() -> None:
    text = """
    - STOCKS & SHARES ISA
    CAPITAL ACCOUNT TRANSACTIONS
    1,000.00Opening Subscription02/04/2019 1,479.23
    Transfer to Capital Account 4.0911/02/2019 4.65
    (2.34)Management Fee : HL Stocks & Shares ISA03/04/2019 1,476.89
    0.06Interest : From 10/03/2019 TO 09/04/201909/04/2019 1,476.95
    """
    rows = _extract_capital_transactions(text)
    assert len(rows) == 4

    first = rows[0]
    assert first["txn_type"] == "deposit"
    assert first["flow_class"] == "external_in"
    assert first["subledger"] == "capital"
    assert first["amount_gbp"] == 1000.0

    transfer = next(r for r in rows if "Transfer to Capital Account" in r["description"])
    assert transfer["flow_class"] == "internal_in"
    assert transfer["amount_gbp"] == 4.09

    fee = next(r for r in rows if "Management Fee" in r["description"])
    assert fee["flow_class"] == "fee"
    assert fee["amount_gbp"] == -2.34


def test_extract_asset_value_rows_handles_inline_and_next_line_names() -> None:
    text = """
    PORTFOLIO OVERVIEW 31 JULY 2025
    - STOCKS & SHARES ISA DETAILED VALUATION
    240 764 799 (35) (4.3)318.40.096Barings Global Agriculture (Acc)
    188 3,126 2,011 1,115 55.41,662.80
    Global X Silver Miners UCITS ETF USD Acc (GBP...)
    Subtotal 22,065
    CAPITAL ACCOUNT TRANSACTIONS
    """
    rows = _extract_asset_value_rows(text, report_date="31 JULY 2025")
    assert len(rows) == 2
    assert rows[0]["account_name"] == "STOCKS & SHARES ISA"
    assert rows[0]["asset_name"] == "Barings Global Agriculture (Acc)"
    assert rows[0]["value_gbp"] == 240.0
    assert rows[1]["asset_name"] == "Global X Silver Miners UCITS ETF USD Acc (GBP...)"
    assert rows[1]["value_gbp"] == 188.0


def test_extract_asset_value_rows_ignores_totals() -> None:
    text = """
    - SIPP DETAILED VALUATION
    2,455 4,312 3,988 324 8.1175.59.946BNY Mellon Emerging Income (Acc)
    Subtotal 10,374
    10,498Stock total
    Capital Account Balance 419
    CAPITAL ACCOUNT TRANSACTIONS
    """
    rows = _extract_asset_value_rows(text, report_date="31 JULY 2025")
    assert len(rows) == 1
    assert rows[0]["asset_name"] == "BNY Mellon Emerging Income (Acc)"
    assert rows[0]["value_gbp"] == 2455.0


def test_extract_asset_value_rows_rejects_concatenated_total_account_date_heading() -> None:
    text = """
    - LIFETIME ISA DETAILED VALUATION
    1,373 2,598 1,689 909 53.8189.25Blackrock Frontiers Investment Tst Ord 1p
    Capital Account Balance 231
    Income Account Balance 0
    21,70255,622TOTAL
    LIFETIME ISA 31 JANUARY 2026
    PAGE 5| WINTER 2026
    CAPITAL ACCOUNT TRANSACTIONS
    """
    rows = _extract_asset_value_rows(text, report_date="31 JANUARY 2026")

    assert len(rows) == 1
    assert rows[0]["asset_name"] == "Blackrock Frontiers Investment Tst Ord 1p"
    assert rows[0]["value_gbp"] == 1373.0
