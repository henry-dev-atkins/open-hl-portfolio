from pathlib import Path

import pandas as pd

from src.common.db import connect_db, ensure_schema
from src.marts.build_report_marts import build_report_marts
from src.prices.asset_identity import canonicalize_asset_name, make_asset_id


def test_build_report_marts_from_overview_and_capital_flows(tmp_path: Path) -> None:
    overview_csv = tmp_path / "overview.csv"
    capital_csv = tmp_path / "capital.csv"
    asset_values_csv = tmp_path / "asset_values.csv"
    db_path = tmp_path / "report_marts.duckdb"

    overview_df = pd.DataFrame(
        [
            {
                "account_name": "Stocks & Shares ISA",
                "value_current": 12000.0,
                "value_previous": 10000.0,
                "change_value": 2000.0,
                "source_pdf": "a.pdf",
                "report_label": "Winter_2026",
                "season": "Winter",
                "year": 2026,
                "report_date": "31 January 2026",
                "previous_period_date": "31 October 2025",
            },
            {
                "account_name": "Lifetime ISA",
                "value_current": 8000.0,
                "value_previous": 7000.0,
                "change_value": 1000.0,
                "source_pdf": "a.pdf",
                "report_label": "Winter_2026",
                "season": "Winter",
                "year": 2026,
                "report_date": "31 January 2026",
                "previous_period_date": "31 October 2025",
            },
        ]
    )
    overview_df.to_csv(overview_csv, index=False)

    capital_df = pd.DataFrame(
        [
            {
                "account_name": "Stocks & Shares ISA",
                "event_date": "03/01/2026",
                "description": "HL ISA Receipt : Pay By Bank",
                "amount_gbp": 500.0,
                "balance_after_gbp": 1000.0,
                "txn_type": "deposit",
                "flow_class": "external_in",
                "source_pdf": "a.pdf",
                "report_label": "Winter_2026",
                "season": "Winter",
                "year": 2026,
                "source_row_num": 1,
            }
        ]
    )
    capital_df.to_csv(capital_csv, index=False)
    asset_values_df = pd.DataFrame(
        [
            {
                "account_name": "Stocks & Shares ISA",
                "asset_name": "Test Asset",
                "value_gbp": 1000.0,
                "report_date": "31 October 2025",
            },
            {
                "account_name": "Stocks & Shares ISA",
                "asset_name": "Test Asset",
                "value_gbp": 1200.0,
                "report_date": "31 January 2026",
            },
        ]
    )
    asset_values_df.to_csv(asset_values_csv, index=False)

    conn = connect_db(db_path)
    ensure_schema(conn)
    canonical_name = canonicalize_asset_name("Test Asset")
    asset_id = make_asset_id(canonical_name)
    conn.execute(
        """
        insert into dim_asset (asset_id, asset_name_canonical, first_seen_date, source_priority)
        values (?, ?, '2025-10-31', 'test')
        """,
        [asset_id, canonical_name],
    )
    conn.execute(
        """
        insert into asset_ticker_mapping (asset_id, provider, ticker, currency, match_status)
        values (?, 'yfinance', 'TEST.L', 'GBP', 'resolved')
        """,
        [asset_id],
    )
    for d, px in [("2025-10-31", 100.0), ("2025-11-30", 106.0), ("2026-01-31", 111.0)]:
        conn.execute(
            """
            insert into raw_market_price_daily
              (provider, ticker, d, open, high, low, close, adj_close, volume, currency, fetched_at)
            values
              ('yfinance', 'TEST.L', ?, ?, ?, ?, ?, ?, 1000, 'GBP', now())
            """,
            [d, px, px, px, px, px],
        )

    account_rows, portfolio_rows, run_id = build_report_marts(
        conn=conn,
        run_date="2026-02-13",
        overview_csv=overview_csv,
        capital_tx_csv=capital_csv,
        asset_values_csv=asset_values_csv,
        price_provider="yfinance",
    )

    assert run_id == "REPORTS_2026-02-13"
    assert account_rows > 0
    assert portfolio_rows > 0

    mart = conn.execute(
        """
        select account_id, d, close_value_gbp, external_flow_gbp, net_deposited_external_to_date_gbp
        from mart_account_daily
        where account_id = 'ISA'
        order by d
        """
    ).df()
    assert not mart.empty
    deposit_days = mart[mart["external_flow_gbp"] > 0]
    assert not deposit_days.empty
    assert float(mart.iloc[0]["close_value_gbp"]) == 10000.0
    assert float(mart.iloc[-1]["close_value_gbp"]) == 12000.0

    end_date = pd.Timestamp("2026-01-31")
    non_flat_intermediate = mart[
        (mart["d"] < end_date) & (mart["close_value_gbp"] != 10000.0)
    ]
    assert not non_flat_intermediate.empty
