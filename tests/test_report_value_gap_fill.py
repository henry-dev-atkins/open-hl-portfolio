from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.db import connect_db, ensure_schema
from src.marts.report_value_gap_fill import build_gap_filled_account_values
from src.prices.asset_identity import canonicalize_asset_name, make_asset_id


def _seed_asset_mapping(conn, asset_name: str, ticker: str, first_seen: str = "2026-01-01") -> str:
    canonical = canonicalize_asset_name(asset_name)
    asset_id = make_asset_id(canonical)
    conn.execute(
        """
        insert into dim_asset (asset_id, asset_name_canonical, first_seen_date, source_priority)
        values (?, ?, ?, 'test')
        """,
        [asset_id, canonical, first_seen],
    )
    conn.execute(
        """
        insert into asset_ticker_mapping (asset_id, provider, ticker, currency, match_status)
        values (?, 'yfinance', ?, 'GBP', 'resolved')
        """,
        [asset_id, ticker],
    )
    return asset_id


def _seed_prices(conn, ticker: str, prices: list[tuple[str, float]]) -> None:
    for d, px in prices:
        conn.execute(
            """
            insert into raw_market_price_daily
              (provider, ticker, d, open, high, low, close, adj_close, volume, currency, fetched_at)
            values
              ('yfinance', ?, ?, ?, ?, ?, ?, ?, 1000, 'GBP', now())
            """,
            [ticker, d, px, px, px, px, px],
        )


def _asset_values_csv(path: Path, rows: list[dict[str, object]]) -> Path:
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def test_gap_fill_price_shape_and_checkpoint_lock(tmp_path: Path) -> None:
    conn = connect_db(tmp_path / "gap_fill_shape.duckdb")
    ensure_schema(conn)

    _seed_asset_mapping(conn, asset_name="Test Asset", ticker="TEST.L")
    _seed_prices(
        conn,
        ticker="TEST.L",
        prices=[
            ("2026-01-01", 100.0),
            ("2026-01-02", 110.0),
            ("2026-01-03", 99.0),
            ("2026-01-04", 99.0),
            ("2026-01-05", 99.0),
        ],
    )

    asset_csv = _asset_values_csv(
        tmp_path / "asset_values.csv",
        [
            {
                "account_name": "Stocks & Shares ISA",
                "asset_name": "Test Asset",
                "value_gbp": 100.0,
                "report_date": "2026-01-01",
            },
            {
                "account_name": "Stocks & Shares ISA",
                "asset_name": "Test Asset",
                "value_gbp": 120.0,
                "report_date": "2026-01-05",
            },
        ],
    )
    value_df = pd.DataFrame(
        {
            "account_id": ["ISA", "ISA"],
            "d": [pd.Timestamp("2026-01-01").date(), pd.Timestamp("2026-01-05").date()],
            "close_value_gbp": [100.0, 120.0],
        }
    )
    flow_df = pd.DataFrame(columns=["account_id", "d", "external_flow_gbp"])

    out, diagnostics = build_gap_filled_account_values(
        conn=conn,
        value_df=value_df,
        flow_df=flow_df,
        asset_values_csv=asset_csv,
        provider="yfinance",
    )

    assert diagnostics["segments_with_price_index"] == 1
    assert len(out) == 5

    day1 = out[out["d"] == pd.Timestamp("2026-01-01").date()].iloc[0]
    day2 = out[out["d"] == pd.Timestamp("2026-01-02").date()].iloc[0]
    day3 = out[out["d"] == pd.Timestamp("2026-01-03").date()].iloc[0]
    day5 = out[out["d"] == pd.Timestamp("2026-01-05").date()].iloc[0]

    assert float(day1["close_value_gbp"]) == 100.0
    assert float(day5["close_value_gbp"]) == 120.0
    assert float(day2["close_value_gbp"]) > float(day3["close_value_gbp"])


def test_gap_fill_fallback_path_uses_flows_and_preserves_endpoints(tmp_path: Path) -> None:
    conn = connect_db(tmp_path / "gap_fill_fallback.duckdb")
    ensure_schema(conn)

    _seed_asset_mapping(conn, asset_name="Flow Asset", ticker="FLOW.L")
    asset_csv = _asset_values_csv(
        tmp_path / "asset_values_fallback.csv",
        [
            {
                "account_name": "Stocks & Shares ISA",
                "asset_name": "Flow Asset",
                "value_gbp": 100.0,
                "report_date": "2026-01-01",
            }
        ],
    )

    value_df = pd.DataFrame(
        {
            "account_id": ["ISA", "ISA"],
            "d": [pd.Timestamp("2026-01-01").date(), pd.Timestamp("2026-01-03").date()],
            "close_value_gbp": [100.0, 110.0],
        }
    )
    flow_df = pd.DataFrame(
        {
            "account_id": ["ISA"],
            "d": [pd.Timestamp("2026-01-02").date()],
            "external_flow_gbp": [10.0],
        }
    )

    out, diagnostics = build_gap_filled_account_values(
        conn=conn,
        value_df=value_df,
        flow_df=flow_df,
        asset_values_csv=asset_csv,
        provider="yfinance",
    )

    assert diagnostics["segments_fallback"] == 1

    day1 = out[out["d"] == pd.Timestamp("2026-01-01").date()].iloc[0]
    day2 = out[out["d"] == pd.Timestamp("2026-01-02").date()].iloc[0]
    day3 = out[out["d"] == pd.Timestamp("2026-01-03").date()].iloc[0]
    assert float(day1["close_value_gbp"]) == 100.0
    assert float(day2["close_value_gbp"]) == 110.0
    assert float(day3["close_value_gbp"]) == 110.0


def test_gap_fill_treats_internal_transfers_as_account_value_steps(tmp_path: Path) -> None:
    conn = connect_db(tmp_path / "gap_fill_internal.duckdb")
    ensure_schema(conn)

    _seed_asset_mapping(conn, asset_name="Transfer Asset", ticker="XFER.L")
    asset_csv = _asset_values_csv(
        tmp_path / "asset_values_internal.csv",
        [
            {
                "account_name": "Stocks & Shares ISA",
                "asset_name": "Transfer Asset",
                "value_gbp": 100.0,
                "report_date": "2026-01-01",
            }
        ],
    )

    value_df = pd.DataFrame(
        {
            "account_id": ["ISA", "ISA"],
            "d": [pd.Timestamp("2026-01-01").date(), pd.Timestamp("2026-01-03").date()],
            "close_value_gbp": [100.0, 200.0],
        }
    )
    flow_df = pd.DataFrame(
        {
            "account_id": ["ISA"],
            "d": [pd.Timestamp("2026-01-02").date()],
            "external_flow_gbp": [0.0],
            "internal_flow_gbp": [100.0],
        }
    )

    out, diagnostics = build_gap_filled_account_values(
        conn=conn,
        value_df=value_df,
        flow_df=flow_df,
        asset_values_csv=asset_csv,
        provider="yfinance",
    )

    assert diagnostics["segments_fallback"] == 1

    day2 = out[out["d"] == pd.Timestamp("2026-01-02").date()].iloc[0]
    assert float(day2["close_value_gbp"]) == 200.0


def test_gap_fill_forward_fills_non_trading_days(tmp_path: Path) -> None:
    conn = connect_db(tmp_path / "gap_fill_ffill.duckdb")
    ensure_schema(conn)

    _seed_asset_mapping(conn, asset_name="Weekend Asset", ticker="WKND.L")
    _seed_prices(
        conn,
        ticker="WKND.L",
        prices=[("2026-01-01", 100.0), ("2026-01-03", 110.0)],
    )
    asset_csv = _asset_values_csv(
        tmp_path / "asset_values_ffill.csv",
        [
            {
                "account_name": "Stocks & Shares ISA",
                "asset_name": "Weekend Asset",
                "value_gbp": 100.0,
                "report_date": "2026-01-01",
            }
        ],
    )
    value_df = pd.DataFrame(
        {
            "account_id": ["ISA", "ISA"],
            "d": [pd.Timestamp("2026-01-01").date(), pd.Timestamp("2026-01-03").date()],
            "close_value_gbp": [100.0, 120.0],
        }
    )
    flow_df = pd.DataFrame(columns=["account_id", "d", "external_flow_gbp"])

    out, _ = build_gap_filled_account_values(
        conn=conn,
        value_df=value_df,
        flow_df=flow_df,
        asset_values_csv=asset_csv,
        provider="yfinance",
    )

    assert len(out) == 3
    day2 = out[out["d"] == pd.Timestamp("2026-01-02").date()]
    assert not day2.empty
    day2_value = float(day2.iloc[0]["close_value_gbp"])
    assert 100.0 < day2_value < 120.0


def test_gap_fill_neutralizes_suspect_split_jump(tmp_path: Path) -> None:
    conn = connect_db(tmp_path / "gap_fill_split_jump.duckdb")
    ensure_schema(conn)

    _seed_asset_mapping(conn, asset_name="Split Asset", ticker="SPLT.L")
    _seed_prices(
        conn,
        ticker="SPLT.L",
        prices=[
            ("2026-01-01", 100.0),
            ("2026-01-02", 4000.0),  # suspicious 40x jump
            ("2026-01-03", 4200.0),
            ("2026-01-04", 4100.0),
            ("2026-01-05", 4300.0),
        ],
    )
    asset_csv = _asset_values_csv(
        tmp_path / "asset_values_split.csv",
        [
            {
                "account_name": "Stocks & Shares ISA",
                "asset_name": "Split Asset",
                "value_gbp": 100.0,
                "report_date": "2026-01-01",
            }
        ],
    )
    value_df = pd.DataFrame(
        {
            "account_id": ["ISA", "ISA"],
            "d": [pd.Timestamp("2026-01-01").date(), pd.Timestamp("2026-01-05").date()],
            "close_value_gbp": [100.0, 100.0],
        }
    )
    flow_df = pd.DataFrame(columns=["account_id", "d", "external_flow_gbp"])

    out, _ = build_gap_filled_account_values(
        conn=conn,
        value_df=value_df,
        flow_df=flow_df,
        asset_values_csv=asset_csv,
        provider="yfinance",
    )

    assert len(out) == 5
    assert float(out.iloc[0]["close_value_gbp"]) == 100.0
    assert float(out.iloc[-1]["close_value_gbp"]) == 100.0
    assert float(out["close_value_gbp"].min()) > 0.0
