from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.db import connect_db, ensure_schema
from src.prices import fetch_yfinance_prices as prices


def _seed_mapping_data(db_path: Path) -> object:
    conn = connect_db(db_path)
    ensure_schema(conn)

    conn.execute(
        """
        insert into dim_asset (asset_id, asset_name_canonical, first_seen_date, source_priority)
        values
          ('ASSET_A', 'asset a', '2020-01-15', 'test'),
          ('ASSET_B', 'asset b', '2021-03-20', 'test')
        """
    )
    conn.execute(
        """
        insert into asset_ticker_mapping (asset_id, provider, ticker, currency, match_status)
        values
          ('ASSET_A', 'yfinance', 'AAA.L', 'GBP', 'resolved'),
          ('ASSET_B', 'yfinance', 'BBB.L', 'USD', 'resolved')
        """
    )
    return conn


def test_fetch_prices_auto_start_date_uses_first_seen(monkeypatch, tmp_path: Path) -> None:
    conn = _seed_mapping_data(tmp_path / "fetch_auto.duckdb")

    calls: list[tuple[str, str | None, str | None]] = []

    def fake_download_ticker(ticker: str, start_date: str | None, end_date: str | None) -> pd.DataFrame:
        calls.append((ticker, start_date, end_date))
        return pd.DataFrame(
            {
                "d": [pd.Timestamp("2026-01-02").date()],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "adj_close": [100.4],
                "volume": [1000],
            }
        )

    monkeypatch.setattr(prices, "_download_ticker", fake_download_ticker)

    result = prices.fetch_prices(conn=conn, provider="yfinance", start_date=None, end_date=None)

    assert result["ticker_count"] >= 2
    assert result["requested_start_min"] == "2020-01-15"
    assert result["requested_start_max"] == "2021-03-20"
    assert result["failed_tickers"] == 0

    call_map = {ticker: start for ticker, start, _ in calls}
    assert call_map["AAA.L"] == "2020-01-15"
    assert call_map["BBB.L"] == "2021-03-20"


def test_fetch_prices_explicit_start_date_overrides_auto(monkeypatch, tmp_path: Path) -> None:
    conn = _seed_mapping_data(tmp_path / "fetch_explicit.duckdb")
    calls: list[tuple[str, str | None, str | None]] = []

    def fake_download_ticker(ticker: str, start_date: str | None, end_date: str | None) -> pd.DataFrame:
        calls.append((ticker, start_date, end_date))
        return pd.DataFrame(
            {
                "d": [pd.Timestamp("2026-01-03").date()],
                "open": [50.0],
                "high": [52.0],
                "low": [49.0],
                "close": [51.0],
                "adj_close": [51.0],
                "volume": [2000],
            }
        )

    monkeypatch.setattr(prices, "_download_ticker", fake_download_ticker)

    result = prices.fetch_prices(
        conn=conn,
        provider="yfinance",
        start_date="2019-01-01",
        end_date="2020-12-31",
    )

    assert result["requested_start_min"] == "2019-01-01"
    assert result["requested_start_max"] == "2019-01-01"
    assert result["requested_end_min"] == "2020-12-31"
    assert result["requested_end_max"] == "2020-12-31"

    assert calls
    assert all(start == "2019-01-01" for _, start, _ in calls)
    assert all(end == "2020-12-31" for _, _, end in calls)


def test_fetch_prices_scopes_benchmark_dates_to_benchmark_ticker(monkeypatch, tmp_path: Path) -> None:
    conn = _seed_mapping_data(tmp_path / "fetch_benchmark_scope.duckdb")
    calls: list[tuple[str, str | None, str | None]] = []

    def fake_download_ticker(ticker: str, start_date: str | None, end_date: str | None) -> pd.DataFrame:
        calls.append((ticker, start_date, end_date))
        return pd.DataFrame(
            {
                "d": [pd.Timestamp("2026-01-04").date()],
                "open": [75.0],
                "high": [76.0],
                "low": [74.0],
                "close": [75.5],
                "adj_close": [75.4],
                "volume": [1500],
            }
        )

    monkeypatch.setattr(
        prices,
        "_load_benchmark_config",
        lambda: {
            "provider": "yfinance",
            "ticker": "AAA.L",
            "start_date": "2010-01-01",
            "end_date": "2015-12-31",
        },
    )
    monkeypatch.setattr(prices, "_download_ticker", fake_download_ticker)

    result = prices.fetch_prices(conn=conn, provider="yfinance", start_date=None, end_date=None)

    assert result["requested_start_min"] == "2010-01-01"
    assert result["requested_start_max"] == "2021-03-20"
    assert result["requested_end_min"] == "2015-12-31"
    assert result["requested_end_max"] == "2015-12-31"

    call_map = {ticker: (start, end) for ticker, start, end in calls}
    assert call_map["AAA.L"] == ("2010-01-01", "2015-12-31")
    assert call_map["BBB.L"] == ("2021-03-20", None)
