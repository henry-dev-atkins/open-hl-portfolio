from __future__ import annotations

import argparse
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from src.common.config import load_yaml
from src.common.db import connect_db, ensure_schema
from src.common.paths import PROJECT_ROOT, get_db_path

try:
    import yfinance as yf
except Exception:  # noqa: BLE001
    yf = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch daily OHLCV prices from Yahoo Finance.")
    parser.add_argument("--db-path", default=None, help="DuckDB file path")
    parser.add_argument("--provider", default="yfinance", help="Provider label in DB")
    parser.add_argument("--start-date", default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="End date YYYY-MM-DD")
    return parser.parse_args()


def _load_resolved_tickers(conn, provider: str) -> pd.DataFrame:
    return conn.execute(
        """
        select
          m.ticker,
          min(m.currency) as currency,
          min(a.first_seen_date) as first_seen_date
        from asset_ticker_mapping m
        left join dim_asset a using (asset_id)
        where m.provider = ?
          and m.match_status = 'resolved'
          and coalesce(trim(m.ticker), '') <> ''
        group by m.ticker
        order by ticker
        """,
        [provider],
    ).df()


def _load_benchmark_config() -> dict[str, Any]:
    cfg_path = PROJECT_ROOT / "config" / "benchmark.yml"
    return load_yaml(cfg_path)


def _date_like_to_yyyy_mm_dd(value: Any) -> str | None:
    if value is None:
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def _download_ticker(ticker: str, start_date: str | None, end_date: str | None) -> pd.DataFrame:
    if yf is None:
        raise RuntimeError("yfinance is not installed. Run: python -m pip install -e .[dev]")

    frame = yf.download(
        tickers=ticker,
        start=start_date,
        end=end_date,
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    if frame is None or frame.empty:
        return pd.DataFrame()

    out = frame.reset_index().rename(
        columns={
            "Date": "d",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )
    out["d"] = pd.to_datetime(out["d"], errors="coerce").dt.date
    out = out[out["d"].notna()].copy()
    return out[["d", "open", "high", "low", "close", "adj_close", "volume"]]


def fetch_prices(conn, provider: str, start_date: str | None, end_date: str | None) -> dict[str, Any]:
    symbols = _load_resolved_tickers(conn=conn, provider=provider)
    bench_cfg = _load_benchmark_config()
    bench_ticker = str(bench_cfg.get("ticker") or "").strip()
    bench_provider = str(bench_cfg.get("provider") or provider).strip() or provider
    symbol_rows = symbols.to_dict(orient="records") if not symbols.empty else []
    if bench_ticker and bench_provider == provider:
        present = {str(r.get("ticker") or "").strip() for r in symbol_rows}
        if bench_ticker not in present:
            symbol_rows.append({"ticker": bench_ticker, "currency": "", "first_seen_date": None})
    if not symbol_rows:
        return {
            "inserted_rows": 0,
            "failed_tickers": 0,
            "ticker_count": 0,
            "requested_start_min": None,
            "requested_start_max": None,
            "requested_end_min": None,
            "requested_end_max": None,
        }

    inserted = 0
    failed = 0
    requested_starts: list[str] = []
    requested_ends: list[str] = []

    cfg_start = str(bench_cfg.get("start_date") or "").strip() or None
    cfg_end = str(bench_cfg.get("end_date") or "").strip() or None

    for row in symbol_rows:
        ticker = str(row.get("ticker") or "").strip()
        currency = str(row.get("currency") or "").strip() or None
        auto_start = _date_like_to_yyyy_mm_dd(row.get("first_seen_date"))
        is_benchmark_ticker = ticker == bench_ticker and bench_provider == provider
        effective_start = str(start_date).strip() if start_date else ((cfg_start if is_benchmark_ticker else None) or auto_start)
        effective_end = str(end_date).strip() if end_date else (cfg_end if is_benchmark_ticker else None)
        if effective_start:
            requested_starts.append(effective_start)
        if effective_end:
            requested_ends.append(effective_end)
        try:
            data = _download_ticker(ticker=ticker, start_date=effective_start, end_date=effective_end)
        except Exception:  # noqa: BLE001
            failed += 1
            continue
        if data.empty:
            continue

        data = data.copy()
        data["provider"] = provider
        data["ticker"] = ticker
        data["currency"] = currency
        data["fetched_at"] = datetime.now(UTC)
        data = data[
            [
                "provider",
                "ticker",
                "d",
                "open",
                "high",
                "low",
                "close",
                "adj_close",
                "volume",
                "currency",
                "fetched_at",
            ]
        ]

        min_d = data["d"].min()
        max_d = data["d"].max()
        conn.execute(
            """
            delete from raw_market_price_daily
            where provider = ?
              and ticker = ?
              and d between ? and ?
            """,
            [provider, ticker, min_d, max_d],
        )

        conn.register("tmp_raw_market_price_daily", data)
        conn.execute("insert into raw_market_price_daily select * from tmp_raw_market_price_daily")
        conn.unregister("tmp_raw_market_price_daily")
        inserted += len(data)

    return {
        "inserted_rows": int(inserted),
        "failed_tickers": int(failed),
        "ticker_count": int(len(symbol_rows)),
        "requested_start_min": min(requested_starts) if requested_starts else None,
        "requested_start_max": max(requested_starts) if requested_starts else None,
        "requested_end_min": min(requested_ends) if requested_ends else None,
        "requested_end_max": max(requested_ends) if requested_ends else None,
    }


def main() -> None:
    args = parse_args()
    conn = connect_db(get_db_path(args.db_path))
    ensure_schema(conn)

    result = fetch_prices(
        conn=conn,
        provider=args.provider,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    print(
        "Yahoo price fetch complete "
        f"provider={args.provider} "
        f"tickers={result['ticker_count']} "
        f"requested_start_min={result['requested_start_min'] or 'auto/open'} "
        f"requested_start_max={result['requested_start_max'] or 'auto/open'} "
        f"requested_end_min={result['requested_end_min'] or 'open'} "
        f"requested_end_max={result['requested_end_max'] or 'open'} "
        f"inserted_rows={result['inserted_rows']} "
        f"failed_tickers={result['failed_tickers']}"
    )


if __name__ == "__main__":
    main()
