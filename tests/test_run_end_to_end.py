from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.common.db import connect_db, ensure_schema
from src.pipeline import run_end_to_end as runner
from src.prices.asset_identity import canonicalize_asset_name, make_asset_id


def _args(tmp_path: Path, **overrides: object) -> argparse.Namespace:
    values = {
        "run_date": "2026-02-13",
        "db_path": str(tmp_path / "hl_portfolio.duckdb"),
        "provider": "yfinance",
        "skip_download": True,
        "skip_parse": True,
        "skip_tickers": True,
        "skip_prices": True,
        "skip_app": True,
        "force_download": False,
        "force_parse": False,
        "force_tickers": False,
        "force_prices": False,
        "disable_auto_search": False,
        "disable_price_gap_fill": False,
        "headless": False,
        "login_timeout_seconds": 900,
        "max_scroll_cycles": 15,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def test_parse_cache_requires_all_expected_files(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(runner, "PROJECT_ROOT", tmp_path)
    paths = runner.build_paths("2026-02-13", db_path=tmp_path / "db.duckdb")

    paths.extracted_csv.parent.mkdir(parents=True, exist_ok=True)
    for path in [paths.extracted_csv, paths.overview_csv, paths.capital_tx_csv]:
        path.write_text("header\nrow\n", encoding="utf-8")

    assert runner.parse_cache_complete(paths) is False

    paths.asset_values_csv.write_text("header\nrow\n", encoding="utf-8")

    assert runner.parse_cache_complete(paths) is True


def test_ticker_and_price_cache_checks_use_current_asset_values(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(runner, "PROJECT_ROOT", tmp_path)
    paths = runner.build_paths("2026-02-13", db_path=tmp_path / "db.duckdb")
    paths.asset_values_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"asset_name": "Demo Global Equity Fund", "report_date": "13 February 2026"},
            {"asset_name": "Demo UK Index Fund", "report_date": "13 February 2026"},
        ]
    ).to_csv(paths.asset_values_csv, index=False)

    asset_ids = [
        make_asset_id(canonicalize_asset_name("Demo Global Equity Fund")),
        make_asset_id(canonicalize_asset_name("Demo UK Index Fund")),
    ]

    conn = connect_db(paths.db_path)
    ensure_schema(conn)
    conn.execute(
        """
        insert into asset_ticker_mapping (asset_id, provider, ticker, currency, match_status)
        values
          (?, 'yfinance', 'AAA.L', 'GBP', 'resolved'),
          (?, 'yfinance', 'BBB.L', 'GBP', 'resolved')
        """,
        asset_ids,
    )

    assert runner.ticker_cache_complete(paths, provider="yfinance") is True
    assert runner.price_cache_complete(paths, provider="yfinance") is False

    conn.execute(
        """
        insert into raw_market_price_daily
          (provider, ticker, d, close, fetched_at)
        values
          ('yfinance', 'AAA.L', '2026-02-13', 100.0, now()),
          ('yfinance', 'BBB.L', '2026-02-13', 200.0, now())
        """
    )

    assert runner.price_cache_complete(paths, provider="yfinance") is True


def test_run_pipeline_skip_flags_run_only_build_quality_and_no_app(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(runner, "PROJECT_ROOT", tmp_path)
    commands: list[list[str]] = []

    def fake_run_command(command, env=None) -> None:
        commands.append(list(command))

    monkeypatch.setattr(runner, "run_command", fake_run_command)

    runner.run_pipeline(_args(tmp_path))

    modules = [cmd[2] for cmd in commands if len(cmd) >= 3 and cmd[1] == "-m"]
    assert modules == [
        "src.marts.build_report_marts",
        "src.quality.reconciliation",
    ]
    assert not any("streamlit" in cmd for command in commands for cmd in command)


def test_force_flags_override_completed_cache(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(runner, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(runner, "download_cache_complete", lambda paths: True)
    monkeypatch.setattr(runner, "parse_cache_complete", lambda paths: True)
    monkeypatch.setattr(runner, "ticker_cache_complete", lambda paths, provider: True)
    monkeypatch.setattr(runner, "price_cache_complete", lambda paths, provider: True)

    commands: list[list[str]] = []
    monkeypatch.setattr(runner, "run_command", lambda command, env=None: commands.append(list(command)))

    runner.run_pipeline(
        _args(
            tmp_path,
            skip_download=False,
            skip_parse=False,
            skip_tickers=False,
            skip_prices=False,
            force_download=True,
            force_parse=True,
            force_tickers=True,
            force_prices=True,
        )
    )

    modules = [cmd[2] for cmd in commands if len(cmd) >= 3 and cmd[1] == "-m"]
    assert modules == [
        "src.fetch.hl_investment_reports_fetch",
        "src.ingest.hl_investment_report_pdf_parser",
        "src.prices.resolve_asset_tickers",
        "src.prices.fetch_yfinance_prices",
        "src.marts.build_report_marts",
        "src.quality.reconciliation",
    ]


def test_skip_app_prints_command_instead_of_launching(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(runner, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(runner, "_streamlit_command", lambda: ["uv", "run", "streamlit", "run", "src/presentation/app.py"])
    monkeypatch.setattr(runner, "run_command", lambda command, env=None: None)

    runner.run_pipeline(_args(tmp_path))

    out = capsys.readouterr().out
    assert "SKIP app launch" in out
    assert "uv run streamlit run src/presentation/app.py" in out
