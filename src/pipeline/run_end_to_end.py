from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from src.common.db import connect_db, ensure_schema
from src.common.paths import PROJECT_ROOT, get_db_path
from src.prices.resolve_asset_tickers import _build_asset_universe, _load_asset_values

DEFAULT_PROVIDER = "yfinance"


@dataclass(frozen=True)
class PipelinePaths:
    run_date: str
    db_path: Path
    reports_dir: Path
    reports_manifest: Path
    extracted_csv: Path
    overview_csv: Path
    capital_tx_csv: Path
    asset_values_csv: Path
    text_dir: Path
    unresolved_assets_csv: Path
    unresolved_candidates_csv: Path
    quality_report_json: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the HL Investment Report pipeline end to end."
    )
    parser.add_argument("--run-date", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--db-path", default=None, help="DuckDB path.")
    parser.add_argument("--provider", default=DEFAULT_PROVIDER, help="Market price provider label.")

    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-parse", action="store_true")
    parser.add_argument("--skip-tickers", action="store_true")
    parser.add_argument("--skip-prices", action="store_true")
    parser.add_argument("--skip-app", action="store_true")

    parser.add_argument("--force-download", action="store_true")
    parser.add_argument("--force-parse", action="store_true")
    parser.add_argument("--force-tickers", action="store_true")
    parser.add_argument("--force-prices", action="store_true")

    parser.add_argument("--disable-auto-search", action="store_true")
    parser.add_argument("--disable-price-gap-fill", action="store_true")

    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--login-timeout-seconds", type=int, default=900)
    parser.add_argument("--max-scroll-cycles", type=int, default=15)
    return parser.parse_args()


def build_paths(run_date: str, db_path: str | Path | None = None) -> PipelinePaths:
    resolved_db = get_db_path(str(db_path) if db_path is not None else None)
    raw_run_dir = PROJECT_ROOT / "data" / "raw" / run_date
    staging_dir = PROJECT_ROOT / "data" / "staging"
    marts_dir = PROJECT_ROOT / "data" / "marts"
    return PipelinePaths(
        run_date=run_date,
        db_path=Path(resolved_db),
        reports_dir=raw_run_dir / "investment_reports",
        reports_manifest=raw_run_dir / "investment_reports_manifest.json",
        extracted_csv=staging_dir / f"investment_reports_extracted_{run_date}.csv",
        overview_csv=staging_dir / f"investment_reports_overview_{run_date}.csv",
        capital_tx_csv=staging_dir / f"investment_reports_capital_txns_{run_date}.csv",
        asset_values_csv=staging_dir / f"investment_reports_asset_values_{run_date}.csv",
        text_dir=staging_dir / "investment_report_text" / run_date,
        unresolved_assets_csv=staging_dir / f"unresolved_assets_{run_date}.csv",
        unresolved_candidates_csv=staging_dir / f"unresolved_ticker_candidates_{run_date}.csv",
        quality_report_json=marts_dir / f"quality_report_REPORTS_{run_date}.json",
    )


def file_has_content(path: Path) -> bool:
    return path.exists() and path.is_file() and path.stat().st_size > 0


def download_cache_complete(paths: PipelinePaths) -> bool:
    return (
        paths.reports_dir.exists()
        and any(paths.reports_dir.glob("*.pdf"))
        and file_has_content(paths.reports_manifest)
    )


def parse_cache_complete(paths: PipelinePaths) -> bool:
    return all(
        file_has_content(path)
        for path in [
            paths.extracted_csv,
            paths.overview_csv,
            paths.capital_tx_csv,
            paths.asset_values_csv,
        ]
    )


def _asset_ids_from_asset_values(asset_values_csv: Path) -> list[str]:
    asset_values = _load_asset_values(asset_values_csv)
    universe = _build_asset_universe(asset_values)
    return sorted(universe["asset_id"].astype(str).unique().tolist())


def ticker_cache_complete(paths: PipelinePaths, provider: str) -> bool:
    if not file_has_content(paths.asset_values_csv):
        return False
    asset_ids = _asset_ids_from_asset_values(paths.asset_values_csv)
    if not asset_ids:
        return False

    conn = _connect_existing_or_empty(paths.db_path)
    placeholders = ",".join(["?"] * len(asset_ids))
    rows = conn.execute(
        f"""
        select count(*) from asset_ticker_mapping
        where provider = ?
          and asset_id in ({placeholders})
        """,
        [provider, *asset_ids],
    ).fetchone()[0]
    return int(rows) == len(asset_ids)


def price_cache_complete(paths: PipelinePaths, provider: str) -> bool:
    conn = _connect_existing_or_empty(paths.db_path)
    resolved_tickers = conn.execute(
        """
        select distinct ticker
        from asset_ticker_mapping
        where provider = ?
          and match_status = 'resolved'
          and coalesce(trim(ticker), '') <> ''
        """,
        [provider],
    ).fetchall()
    tickers = [str(row[0]) for row in resolved_tickers]
    if not tickers:
        return False

    placeholders = ",".join(["?"] * len(tickers))
    rows = conn.execute(
        f"""
        select count(distinct ticker)
        from raw_market_price_daily
        where provider = ?
          and ticker in ({placeholders})
        """,
        [provider, *tickers],
    ).fetchone()[0]
    return int(rows) == len(tickers)


def _connect_existing_or_empty(db_path: Path) -> duckdb.DuckDBPyConnection:
    conn = connect_db(db_path)
    ensure_schema(conn)
    return conn


def _python_module_command(module: str, args: Sequence[str]) -> list[str]:
    return [sys.executable, "-m", module, *args]


def _streamlit_command() -> list[str]:
    if shutil.which("uv"):
        return ["uv", "run", "streamlit", "run", "src/presentation/app.py"]
    return [sys.executable, "-m", "streamlit", "run", "src/presentation/app.py"]


def run_command(command: Sequence[str], env: dict[str, str] | None = None) -> None:
    print("RUN  " + " ".join(command), flush=True)
    subprocess.run(list(command), check=True, env=env)


def _skip_or_run(
    step_name: str,
    should_skip: bool,
    command: Sequence[str],
    env: dict[str, str] | None = None,
) -> None:
    if should_skip:
        print(f"SKIP {step_name}: cached output already exists", flush=True)
        return
    run_command(command, env=env)
    print(f"DONE {step_name}", flush=True)


def run_pipeline(args: argparse.Namespace) -> None:
    paths = build_paths(run_date=args.run_date, db_path=args.db_path)

    print(f"HL end-to-end pipeline run_date={args.run_date}", flush=True)
    print(f"DB={paths.db_path}", flush=True)

    download_command = _python_module_command(
        "src.fetch.hl_investment_reports_fetch",
        [
            "--run-date",
            args.run_date,
            "--output-dir",
            str(paths.reports_dir),
            "--manifest-path",
            str(paths.reports_manifest),
            "--login-timeout-seconds",
            str(args.login_timeout_seconds),
            "--max-scroll-cycles",
            str(args.max_scroll_cycles),
            *(["--headless"] if args.headless else []),
        ],
    )
    _skip_or_run(
        "download",
        args.skip_download
        or (download_cache_complete(paths) and not args.force_download),
        download_command,
    )

    parse_command = _python_module_command(
        "src.ingest.hl_investment_report_pdf_parser",
        [
            "--run-date",
            args.run_date,
            "--input-dir",
            str(paths.reports_dir),
            "--output-csv",
            str(paths.extracted_csv),
            "--overview-csv",
            str(paths.overview_csv),
            "--capital-tx-csv",
            str(paths.capital_tx_csv),
            "--asset-value-csv",
            str(paths.asset_values_csv),
            "--text-dir",
            str(paths.text_dir),
        ],
    )
    _skip_or_run(
        "parse",
        args.skip_parse or (parse_cache_complete(paths) and not args.force_parse),
        parse_command,
    )

    ticker_command = _python_module_command(
        "src.prices.resolve_asset_tickers",
        [
            "--run-date",
            args.run_date,
            "--db-path",
            str(paths.db_path),
            "--provider",
            args.provider,
            "--asset-values-csv",
            str(paths.asset_values_csv),
            "--unresolved-assets-csv",
            str(paths.unresolved_assets_csv),
            "--unresolved-candidates-csv",
            str(paths.unresolved_candidates_csv),
            *(["--disable-auto-search"] if args.disable_auto_search else []),
        ],
    )
    _skip_or_run(
        "ticker resolution",
        args.skip_tickers
        or (ticker_cache_complete(paths, provider=args.provider) and not args.force_tickers),
        ticker_command,
    )

    price_command = _python_module_command(
        "src.prices.fetch_yfinance_prices",
        [
            "--db-path",
            str(paths.db_path),
            "--provider",
            args.provider,
        ],
    )
    _skip_or_run(
        "price fetch",
        args.skip_prices
        or (price_cache_complete(paths, provider=args.provider) and not args.force_prices),
        price_command,
    )

    build_args = [
        "--run-date",
        args.run_date,
        "--db-path",
        str(paths.db_path),
        "--price-provider",
        args.provider,
        "--overview-csv",
        str(paths.overview_csv),
        "--capital-tx-csv",
        str(paths.capital_tx_csv),
        "--asset-values-csv",
        str(paths.asset_values_csv),
        *(["--disable-price-gap-fill"] if args.disable_price_gap_fill else []),
    ]
    run_command(_python_module_command("src.marts.build_report_marts", build_args))
    print("DONE mart build", flush=True)

    run_id = f"REPORTS_{args.run_date}"
    run_command(
        _python_module_command(
            "src.quality.reconciliation",
            [
                "--db-path",
                str(paths.db_path),
                "--run-id",
                run_id,
                "--output-json",
                str(paths.quality_report_json),
            ],
        )
    )
    print("DONE quality report", flush=True)

    app_env = os.environ.copy()
    app_env["HL_DB_PATH"] = str(paths.db_path)
    app_command = _streamlit_command()
    if args.skip_app:
        print("SKIP app launch", flush=True)
        print("Run dashboard with:", flush=True)
        print(" ".join(app_command), flush=True)
        return
    run_command(app_command, env=app_env)


def main() -> None:
    run_pipeline(parse_args())


if __name__ == "__main__":
    main()
