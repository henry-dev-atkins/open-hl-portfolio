# HL Portfolio Analytics

Flow-corrected account, asset, attribution, concentration, and cost-drag analytics for Hargreaves Lansdown portfolio exports.

This project turns HL CSV exports or Investment Report PDFs into normalized DuckDB tables and a Streamlit dashboard. It is designed for local use with your own financial records; credentials are never stored by the project.

## Security And Privacy

Use this at your own risk. Real HL exports, investment report PDFs, generated DuckDB files, screenshots, and browser debug files can contain sensitive financial information.

- Do not commit anything from `data/raw/`, `data/staging/`, or `data/marts/`.
- Do not share screenshots or files containing account numbers, balances, holdings, addresses, or transaction history.
- Prefer the sanitized demo dataset in `examples/demo_data/` for examples, bug reports, and screenshots.
- If you need to share real data privately, redact it first and verify the redacted file still reproduces the issue.

See `docs/privacy_and_redaction.md` for a short redaction checklist.

## Capabilities

- Parses HL transaction and valuation CSV exports.
- Parses HL Investment Report PDFs into account, capital transaction, and asset valuation datasets.
- Builds normalized staging tables and analysis marts in DuckDB.
- Separates external cashflows from investment performance.
- Resolves asset names to Yahoo Finance tickers with optional manual overrides.
- Fetches market prices for asset-level analytics and benchmark context.
- Serves an interactive Streamlit dashboard.

## Quick Start: UV Setup

Install `uv`, then sync the project environment from `pyproject.toml` and `uv.lock`:

```bash
uv sync --extra dev
```

Install the Chromium browser used by Playwright fetch flows:

```bash
uv run python -m playwright install chromium
```

Playwright is only needed for browser-based HL fetching. Local demo runs, manual CSV imports, PDF parsing, tests, and the dashboard use the Python environment created by `uv sync`.

Optional environment defaults can be stored in `.env`:

```bash
cp .env.example .env
```

On Windows PowerShell, use this equivalent copy command:

```powershell
Copy-Item .env.example .env
```

## Safe Demo Run

The fastest way to evaluate the project without personal data is the sanitized demo dataset in `examples/demo_data/`.

Run the demo import:

```bash
uv run python -m src.ingest.run_import --input-dir examples/demo_data/raw/2026-01-03 --db-path data/marts/hl_portfolio_demo.duckdb --run-id DEMO_RUN_2026_01_03 --notes "Sanitized demo dataset"
```

Build marts:

```bash
uv run python -m src.marts.build_marts --db-path data/marts/hl_portfolio_demo.duckdb --run-id DEMO_RUN_2026_01_03
```

Write a quality report:

```bash
uv run python -m src.quality.reconciliation --db-path data/marts/hl_portfolio_demo.duckdb --run-id DEMO_RUN_2026_01_03 --output-json data/marts/quality_report_DEMO_RUN_2026_01_03.json
```

Launch the dashboard against the demo database:

```bash
HL_DB_PATH=data/marts/hl_portfolio_demo.duckdb uv run streamlit run src/presentation/app.py
```

On Windows PowerShell:

```powershell
$env:HL_DB_PATH = "data/marts/hl_portfolio_demo.duckdb"
uv run streamlit run src/presentation/app.py
```

Demo outputs:

- `data/marts/hl_portfolio_demo.duckdb`
- `data/marts/quality_report_DEMO_RUN_2026_01_03.json`

## End-To-End Investment Report Run

For normal personal use, run the full PDF-based workflow with one command:

```bash
uv run python -m src.pipeline.run_end_to_end --run-date 2026-02-13
```

This runs:

1. Download Investment Report PDFs after manual HL login.
2. Parse reports into staging CSVs.
3. Resolve report assets to Yahoo Finance tickers.
4. Fetch daily yfinance prices for resolved tickers.
5. Build account, portfolio, asset, attribution, concentration, and cost-drag marts.
6. Write a quality report.
7. Launch the Streamlit dashboard against the generated DuckDB file.

The runner reuses cached artifacts for the same `--run-date` where possible:

- Existing PDFs plus manifest skip the download step.
- Existing staging CSVs skip parsing.
- Existing ticker mappings skip ticker resolution.
- Existing price rows skip price fetching.
- Marts and the quality report are rebuilt every run so outputs reflect current config and cached data.

Useful options:

```bash
uv run python -m src.pipeline.run_end_to_end --run-date 2026-02-13 --skip-app
uv run python -m src.pipeline.run_end_to_end --run-date 2026-02-13 --force-download
uv run python -m src.pipeline.run_end_to_end --run-date 2026-02-13 --force-parse --force-tickers --force-prices
uv run python -m src.pipeline.run_end_to_end --run-date 2026-02-13 --disable-auto-search
uv run python -m src.pipeline.run_end_to_end --run-date 2026-02-13 --disable-price-gap-fill
```

By default, the runner uses `data/marts/hl_portfolio.duckdb`. Override it with:

```bash
uv run python -m src.pipeline.run_end_to_end \
  --run-date 2026-02-13 \
  --db-path data/marts/my_portfolio.duckdb
```

## Run With Your Own CSV Exports

Place HL CSV exports into a dated raw-data folder:

```text
data/raw/<YYYY-MM-DD>/
```

Example:

```text
data/raw/2026-02-13/
```

Transaction exports, valuation exports, and holdings snapshots can be mixed in the same folder. File type is inferred from filename and CSV structure.

Import the raw files:

```bash
uv run python -m src.ingest.run_import --input-dir data/raw/2026-02-13 --db-path data/marts/hl_portfolio.duckdb --run-id CSV_2026_02_13
```

Build marts:

```bash
uv run python -m src.marts.build_marts --db-path data/marts/hl_portfolio.duckdb --run-id CSV_2026_02_13
```

Write a quality report:

```bash
uv run python -m src.quality.reconciliation --db-path data/marts/hl_portfolio.duckdb --run-id CSV_2026_02_13 --output-json data/marts/quality_report_CSV_2026_02_13.json
```

Optional, for fuller asset-level analytics on CSV runs: include one or more holdings snapshot files matching `holdings_snapshot*.csv` in the same raw-data folder.

## Advanced: Run Investment Report Stages Manually

HL portfolio history may expose account history as Investment Report PDFs. This flow can download reports after manual login, parse them into structured staging CSVs, and build the report-based marts.

Download reports after manual HL login:

```bash
uv run python -m src.fetch.hl_investment_reports_fetch --run-date 2026-08-19
```

What happens:

1. A browser opens.
2. You log in to HL manually, including MFA.
3. The script continues after it detects the authenticated account area.
4. Investment Report PDFs are downloaded to `data/raw/2026-02-13/investment_reports/`.
5. A manifest is written to `data/raw/2026-02-13/investment_reports_manifest.json`.

Parse already-downloaded PDFs:

```bash
uv run python -m src.ingest.hl_investment_report_pdf_parser --run-date 2026-02-13 --text-dir data/staging/investment_report_text/2026-02-13
```

Build report marts:

```bash
uv run python -m src.marts.build_report_marts --run-date 2026-02-13 --db-path data/marts/hl_portfolio.duckdb
```

Report-flow outputs:

- `data/raw/<RunDate>/investment_reports/`
- `data/raw/<RunDate>/investment_reports_manifest.json`
- `data/staging/investment_reports_extracted_<RunDate>.csv`
- `data/staging/investment_reports_overview_<RunDate>.csv`
- `data/staging/investment_reports_capital_txns_<RunDate>.csv`
- `data/staging/investment_reports_asset_values_<RunDate>.csv`
- `data/staging/investment_report_text/<RunDate>/`
- `data/marts/hl_portfolio.duckdb`

## Automated CSV Fetch

If you prefer not to manually download CSV exports, the site fetcher opens HL for manual login and then attempts to collect export/download links.

```bash
uv run python -m src.fetch.hl_site_fetch --run-date 2026-02-13 --start-url https://online.hl.co.uk/my-accounts/login --max-pages 30 --run-pipeline --db-path data/marts/hl_portfolio.duckdb
```

Downloaded CSVs are saved under `data/raw/<RunDate>/`, with a manifest at `data/raw/<RunDate>/download_manifest.json`.

## Ticker Resolution And Price Fetch

For asset-level report analytics, resolve parsed asset names to Yahoo Finance tickers:

```bash
uv run python -m src.prices.resolve_asset_tickers --run-date 2026-02-13 --db-path data/marts/hl_portfolio.duckdb
```

Then fetch daily prices:

```bash
uv run python -m src.prices.fetch_yfinance_prices --db-path data/marts/hl_portfolio.duckdb
```

When `--start-date` is omitted, the price fetcher backfills each ticker from the earliest mapped asset checkpoint date in `dim_asset.first_seen_date`.

Rebuild report marts after fetching prices so price-shaped gap fill can run:

```bash
uv run python -m src.marts.build_report_marts --run-date 2026-02-13 --db-path data/marts/hl_portfolio.duckdb
```

Disable price gap fill if you only want report checkpoints:

```bash
uv run python -m src.marts.build_report_marts --run-date 2026-02-13 --db-path data/marts/hl_portfolio.duckdb --disable-price-gap-fill
```

Optional manual mappings live in `config/asset_ticker_overrides.yml`.

## Dashboard

Launch the dashboard:

```bash
uv run streamlit run src/presentation/app.py
```

By default, the app reads `data/marts/hl_portfolio.duckdb`. To point it at another database, set `HL_DB_PATH` before launching.

Linux/macOS:

```bash
HL_DB_PATH=data/marts/hl_portfolio_demo.duckdb uv run streamlit run src/presentation/app.py
```

Windows PowerShell:

```powershell
$env:HL_DB_PATH = "data/marts/hl_portfolio_demo.duckdb"
uv run streamlit run src/presentation/app.py
```

Dashboard pages:

- `Overview`: portfolio value vs deposits and corrected P/L.
- `Accounts`: account-specific drilldown.
- `Cashflows`: external and internal flows.
- `Performance`: cumulative TWR, daily return, and drawdown.
- `Assets`: per-asset report checkpoints and single-asset performance.
- `Attribution`: allocation, selection, interaction, and account contribution.
- `Rolling & Concentration`: rolling returns, trailing windows, drawdown recovery, and concentration risk.
- `Cost Drag`: fee, tax, and idle-cash drag.

## Data Contract

Raw CSV input:

- Path pattern: `data/raw/<YYYY-MM-DD>/*.csv`
- Transaction exports and valuation exports can be mixed in one run directory.
- File type is inferred by filename and CSV structure.

Holdings snapshot input:

- Path pattern: `data/raw/<YYYY-MM-DD>/holdings_snapshot*.csv`
- Required columns: `account_name`, `as_of_date`, `asset_name`, `market_value_gbp`
- Optional columns: `account_id`, `isin`, `sedol`, `units`, `currency`, `source_file`

Staging tables:

- `stg_transactions`: classified transaction events and signed amounts.
- `stg_account_value_daily`: daily account close values.
- `stg_account_flow_daily`: aggregated external/internal daily flows.
- `stg_asset_checkpoint`: per-account asset valuation checkpoints.
- `stg_account_cost_daily`: daily fee, tax, and interest components.
- `stg_account_cash_daily`: daily cash balances, observed or proxied.

Marts:

- `mart_account_daily`: account-level corrected P/L and return.
- `mart_portfolio_daily`: portfolio-level corrected P/L and return.
- `mart_asset_daily`: reconstructed daily asset values, returns, and weights.
- `mart_attribution_daily`: allocation, selection, interaction, and residual effects.
- `mart_concentration_daily`: concentration metrics and breach flags.
- `mart_cost_drag_account_daily`: account-level fee, tax, and idle-cash drag.
- `mart_cost_drag_portfolio_daily`: portfolio-level fee, tax, and idle-cash drag.

## Metric Definitions

- Daily corrected P/L: `daily_pnl = V_t - V_(t-1) - external_flow_t`
- Daily corrected return: `daily_return = daily_pnl / (V_(t-1) + 0.5 * external_flow_t)`
- Cumulative return: chain-linked `product(1 + daily_return) - 1`
- Net deposited cash: cumulative external inflows minus external outflows

The daily return uses a Modified Dietz-style daily approximation.

## Configuration

- `config/accounts.yml`: account IDs, display names, account types, wrappers, and name matching.
- `config/txn_type_map.yml`: transaction classification and flow classes.
- `config/asset_ticker_overrides.yml`: optional manual ticker mappings.
- `config/benchmark.yml`: benchmark ticker/provider for dashboard context.
- `config/attribution_policy.yml`: optional account policy weights and coverage threshold.
- `config/risk_limits.yml`: concentration warning and critical thresholds.
- `config/cost_drag.yml`: idle-cash benchmark assumptions.

## Optional PowerShell Wrappers

The project still includes PowerShell wrappers for convenience, but they are not required when using `uv`.

- `scripts/run_demo.ps1`: demo import, mart build, and quality report.
- `scripts/run_all.ps1`: CSV import, mart build, and quality report.
- `scripts/fetch_hl_exports.ps1`: automated CSV fetch plus optional pipeline run.
- `scripts/fetch_investment_reports.ps1`: PDF fetch, parse, and report mart build.
- `scripts/fetch_prices.ps1`: ticker resolution and yfinance price fetch.

Example:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_demo.ps1
```

## Troubleshooting

- Missing `mart_asset_daily` rows on CSV runs: include `holdings_snapshot*.csv` files under `data/raw/<RunDate>/`.
- Missing benchmark context: set `config/benchmark.yml`, resolve/fetch prices, then rebuild report marts.
- Cost drag shows proxy cash quality: report-mode cash is estimated from parsed capital/income account balances when explicit valuation cash is unavailable.
- No Investment Report PDFs downloaded: confirm the browser reached the authenticated account area and that HL exposes report links for the account history page.
- Empty dashboard: confirm `HL_DB_PATH` points to an existing DuckDB file and that marts were built.

## Extending Analyses

To add a new analysis:

1. Add or adjust classification rules in `config/txn_type_map.yml`.
2. Add metric logic in `src/metrics/`.
3. Materialize new tables in `src/marts/` or `sql/`.
4. Add a dashboard page in `src/presentation/pages/`.
5. Add focused regression tests in `tests/`.
