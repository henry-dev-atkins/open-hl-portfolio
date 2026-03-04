# HL Portfolio Analytics

Hargreaves Lansdown has a *terrible* portfolio analysis tool with no tracking of deposits vs performance, no correlation or optimisation methods and no cost-performance breakdown. 
I built this tool to analyse my account and hopefully some other people may find this useful as well. 

Flow-corrected account and portfolio analytics for Hargreaves Lansdown exports.

# Security Note. 
*I take no responsibility for anything - use at your own risk.* That said, I personally use this.
Never share private information, especially in version controlled environments like GitHub. 
Therefore, this tool will never store, save, cache or otherwise interact with your login/account details.

It does:
- Allows *you* to login to the site independent to the tool,
  - Once you are logged in the tool gets to work.
- Gathers your account's investment reports,
- Saves them to your computer,
- Analyses them and displays the result,
- Stores the investment reports in your `Downloads/...` folder.

It has significant risks from (These are Issues I am working on):
- SQL injection (fixing is a To Do),
- Dependency vulnerabilities (partially mitigated by pinned versions in the `uv`/toml file),
- Playwright installation security (To Do),
- Accidental exposure of generated artefacts (see the `.gitignore` and `docs/privacy_and_redaction.md`).

## Capabilities

- Gather your investment report csv's from the website,
- Parse them into a functioning table,
- Query yahoo-finance for daily valuations with intelligent name searching,
- Human edits of the yahoo-finance/HL ticker mapping,
- Builds normalized staging tables in a database,
- Serves a Streamlit dashboard for interactive review


## Safe Demo Run

The fastest way to evaluate the repo without using personal account data is to run the
sanitized demo dataset in [`examples/demo_data/`](examples/demo_data/README.md):

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_demo.ps1
$env:HL_DB_PATH = "data/marts/hl_portfolio_demo.duckdb"
streamlit run src/presentation/app.py
```

Outputs:

- Demo DB: `data/marts/hl_portfolio_demo.duckdb`
- Demo quality report: `data/marts/quality_report_DEMO_RUN_2026_01_03.json`

The demo files are synthetic and safe to inspect, modify, and share.

## Quick Start With Your Own Data

This repo is PowerShell-first for the scripted workflows below. The core Python package
and tests do not require HL credentials.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -e .[dev]
python -m playwright install chromium
```

Place HL exports into `data/raw/YYYY-MM-DD/` and run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_all.ps1 -RunDate 2026-02-13
```

Optional (for full asset-level analytics on CSV runs): include one or more
holdings snapshot files matching `holdings_snapshot*.csv` in `data/raw/YYYY-MM-DD/`.

Launch dashboard:

```powershell
streamlit run src/presentation/app.py
```

If you want the dashboard to point at a non-default database, set `HL_DB_PATH` first:

```powershell
$env:HL_DB_PATH = "data/marts/hl_portfolio.duckdb"
streamlit run src/presentation/app.py
```

## Automated Site Fetch (No Credential Storage)

If you do not want to manually download CSV files, use the fetcher:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\fetch_hl_exports.ps1 -RunDate 2026-02-13
```

What happens:

1. A browser opens on HL login.
2. You type credentials and MFA manually.
3. Return to terminal and press Enter.
4. Script crawls likely account/history pages and clicks download/export controls.
5. CSVs are saved into `data/raw/<RunDate>/`.
6. Pipeline runs automatically (unless `-RunPipeline:$false`).

Manifest output:

- `data/raw/<RunDate>/download_manifest.json`

## HL Investment Reports Flow (PDF-Based)

HL portfolio history may expose report links as PDFs rather than clean CSV endpoints.
Use this flow to download all `Investment Report` PDFs and parse them into a structured CSV:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\fetch_investment_reports.ps1 -RunDate 2026-02-13
```

Script options:

- `-FetchReports true|false` (set `false` to skip login/download and reuse existing PDFs)
- `-ParseReports true|false`
- `-BuildMarts true|false`
- `-DbPath <duckdb path>`

Parse/build from already-downloaded PDFs only:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\fetch_investment_reports.ps1 -RunDate 2026-02-13 -FetchReports false -ParseReports true -BuildMarts true
```

Outputs:

- PDFs: `data/raw/<RunDate>/investment_reports/`
- Fetch manifest: `data/raw/<RunDate>/investment_reports_manifest.json`
- Parsed metrics CSV: `data/staging/investment_reports_extracted_<RunDate>.csv`
- Parsed overview checkpoints CSV: `data/staging/investment_reports_overview_<RunDate>.csv`
- Parsed capital transactions CSV: `data/staging/investment_reports_capital_txns_<RunDate>.csv`
- Parsed asset valuation checkpoints CSV: `data/staging/investment_reports_asset_values_<RunDate>.csv`
- Unresolved ticker queue CSV: `data/staging/unresolved_assets_<RunDate>.csv`
- Unresolved candidate ticker CSV: `data/staging/unresolved_ticker_candidates_<RunDate>.csv`
- Optional extracted text: `data/staging/investment_report_text/<RunDate>/`
- Mart refresh from report datasets:
  - `mart_account_daily`
  - `mart_portfolio_daily`
  - `mart_asset_daily`
  - `mart_attribution_daily`
  - `mart_concentration_daily`
  - `mart_cost_drag_account_daily`
  - `mart_cost_drag_portfolio_daily`
  - By default, `src.marts.build_report_marts` uses yfinance price shape to gap-fill daily
    valuations between report checkpoints (disable with `--disable-price-gap-fill`).

## Notes

- Credentials are never captured or stored by this project.
- Manual login/export workflow is assumed.
- Transaction type mappings are configurable in `config/txn_type_map.yml`.

## Privacy And Redaction

Treat HL exports, investment reports, screenshots, and generated DuckDB files as personal
financial records.

- Do not commit anything from `data/raw/`, `data/staging/`, or `data/marts/`.
- Do not post screenshots, PDFs, or issue attachments containing account numbers, balances,
  transaction history, or broker-specific identifiers.
- Prefer the sanitized demo dataset in `examples/demo_data/` for bug reports,
  reproductions, and documentation examples.
- If you must share a real export privately, remove names, account numbers, balances,
  addresses, and any row that is not required to reproduce the bug first.

See [`docs/privacy_and_redaction.md`](docs/privacy_and_redaction.md) for a short checklist.

## YFinance Ticker Resolution and Price Fetch

Resolve report asset names to Yahoo tickers (with HL/Yahoo validation links):

```powershell
python -m src.prices.resolve_asset_tickers --run-date 2026-02-13 --db-path data/marts/hl_portfolio.duckdb
```

Fetch daily Yahoo prices for resolved tickers:

```powershell
python -m src.prices.fetch_yfinance_prices --db-path data/marts/hl_portfolio.duckdb
```

When `--start-date` is omitted, the fetch step now auto-backfills each ticker from the
earliest mapped asset checkpoint date in `dim_asset.first_seen_date`.

Or run both steps with one script:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\fetch_prices.ps1 -RunDate 2026-02-13
```

Recommended report valuation sequence:

1. Parse investment reports to generate overview/capital/asset CSVs.
2. Resolve asset tickers.
3. Fetch yfinance prices.
4. Build report marts (gap-fill enabled by default):

```powershell
python -m src.marts.build_report_marts --run-date 2026-02-13 --db-path data/marts/hl_portfolio.duckdb
```

Manual override config (optional):

- `config/asset_ticker_overrides.yml`

Link fields stored in mapping dataset:

- `yf_quote_url`
- `yf_history_url`
- `hl_security_url` (when known)
- `hl_search_url` (always populated fallback)

## Data Contract

### Input (Raw)

- `data/raw/<YYYY-MM-DD>/*.csv`
- Transaction exports and valuation exports can be mixed in one run directory.
- File type is inferred by filename and CSV structure.

### Holdings Snapshot Contract (CSV Mode Asset Rebuild)

Path pattern:

- `data/raw/<YYYY-MM-DD>/holdings_snapshot*.csv`

Required columns:

- `account_name`
- `as_of_date`
- `asset_name`
- `market_value_gbp`

Optional columns:

- `account_id`
- `isin`
- `sedol`
- `units`
- `currency`
- `source_file`

### Staging (DuckDB)

- `stg_transactions`: classified transaction events and signed amounts
- `stg_account_value_daily`: daily account close values
- `stg_account_flow_daily`: aggregated external/internal daily flows
- `stg_asset_checkpoint`: per-account asset valuation checkpoints
- `stg_account_cost_daily`: daily fee/tax/interest components
- `stg_account_cash_daily`: daily cash balances (observed or proxy)

### Marts (DuckDB)

- `mart_account_daily`: account-level daily corrected P/L and return
- `mart_portfolio_daily`: portfolio-level daily corrected P/L and return
- `mart_asset_daily`: reconstructed daily asset values, returns, and weights
- `mart_attribution_daily`: daily allocation/selection/interaction decomposition
- `mart_concentration_daily`: concentration metrics and breach flags
- `mart_cost_drag_account_daily`: account-level fee/tax/idle-cash drag
- `mart_cost_drag_portfolio_daily`: portfolio-level fee/tax/idle-cash drag

## Metric Definitions

- Daily corrected P/L:
  - `daily_pnl = V_t - V_(t-1) - external_flow_t`
- Daily corrected return (Modified Dietz daily approximation):
  - `daily_return = daily_pnl / (V_(t-1) + 0.5 * external_flow_t)`
- Cumulative return:
  - chain-linked `product(1 + daily_return) - 1`
- Net deposited cash:
  - cumulative sum of external inflows minus external outflows

## One-Command Pipeline

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_all.ps1 -RunDate 2026-02-13
```

Options:

- `-RunDate`: folder under `data/raw/`
- `-RunId`: explicit run identifier (optional)
- `-DbPath`: DuckDB file path (optional)
- `-Notes`: import note stored in `raw_import_runs` (optional)

## Dashboard

```powershell
streamlit run src/presentation/app.py
```

Pages:

- `Overview`: portfolio value vs deposits and corrected P/L
- `Accounts`: account-specific drilldown
- `Cashflows`: external/internal flows
- `Performance`: cumulative TWR, daily return, drawdown
- `Assets`: per-asset report checkpoints and single-asset performance over time
- `Attribution`: allocation vs selection decomposition and account contribution drilldown
- `Rolling & Concentration`: rolling 6M/1Y returns, worst trailing windows, drawdown recovery, concentration risk
- `Cost Drag`: fee/tax/idle-cash drag decomposition with cash source quality

## Advanced Analytics Config

- `config/benchmark.yml`: benchmark ticker/provider for contextual benchmark returns
- `config/attribution_policy.yml`: optional account policy weights and coverage threshold
- `config/risk_limits.yml`: concentration warning/critical thresholds
- `config/cost_drag.yml`: idle-cash benchmark annual rate and compounding frequency

## Troubleshooting

- Missing `mart_asset_daily` rows on CSV runs:
  - Ensure `holdings_snapshot*.csv` files exist under `data/raw/<RunDate>/`.
- Missing benchmark context:
  - Set `config/benchmark.yml` and run `scripts/fetch_prices.ps1` (or `src.prices.fetch_yfinance_prices`).
- Cost drag shows proxy cash quality:
  - Report-mode cash is estimated from parsed capital/income account balances when explicit valuation cash is unavailable.

## Extending Analyses

Add new analysis with minimal coupling:

1. Add/adjust classification rules in `config/txn_type_map.yml`.
2. Add new metric logic in `src/metrics/`.
3. Materialize new tables in `src/marts/` or `sql/`.
4. Add new dashboard page in `src/presentation/pages/`.
5. Add regression tests in `tests/`.
