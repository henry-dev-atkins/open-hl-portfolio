param(
  [string]$RunDate = "2026-01-03",
  [string]$DemoDataDir = "examples/demo_data",
  [string]$DbPath = "data/marts/hl_portfolio_demo.duckdb",
  [string]$RunId = "DEMO_RUN_2026_01_03"
)

$ErrorActionPreference = "Stop"

$inputDir = Join-Path $DemoDataDir "raw\$RunDate"
if (-not (Test-Path $inputDir)) {
  throw "Demo input directory not found: $inputDir"
}

$qualityPath = "data/marts/quality_report_$RunId.json"

Write-Host "Running sanitized demo pipeline with RunId=$RunId RunDate=$RunDate"

python -m src.ingest.run_import `
  --input-dir $inputDir `
  --db-path $DbPath `
  --run-id $RunId `
  --notes "Sanitized demo dataset"
if ($LASTEXITCODE -ne 0) { throw "Demo import step failed." }

python -m src.marts.build_marts --db-path $DbPath --run-id $RunId
if ($LASTEXITCODE -ne 0) { throw "Demo mart build step failed." }

python -m src.quality.reconciliation --db-path $DbPath --run-id $RunId --output-json $qualityPath
if ($LASTEXITCODE -ne 0) { throw "Demo quality step failed." }

Write-Host "Demo pipeline complete. DB=$DbPath QualityReport=$qualityPath"
Write-Host "To inspect the demo dashboard:"
Write-Host '  $env:HL_DB_PATH = "data/marts/hl_portfolio_demo.duckdb"'
Write-Host "  streamlit run src/presentation/app.py"
