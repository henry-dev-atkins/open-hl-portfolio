param(
  [string]$RunDate = (Get-Date -Format "yyyy-MM-dd"),
  [string]$DataDir = "data",
  [string]$DbPath = "data/marts/hl_portfolio.duckdb",
  [string]$RunId = "",
  [string]$Notes = ""
)

$ErrorActionPreference = "Stop"

$inputDir = Join-Path $DataDir "raw\$RunDate"
if (-not (Test-Path $inputDir)) {
  throw "Input directory not found: $inputDir"
}

if ([string]::IsNullOrWhiteSpace($RunId)) {
  $RunId = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
}

Write-Host "Running HL pipeline with RunId=$RunId RunDate=$RunDate"
if ([string]::IsNullOrWhiteSpace($Notes)) {
  python -m src.ingest.run_import --input-dir $inputDir --db-path $DbPath --run-id $RunId
}
else {
  python -m src.ingest.run_import --input-dir $inputDir --db-path $DbPath --run-id $RunId --notes $Notes
}
if ($LASTEXITCODE -ne 0) { throw "Import step failed." }

python -m src.marts.build_marts --db-path $DbPath --run-id $RunId
if ($LASTEXITCODE -ne 0) { throw "Mart build step failed." }

$qualityPath = Join-Path $DataDir "marts\quality_report_$RunId.json"
python -m src.quality.reconciliation --db-path $DbPath --run-id $RunId --output-json $qualityPath
if ($LASTEXITCODE -ne 0) { throw "Quality step failed." }

Write-Host "Pipeline complete. DB=$DbPath QualityReport=$qualityPath"
Write-Host "Advanced marts refreshed: mart_asset_daily, mart_attribution_daily, mart_concentration_daily, mart_cost_drag_*"
