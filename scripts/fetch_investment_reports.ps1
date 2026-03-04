param(
  [string]$RunDate = (Get-Date -Format "yyyy-MM-dd"),
  [string]$FetchReports = "true",
  [string]$ParseReports = "true",
  [string]$BuildMarts = "true",
  [string]$DbPath = "data/marts/hl_portfolio.duckdb"
)

$ErrorActionPreference = "Stop"

function Convert-ToBool {
  param(
    [string]$Value,
    [string]$Name
  )
  if ($null -eq $Value) {
    $Value = ""
  }
  $normalized = $Value.Trim().TrimStart('$').ToLowerInvariant()
  switch ($normalized) {
    "true" { return $true }
    "false" { return $false }
    "1" { return $true }
    "0" { return $false }
    default {
      throw ("Invalid value for -{0}: '{1}'. Use true|false or 1|0." -f $Name, $Value)
    }
  }
}

$FetchReportsFlag = Convert-ToBool -Value $FetchReports -Name "FetchReports"
$ParseReportsFlag = Convert-ToBool -Value $ParseReports -Name "ParseReports"
$BuildMartsFlag = Convert-ToBool -Value $BuildMarts -Name "BuildMarts"

if ($FetchReportsFlag) {
  python -m src.fetch.hl_investment_reports_fetch --run-date $RunDate
  if ($LASTEXITCODE -ne 0) {
    throw "Investment report fetch failed."
  }
}

if ($ParseReportsFlag) {
  python -m src.ingest.hl_investment_report_pdf_parser --run-date $RunDate --text-dir "data/staging/investment_report_text/$RunDate"
  if ($LASTEXITCODE -ne 0) {
    throw "Investment report parse failed."
  }
}

if ($BuildMartsFlag) {
  python -m src.marts.build_report_marts --run-date $RunDate --db-path $DbPath
  if ($LASTEXITCODE -ne 0) {
    throw "Report marts build failed."
  }
}

Write-Host "Investment report flow completed for RunDate=$RunDate"
Write-Host "Advanced marts refreshed: mart_asset_daily, mart_attribution_daily, mart_concentration_daily, mart_cost_drag_*"
