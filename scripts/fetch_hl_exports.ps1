param(
  [string]$RunDate = (Get-Date -Format "yyyy-MM-dd"),
  [string]$StartUrl = "https://online.hl.co.uk/my-accounts/login",
  [int]$MaxPages = 30,
  [switch]$RunPipeline = $true,
  [string]$DbPath = "data/marts/hl_portfolio.duckdb"
)

$ErrorActionPreference = "Stop"

python -m src.fetch.hl_site_fetch `
  --run-date $RunDate `
  --start-url $StartUrl `
  --max-pages $MaxPages `
  $(if ($RunPipeline) { "--run-pipeline" }) `
  --db-path $DbPath

if ($LASTEXITCODE -ne 0) {
  throw "HL fetch script failed."
}

Write-Host "Fetch flow completed for RunDate=$RunDate"
