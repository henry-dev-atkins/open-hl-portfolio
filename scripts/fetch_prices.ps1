param(
  [string]$RunDate = (Get-Date -Format "yyyy-MM-dd"),
  [string]$DbPath = "data/marts/hl_portfolio.duckdb",
  [string]$DisableAutoSearch = "false"
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

$DisableAutoSearchFlag = Convert-ToBool -Value $DisableAutoSearch -Name "DisableAutoSearch"

if ($DisableAutoSearchFlag) {
  python -m src.prices.resolve_asset_tickers --run-date $RunDate --db-path $DbPath --disable-auto-search
}
else {
  python -m src.prices.resolve_asset_tickers --run-date $RunDate --db-path $DbPath
}
if ($LASTEXITCODE -ne 0) {
  throw "Ticker resolution failed."
}

python -m src.prices.fetch_yfinance_prices --db-path $DbPath
if ($LASTEXITCODE -ne 0) {
  throw "Yahoo Finance price fetch failed."
}

Write-Host "Price flow completed for RunDate=$RunDate"
