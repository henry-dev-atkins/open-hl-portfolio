from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.parse_utils import coerce_date, coerce_decimal, load_csv_flexible, pick_column


def parse_holdings_snapshot_file(path: Path, run_id: str, source_file: str | None = None) -> pd.DataFrame:
    raw_df = load_csv_flexible(path)
    if raw_df.empty:
        raise ValueError(f"Holdings snapshot file is empty: {path}")

    account_name = pick_column(raw_df, ["account_name", "account", "portfolio"], required=True)
    account_id = pick_column(raw_df, ["account_id", "account_number"], required=False)
    as_of_date = pick_column(raw_df, ["as_of_date", "date", "valuation_date", "snapshot_date"], required=True)
    asset_name = pick_column(raw_df, ["asset_name", "asset", "holding", "instrument_name", "security"], required=True)
    market_value = pick_column(
        raw_df,
        ["market_value_gbp", "value_gbp", "market_value", "holding_value", "value"],
        required=True,
    )
    isin = pick_column(raw_df, ["isin"], required=False)
    sedol = pick_column(raw_df, ["sedol"], required=False)
    units = pick_column(raw_df, ["units", "quantity"], required=False)
    currency = pick_column(raw_df, ["currency"], required=False)
    source_file_col = pick_column(raw_df, ["source_file"], required=False)

    out = pd.DataFrame(
        {
            "run_id": run_id,
            "source_file": (
                source_file_col.astype(str).str.strip()
                if source_file_col is not None
                else (source_file or path.name)
            ),
            "row_num": range(1, len(raw_df) + 1),
            "account_name_raw": account_name.astype(str).str.strip(),
            "account_id": account_id.astype(str).str.strip() if account_id is not None else None,
            "as_of_date": coerce_date(as_of_date),
            "asset_name": asset_name.astype(str).str.strip(),
            "market_value_gbp": coerce_decimal(market_value),
            "isin": isin.astype(str).str.strip() if isin is not None else None,
            "sedol": sedol.astype(str).str.strip() if sedol is not None else None,
            "units": coerce_decimal(units) if units is not None else None,
            "currency": currency.astype(str).str.strip() if currency is not None else None,
        }
    )

    out = out[
        out["as_of_date"].notna()
        & out["asset_name"].notna()
        & (out["asset_name"].astype(str).str.len() > 0)
        & out["market_value_gbp"].notna()
    ].copy()
    if out.empty:
        raise ValueError(f"No valid holdings snapshot rows found in {path}")

    return out[
        [
            "run_id",
            "source_file",
            "row_num",
            "account_name_raw",
            "account_id",
            "as_of_date",
            "asset_name",
            "market_value_gbp",
            "isin",
            "sedol",
            "units",
            "currency",
        ]
    ]
