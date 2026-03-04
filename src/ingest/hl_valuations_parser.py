from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.parse_utils import coerce_date, coerce_decimal, load_csv_flexible, pick_column


def parse_valuations_file(path: Path, run_id: str, source_file: str | None = None) -> pd.DataFrame:
    raw_df = load_csv_flexible(path)
    if raw_df.empty:
        raise ValueError(f"Valuation file is empty: {path}")

    account_name = pick_column(raw_df, ["account", "account_name", "portfolio"], required=True)
    account_id = pick_column(raw_df, ["account_id", "account_number"], required=False)
    valuation_date = pick_column(raw_df, ["date", "valuation_date", "as_of_date"], required=True)
    total_value = pick_column(
        raw_df,
        ["total_value", "portfolio_value", "valuation", "value_gbp", "total"],
        required=True,
    )
    cash_value = pick_column(raw_df, ["cash_value", "cash"], required=False)
    invested_value = pick_column(
        raw_df,
        ["invested_value", "investments_value", "holdings_value"],
        required=False,
    )

    out = pd.DataFrame(
        {
            "run_id": run_id,
            "source_file": source_file or path.name,
            "row_num": range(1, len(raw_df) + 1),
            "account_name_raw": account_name.astype(str).str.strip(),
            "account_id": account_id.astype(str).str.strip() if account_id is not None else None,
            "valuation_date": coerce_date(valuation_date),
            "total_value_gbp": coerce_decimal(total_value),
            "cash_value_gbp": coerce_decimal(cash_value) if cash_value is not None else None,
            "invested_value_gbp": coerce_decimal(invested_value) if invested_value is not None else None,
        }
    )

    out = out[out["valuation_date"].notna() & out["total_value_gbp"].notna()].copy()
    if out.empty:
        raise ValueError(f"No valid valuation rows found in {path}")

    return out[
        [
            "run_id",
            "source_file",
            "row_num",
            "account_name_raw",
            "account_id",
            "valuation_date",
            "total_value_gbp",
            "cash_value_gbp",
            "invested_value_gbp",
        ]
    ]
