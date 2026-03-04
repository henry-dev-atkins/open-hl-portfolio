from __future__ import annotations

import pandas as pd


def account_summary(account_daily_df: pd.DataFrame) -> pd.DataFrame:
    if account_daily_df.empty:
        return pd.DataFrame(
            columns=[
                "account_id",
                "as_of_date",
                "close_value_gbp",
                "net_deposited_external_to_date_gbp",
                "cumulative_gain_vs_external_deposits_gbp",
                "cumulative_twr",
            ]
        )
    latest = (
        account_daily_df.sort_values("d")
        .groupby("account_id", as_index=False)
        .tail(1)
        .rename(columns={"d": "as_of_date"})
    )
    return latest[
        [
            "account_id",
            "as_of_date",
            "close_value_gbp",
            "net_deposited_external_to_date_gbp",
            "cumulative_gain_vs_external_deposits_gbp",
            "cumulative_twr",
        ]
    ].sort_values("account_id")


def portfolio_summary(portfolio_daily_df: pd.DataFrame) -> pd.DataFrame:
    if portfolio_daily_df.empty:
        return pd.DataFrame(
            columns=[
                "as_of_date",
                "close_value_gbp",
                "net_deposited_external_to_date_gbp",
                "cumulative_twr",
                "cumulative_gain_vs_external_deposits_gbp",
            ]
        )
    row = portfolio_daily_df.sort_values("d").tail(1).copy()
    row["as_of_date"] = row["d"]
    row["cumulative_gain_vs_external_deposits_gbp"] = (
        row["close_value_gbp"] - row["net_deposited_external_to_date_gbp"]
    )
    return row[
        [
            "as_of_date",
            "close_value_gbp",
            "net_deposited_external_to_date_gbp",
            "cumulative_twr",
            "cumulative_gain_vs_external_deposits_gbp",
        ]
    ]
