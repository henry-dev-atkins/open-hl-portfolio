from __future__ import annotations

import pandas as pd

from src.presentation.asset_series import build_yfinance_shaped_asset_series


def test_build_yfinance_shaped_asset_series_is_anchored_and_non_flat() -> None:
    checkpoints = pd.DataFrame(
        {
            "report_date": [pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-05")],
            "value_gbp": [100.0, 100.0],
        }
    )
    prices = pd.DataFrame(
        {
            "d": [
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-01-02"),
                pd.Timestamp("2020-01-03"),
                pd.Timestamp("2020-01-04"),
                pd.Timestamp("2020-01-05"),
            ],
            "px": [100.0, 110.0, 95.0, 105.0, 100.0],
        }
    )

    out = build_yfinance_shaped_asset_series(checkpoints=checkpoints, price_history=prices)

    assert len(out) == 5
    assert float(out.iloc[0]["value_gbp"]) == 100.0
    assert float(out.iloc[-1]["value_gbp"]) == 100.0
    assert out["value_gbp"].nunique() > 2
