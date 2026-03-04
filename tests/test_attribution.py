from __future__ import annotations

import pandas as pd

from src.metrics.attribution import compute_attribution_daily
from src.prices.asset_identity import canonicalize_asset_name, make_asset_id


def test_attribution_identity_holds() -> None:
    asset_id = make_asset_id(canonicalize_asset_name("Asset One"))
    asset_daily = pd.DataFrame(
        [
            {"account_id": "ISA", "asset_id": asset_id, "d": "2026-01-01", "value_gbp": 100.0, "daily_return": 0.0},
            {"account_id": "ISA", "asset_id": asset_id, "d": "2026-01-02", "value_gbp": 110.0, "daily_return": 0.10},
            {"account_id": "ISA", "asset_id": asset_id, "d": "2026-01-03", "value_gbp": 104.5, "daily_return": -0.05},
        ]
    )
    account_daily = pd.DataFrame(
        [
            {"account_id": "ISA", "d": "2026-01-01", "close_value_gbp": 100.0},
            {"account_id": "ISA", "d": "2026-01-02", "close_value_gbp": 110.0},
            {"account_id": "ISA", "d": "2026-01-03", "close_value_gbp": 104.5},
        ]
    )
    portfolio_daily = pd.DataFrame(
        [
            {"d": "2026-01-01", "daily_return_flow_corrected": 0.0},
            {"d": "2026-01-02", "daily_return_flow_corrected": 0.10},
            {"d": "2026-01-03", "daily_return_flow_corrected": -0.05},
        ]
    )

    out = compute_attribution_daily(
        asset_daily_df=asset_daily,
        account_daily_df=account_daily,
        portfolio_daily_df=portfolio_daily,
    )
    assert not out.empty
    for _, row in out.iterrows():
        lhs = float(row["portfolio_return"])
        rhs = float(
            row["benchmark_return"]
            + row["allocation_effect"]
            + row["selection_effect"]
            + row["interaction_effect"]
            + row["residual_effect"]
        )
        assert abs(lhs - rhs) < 1e-10
