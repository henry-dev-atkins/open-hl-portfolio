from __future__ import annotations

import pandas as pd

from src.metrics.concentration import compute_concentration_daily


def test_concentration_metrics_and_breaches() -> None:
    asset_daily = pd.DataFrame(
        [
            {"account_id": "ISA", "asset_id": "A", "d": "2026-01-01", "value_gbp": 50.0, "weight": 0.5},
            {"account_id": "ISA", "asset_id": "B", "d": "2026-01-01", "value_gbp": 30.0, "weight": 0.3},
            {"account_id": "ISA", "asset_id": "C", "d": "2026-01-01", "value_gbp": 20.0, "weight": 0.2},
        ]
    )
    out = compute_concentration_daily(asset_daily_df=asset_daily)
    assert not out.empty

    portfolio = out[(out["scope_type"] == "portfolio") & (out["scope_id"] == "PORTFOLIO")].iloc[0]
    assert round(float(portfolio["max_single_weight"]), 6) == 0.5
    assert round(float(portfolio["top5_weight"]), 6) == 1.0
    assert round(float(portfolio["hhi"]), 6) == round(0.25 + 0.09 + 0.04, 6)
    assert portfolio["breach_hhi"] == "critical"
