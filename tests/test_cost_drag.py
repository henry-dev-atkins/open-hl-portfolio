from __future__ import annotations

import pandas as pd

from src.metrics.cost_drag import compute_cost_drag_tables


def test_compute_cost_drag_tables() -> None:
    account_daily = pd.DataFrame(
        [
            {"account_id": "ISA", "d": "2026-01-01"},
            {"account_id": "ISA", "d": "2026-01-02"},
        ]
    )
    account_cost = pd.DataFrame(
        [
            {"account_id": "ISA", "d": "2026-01-01", "fee_gbp": -2.0, "tax_gbp": -1.0, "cash_interest_gbp": 0.0},
            {"account_id": "ISA", "d": "2026-01-02", "fee_gbp": 0.0, "tax_gbp": 0.0, "cash_interest_gbp": 0.0},
        ]
    )
    account_cash = pd.DataFrame(
        [
            {"account_id": "ISA", "d": "2026-01-01", "cash_balance_gbp": 1000.0, "source_quality": "observed"},
            {"account_id": "ISA", "d": "2026-01-02", "cash_balance_gbp": 1000.0, "source_quality": "observed"},
        ]
    )

    account_out, portfolio_out = compute_cost_drag_tables(
        account_daily_df=account_daily,
        account_cost_df=account_cost,
        account_cash_df=account_cash,
        annual_cash_rate=0.365,
        periods_per_year=365,
        source_run_id="RUN_1",
    )
    assert not account_out.empty
    day1 = account_out[account_out["d"] == pd.Timestamp("2026-01-01").date()].iloc[0]
    day2 = account_out[account_out["d"] == pd.Timestamp("2026-01-02").date()].iloc[0]

    assert float(day1["fee_drag_gbp"]) == 2.0
    assert float(day1["tax_drag_gbp"]) == 1.0
    assert float(day1["idle_cash_drag_gbp"]) == 0.0
    assert float(day2["cash_balance_prev_gbp"]) == 1000.0
    assert float(day2["idle_cash_drag_gbp"]) > 0.0
    assert len(portfolio_out) == 2
