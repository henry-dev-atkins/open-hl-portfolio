import math

import pandas as pd

from src.metrics.performance import (
    compute_account_daily_metrics,
    compute_portfolio_daily_metrics,
    compute_trading_metrics,
)


def test_daily_flow_corrected_return_with_deposit() -> None:
    value_df = pd.DataFrame(
        {
            "account_id": ["ACC1", "ACC1"],
            "d": [pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-02")],
            "close_value_gbp": [1000.0, 1110.0],
        }
    )
    flow_df = pd.DataFrame(
        {
            "account_id": ["ACC1"],
            "d": [pd.Timestamp("2026-01-02")],
            "external_flow_gbp": [100.0],
            "internal_flow_gbp": [0.0],
        }
    )

    out = compute_account_daily_metrics(value_df=value_df, flow_df=flow_df)
    day2 = out[out["d"] == pd.Timestamp("2026-01-02").date()].iloc[0]

    # P/L corrected for 100 deposit is 10.
    assert round(float(day2["daily_pnl_flow_corrected_gbp"]), 6) == 10.0

    # Modified Dietz denominator = 1000 + 0.5*100 = 1050.
    expected_return = 10.0 / 1050.0
    assert abs(float(day2["daily_return_flow_corrected"]) - expected_return) < 1e-9


def test_portfolio_aggregation() -> None:
    account_daily = pd.DataFrame(
        {
            "account_id": ["A", "B", "A", "B"],
            "d": [
                pd.Timestamp("2026-01-01").date(),
                pd.Timestamp("2026-01-01").date(),
                pd.Timestamp("2026-01-02").date(),
                pd.Timestamp("2026-01-02").date(),
            ],
            "close_value_gbp": [500.0, 500.0, 560.0, 560.0],
            "external_flow_gbp": [0.0, 0.0, 50.0, 50.0],
            "internal_flow_gbp": [0.0, 0.0, 0.0, 0.0],
            "net_deposited_external_to_date_gbp": [0.0, 0.0, 50.0, 50.0],
            "daily_pnl_flow_corrected_gbp": [0.0, 0.0, 10.0, 10.0],
            "daily_return_flow_corrected": [0.0, 0.0, 10.0 / 525.0, 10.0 / 525.0],
            "cumulative_twr": [0.0, 0.0, 10.0 / 525.0, 10.0 / 525.0],
            "cumulative_gain_vs_external_deposits_gbp": [500.0, 500.0, 510.0, 510.0],
        }
    )
    p = compute_portfolio_daily_metrics(account_daily_df=account_daily)
    day2 = p[p["d"] == pd.Timestamp("2026-01-02").date()].iloc[0]
    assert round(float(day2["close_value_gbp"]), 2) == 1120.0
    assert round(float(day2["external_flow_gbp"]), 2) == 100.0


def test_trading_metrics_from_daily_flow_corrected_returns() -> None:
    returns = pd.Series([0.10, -0.05, 0.00, 0.15, -0.10])
    metrics = compute_trading_metrics(returns)

    assert round(float(metrics["avg_return"]), 6) == 0.02
    assert round(float(metrics["avg_win"]), 6) == 0.125
    assert round(float(metrics["avg_loss"]), 6) == -0.075
    assert round(float(metrics["win_rate"]), 6) == 0.5
    assert round(float(metrics["win_loss_ratio"]), 6) == 1.0
    assert round(float(metrics["payoff_ratio"]), 6) == round(0.125 / 0.075, 6)
    assert round(float(metrics["profit_factor"]), 6) == round(0.25 / 0.15, 6)
    assert round(float(metrics["omega_ratio"]), 6) == round(0.25 / 0.15, 6)
    assert round(float(metrics["kelly_criterion"]), 6) == 0.2
    assert round(float(metrics["expected_return"]), 6) == 0.025

    assert round(float(metrics["volatility"]), 6) == round(0.1036822068, 6)
    assert round(float(metrics["implied_volatility"]), 6) == round(0.1036822068 * (252**0.5), 6)
    assert math.isfinite(float(metrics["sharpe_ratio"]))
    assert math.isfinite(float(metrics["sortino_ratio"]))
    assert math.isfinite(float(metrics["information_ratio"]))


def test_trading_metrics_handles_no_losses() -> None:
    returns = pd.Series([0.01, 0.02, 0.00])
    metrics = compute_trading_metrics(returns)

    assert float(metrics["win_rate"]) == 1.0
    assert math.isinf(float(metrics["win_loss_ratio"]))
    assert math.isinf(float(metrics["payoff_ratio"]))
    assert math.isinf(float(metrics["profit_factor"]))
    assert float(metrics["kelly_criterion"]) == 1.0
