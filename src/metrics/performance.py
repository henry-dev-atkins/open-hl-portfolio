from __future__ import annotations

import numpy as np
import pandas as pd

TRADING_METRIC_KEYS = [
    "sharpe_ratio",
    "sortino_ratio",
    "omega_ratio",
    "payoff_ratio",
    "profit_factor",
    "kelly_criterion",
    "expected_return",
    "avg_loss",
    "avg_return",
    "avg_win",
    "implied_volatility",
    "information_ratio",
    "volatility",
    "win_loss_ratio",
    "win_rate",
]


def _empty_trading_metrics() -> dict[str, float]:
    return {key: np.nan for key in TRADING_METRIC_KEYS}


def _safe_ratio(numerator: float, denominator: float) -> float:
    if abs(denominator) < 1e-12:
        if abs(numerator) < 1e-12:
            return np.nan
        return np.inf if numerator > 0.0 else -np.inf
    return numerator / denominator


def _ensure_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def _compute_perf_frame(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    frame = frame.sort_values("d").reset_index(drop=True)
    frame["prev_close_value"] = frame["close_value_gbp"].shift(1)
    frame["daily_pnl_flow_corrected_gbp"] = np.where(
        frame["prev_close_value"].isna(),
        0.0,
        frame["close_value_gbp"] - frame["prev_close_value"] - frame["external_flow_gbp"],
    )
    denom = frame["prev_close_value"] + 0.5 * frame["external_flow_gbp"]
    frame["daily_return_flow_corrected"] = np.where(
        frame["prev_close_value"].isna() | (denom.abs() < 1e-12),
        0.0,
        frame["daily_pnl_flow_corrected_gbp"] / denom,
    )
    frame["cumulative_twr"] = (1.0 + frame["daily_return_flow_corrected"]).cumprod() - 1.0
    frame["net_deposited_external_to_date_gbp"] = frame["external_flow_gbp"].cumsum()
    frame["cumulative_gain_vs_external_deposits_gbp"] = (
        frame["close_value_gbp"] - frame["net_deposited_external_to_date_gbp"]
    )
    return frame


def compute_account_daily_metrics(value_df: pd.DataFrame, flow_df: pd.DataFrame) -> pd.DataFrame:
    if value_df.empty:
        return pd.DataFrame(
            columns=[
                "account_id",
                "d",
                "close_value_gbp",
                "external_flow_gbp",
                "internal_flow_gbp",
                "net_deposited_external_to_date_gbp",
                "daily_pnl_flow_corrected_gbp",
                "daily_return_flow_corrected",
                "cumulative_twr",
                "cumulative_gain_vs_external_deposits_gbp",
            ]
        )

    values = value_df.copy()
    flows = flow_df.copy() if not flow_df.empty else pd.DataFrame(columns=["account_id", "d"])
    values["d"] = _ensure_date(values["d"])
    if not flows.empty:
        flows["d"] = _ensure_date(flows["d"])

    out_frames: list[pd.DataFrame] = []
    for account_id, account_values in values.groupby("account_id"):
        account_values = account_values.sort_values("d")
        start_d = account_values["d"].min()
        end_d = account_values["d"].max()
        date_index = pd.date_range(start=start_d, end=end_d, freq="D").date

        base = pd.DataFrame({"d": date_index})
        base = base.merge(
            account_values[["d", "close_value_gbp"]],
            how="left",
            on="d",
        )
        account_flows = (
            flows[flows["account_id"] == account_id][["d", "external_flow_gbp", "internal_flow_gbp"]]
            if not flows.empty
            else pd.DataFrame(columns=["d", "external_flow_gbp", "internal_flow_gbp"])
        )
        base = base.merge(account_flows, how="left", on="d")

        base["close_value_gbp"] = pd.to_numeric(base["close_value_gbp"], errors="coerce")
        base["external_flow_gbp"] = pd.to_numeric(base["external_flow_gbp"], errors="coerce")
        base["internal_flow_gbp"] = pd.to_numeric(base["internal_flow_gbp"], errors="coerce")
        base["close_value_gbp"] = base["close_value_gbp"].ffill().bfill().fillna(0.0)
        base["external_flow_gbp"] = base["external_flow_gbp"].fillna(0.0)
        base["internal_flow_gbp"] = base["internal_flow_gbp"].fillna(0.0)

        perf = _compute_perf_frame(base)
        perf["account_id"] = account_id
        out_frames.append(perf)

    final_df = pd.concat(out_frames, ignore_index=True)
    return final_df[
        [
            "account_id",
            "d",
            "close_value_gbp",
            "external_flow_gbp",
            "internal_flow_gbp",
            "net_deposited_external_to_date_gbp",
            "daily_pnl_flow_corrected_gbp",
            "daily_return_flow_corrected",
            "cumulative_twr",
            "cumulative_gain_vs_external_deposits_gbp",
        ]
    ].sort_values(["account_id", "d"])


def compute_portfolio_daily_metrics(account_daily_df: pd.DataFrame) -> pd.DataFrame:
    if account_daily_df.empty:
        return pd.DataFrame(
            columns=[
                "d",
                "close_value_gbp",
                "external_flow_gbp",
                "net_deposited_external_to_date_gbp",
                "daily_pnl_flow_corrected_gbp",
                "daily_return_flow_corrected",
                "cumulative_twr",
            ]
        )

    grouped = (
        account_daily_df.groupby("d", as_index=False)[["close_value_gbp", "external_flow_gbp"]]
        .sum()
        .sort_values("d")
    )
    perf = _compute_perf_frame(grouped)
    return perf[
        [
            "d",
            "close_value_gbp",
            "external_flow_gbp",
            "net_deposited_external_to_date_gbp",
            "daily_pnl_flow_corrected_gbp",
            "daily_return_flow_corrected",
            "cumulative_twr",
        ]
    ]


def compute_trading_metrics(
    daily_returns: pd.Series,
    *,
    periods_per_year: int = 252,
    risk_free_rate_annual: float = 0.0,
    minimum_acceptable_return_daily: float = 0.0,
    benchmark_daily_return: float = 0.0,
) -> dict[str, float]:
    if periods_per_year <= 0:
        raise ValueError("periods_per_year must be positive")

    metrics = _empty_trading_metrics()
    returns = pd.to_numeric(pd.Series(daily_returns), errors="coerce")
    returns = returns[np.isfinite(returns)]
    if returns.empty:
        return metrics

    annualize = float(np.sqrt(periods_per_year))
    wins = returns[returns > 0.0]
    losses = returns[returns < 0.0]
    win_count = int(len(wins))
    loss_count = int(len(losses))
    trade_count = win_count + loss_count

    avg_return = float(returns.mean())
    avg_win = float(wins.mean()) if win_count else np.nan
    avg_loss = float(losses.mean()) if loss_count else np.nan

    daily_volatility = float(returns.std(ddof=1)) if len(returns) > 1 else np.nan
    implied_volatility = daily_volatility * annualize if np.isfinite(daily_volatility) else np.nan

    risk_free_daily = 0.0
    if risk_free_rate_annual > -1.0:
        risk_free_daily = float((1.0 + risk_free_rate_annual) ** (1.0 / periods_per_year) - 1.0)
    excess_returns = returns - risk_free_daily

    sharpe_ratio = np.nan
    if np.isfinite(daily_volatility) and daily_volatility > 0.0:
        sharpe_ratio = float(excess_returns.mean() / daily_volatility * annualize)

    downside = np.minimum(returns - minimum_acceptable_return_daily, 0.0)
    downside_deviation = float(np.sqrt(np.mean(np.square(downside))))
    sortino_ratio = np.nan
    if downside_deviation > 0.0:
        sortino_ratio = float(
            (returns.mean() - minimum_acceptable_return_daily) / downside_deviation * annualize
        )

    omega_numerator = float(np.maximum(returns - minimum_acceptable_return_daily, 0.0).sum())
    omega_denominator = float(np.maximum(minimum_acceptable_return_daily - returns, 0.0).sum())
    omega_ratio = _safe_ratio(omega_numerator, omega_denominator)

    gross_profit = float(wins.sum()) if win_count else 0.0
    gross_loss_abs = float((-losses).sum()) if loss_count else 0.0
    profit_factor = _safe_ratio(gross_profit, gross_loss_abs)

    if win_count and loss_count:
        payoff_ratio = float(avg_win / abs(avg_loss))
    elif win_count and not loss_count:
        payoff_ratio = np.inf
    else:
        payoff_ratio = np.nan

    win_loss_ratio = _safe_ratio(float(win_count), float(loss_count))
    win_rate = float(win_count / trade_count) if trade_count else np.nan

    expected_return = np.nan
    if trade_count and np.isfinite(win_rate):
        safe_avg_win = avg_win if np.isfinite(avg_win) else 0.0
        safe_avg_loss = avg_loss if np.isfinite(avg_loss) else 0.0
        expected_return = float(win_rate * safe_avg_win + (1.0 - win_rate) * safe_avg_loss)

    if np.isfinite(win_rate):
        if np.isinf(payoff_ratio):
            kelly_criterion = float(win_rate)
        elif np.isfinite(payoff_ratio) and payoff_ratio > 0.0:
            kelly_criterion = float(win_rate - ((1.0 - win_rate) / payoff_ratio))
        else:
            kelly_criterion = np.nan
    else:
        kelly_criterion = np.nan

    active_returns = returns - benchmark_daily_return
    tracking_error = float(active_returns.std(ddof=1)) if len(active_returns) > 1 else np.nan
    information_ratio = np.nan
    if np.isfinite(tracking_error) and tracking_error > 0.0:
        information_ratio = float(active_returns.mean() / tracking_error * annualize)

    metrics.update(
        {
            "sharpe_ratio": sharpe_ratio,
            "sortino_ratio": sortino_ratio,
            "omega_ratio": omega_ratio,
            "payoff_ratio": payoff_ratio,
            "profit_factor": profit_factor,
            "kelly_criterion": kelly_criterion,
            "expected_return": expected_return,
            "avg_loss": avg_loss,
            "avg_return": avg_return,
            "avg_win": avg_win,
            "implied_volatility": implied_volatility,
            "information_ratio": information_ratio,
            "volatility": daily_volatility,
            "win_loss_ratio": win_loss_ratio,
            "win_rate": win_rate,
        }
    )
    return metrics
