from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from src.common.config import load_yaml
from src.common.paths import PROJECT_ROOT


def _to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def _load_policy_weights(path: str | Path | None = None) -> tuple[dict[str, float], float]:
    cfg_path = Path(path) if path else (PROJECT_ROOT / "config" / "attribution_policy.yml")
    cfg = load_yaml(cfg_path)
    weights = cfg.get("account_policy_weights", {})
    coverage_min_weight = float(cfg.get("coverage_min_weight", 0.7))
    if not isinstance(weights, dict):
        return {}, coverage_min_weight
    out: dict[str, float] = {}
    for account_id, value in weights.items():
        try:
            out[str(account_id)] = float(value)
        except Exception:  # noqa: BLE001
            continue
    return out, coverage_min_weight


def _policy_weights_for_day(account_ids: list[str], configured: dict[str, float]) -> dict[str, float]:
    if not account_ids:
        return {}
    available = {aid: configured[aid] for aid in account_ids if aid in configured}
    if not available:
        equal = 1.0 / len(account_ids)
        return {aid: equal for aid in account_ids}
    total = float(sum(max(v, 0.0) for v in available.values()))
    if total <= 0:
        equal = 1.0 / len(account_ids)
        return {aid: equal for aid in account_ids}
    normalized = {aid: max(v, 0.0) / total for aid, v in available.items()}
    missing = [aid for aid in account_ids if aid not in normalized]
    if missing:
        rem = max(0.0, 1.0 - sum(normalized.values()))
        each = rem / len(missing) if missing else 0.0
        for aid in missing:
            normalized[aid] = each
    return normalized


def compute_attribution_daily(
    asset_daily_df: pd.DataFrame,
    account_daily_df: pd.DataFrame,
    portfolio_daily_df: pd.DataFrame,
    config_path: str | Path | None = None,
) -> pd.DataFrame:
    if asset_daily_df.empty or account_daily_df.empty or portfolio_daily_df.empty:
        return pd.DataFrame(
            columns=[
                "d",
                "benchmark_return",
                "allocation_effect",
                "selection_effect",
                "interaction_effect",
                "residual_effect",
                "portfolio_return",
                "cumulative_benchmark_return",
                "cumulative_allocation_effect",
                "cumulative_selection_effect",
                "cumulative_interaction_effect",
                "cumulative_residual_effect",
            ]
        )

    cfg_weights, coverage_min_weight = _load_policy_weights(path=config_path)

    assets = asset_daily_df.copy()
    assets["d"] = _to_date(assets["d"])
    assets["value_gbp"] = pd.to_numeric(assets["value_gbp"], errors="coerce")
    assets["daily_return"] = pd.to_numeric(assets["daily_return"], errors="coerce")
    assets = assets[assets["d"].notna() & assets["value_gbp"].notna() & assets["daily_return"].notna()].copy()
    if assets.empty:
        return pd.DataFrame()

    assets = assets.sort_values(["account_id", "asset_id", "d"])
    assets["prev_value"] = assets.groupby(["account_id", "asset_id"])["value_gbp"].shift(1)
    prev_sum = (
        assets.groupby(["account_id", "d"], as_index=False)["prev_value"]
        .sum()
        .rename(columns={"prev_value": "account_prev_value"})
    )
    assets = assets.merge(prev_sum, on=["account_id", "d"], how="left")
    assets["w_prev_asset"] = np.where(
        assets["account_prev_value"] > 0,
        assets["prev_value"] / assets["account_prev_value"],
        0.0,
    )

    assets["weighted_return"] = assets["w_prev_asset"] * assets["daily_return"]
    account_rp = assets.groupby(["account_id", "d"], as_index=False)["weighted_return"].sum().rename(
        columns={"weighted_return": "r_p_a"}
    )
    account_rb = (
        assets[assets["w_prev_asset"] > 0]
        .groupby(["account_id", "d"], as_index=False)["daily_return"]
        .mean()
        .rename(columns={"daily_return": "r_b_a"})
    )
    account_frame = account_rp.merge(account_rb, on=["account_id", "d"], how="left")
    account_frame["r_b_a"] = account_frame["r_b_a"].fillna(0.0)

    accounts = account_daily_df.copy()
    accounts["d"] = _to_date(accounts["d"])
    accounts["close_value_gbp"] = pd.to_numeric(accounts["close_value_gbp"], errors="coerce")
    accounts = accounts[accounts["d"].notna() & accounts["close_value_gbp"].notna()].copy()
    accounts = accounts.sort_values(["account_id", "d"])
    accounts["prev_close_value"] = accounts.groupby("account_id")["close_value_gbp"].shift(1)
    total_prev = accounts.groupby("d", as_index=False)["prev_close_value"].sum().rename(
        columns={"prev_close_value": "portfolio_prev_value"}
    )
    accounts = accounts.merge(total_prev, on="d", how="left")
    accounts["W_p_a"] = np.where(
        accounts["portfolio_prev_value"] > 0,
        accounts["prev_close_value"] / accounts["portfolio_prev_value"],
        0.0,
    )
    account_frame = account_frame.merge(accounts[["account_id", "d", "W_p_a"]], on=["account_id", "d"], how="left")
    account_frame["W_p_a"] = account_frame["W_p_a"].fillna(0.0)

    portfolio = portfolio_daily_df.copy()
    portfolio["d"] = _to_date(portfolio["d"])
    portfolio["portfolio_return"] = pd.to_numeric(portfolio["daily_return_flow_corrected"], errors="coerce")
    portfolio = portfolio[portfolio["d"].notna() & portfolio["portfolio_return"].notna()][["d", "portfolio_return"]]
    if portfolio.empty:
        return pd.DataFrame()

    rows: list[dict[str, float | str | object]] = []
    for d, day in account_frame.groupby("d"):
        day = day.copy()
        account_ids = day["account_id"].astype(str).tolist()
        wb = _policy_weights_for_day(account_ids=account_ids, configured=cfg_weights)
        day["W_b_a"] = day["account_id"].astype(str).map(wb).fillna(0.0)
        coverage = float(day["W_p_a"].sum())
        if coverage < coverage_min_weight:
            continue

        r_b = float((day["W_b_a"] * day["r_b_a"]).sum())
        allocation = float(((day["W_p_a"] - day["W_b_a"]) * (day["r_b_a"] - r_b)).sum())
        selection = float((day["W_b_a"] * (day["r_p_a"] - day["r_b_a"])).sum())
        interaction = float(((day["W_p_a"] - day["W_b_a"]) * (day["r_p_a"] - day["r_b_a"])).sum())
        p_row = portfolio[portfolio["d"] == d]
        if p_row.empty:
            continue
        portfolio_return = float(p_row.iloc[0]["portfolio_return"])
        residual = float(portfolio_return - (allocation + selection + interaction + r_b))

        rows.append(
            {
                "d": d,
                "benchmark_return": r_b,
                "allocation_effect": allocation,
                "selection_effect": selection,
                "interaction_effect": interaction,
                "residual_effect": residual,
                "portfolio_return": portfolio_return,
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out = out.sort_values("d").reset_index(drop=True)
    out["cumulative_benchmark_return"] = (1.0 + out["benchmark_return"]).cumprod() - 1.0
    out["cumulative_allocation_effect"] = out["allocation_effect"].cumsum()
    out["cumulative_selection_effect"] = out["selection_effect"].cumsum()
    out["cumulative_interaction_effect"] = out["interaction_effect"].cumsum()
    out["cumulative_residual_effect"] = out["residual_effect"].cumsum()
    return out
