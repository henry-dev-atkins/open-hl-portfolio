from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from src.common.config import load_yaml
from src.common.paths import PROJECT_ROOT


def _to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def _load_limits(path: str | Path | None = None) -> dict[str, dict[str, float]]:
    cfg_path = Path(path) if path else (PROJECT_ROOT / "config" / "risk_limits.yml")
    cfg = load_yaml(cfg_path)
    out: dict[str, dict[str, float]] = {}
    for key in ("max_single_weight", "top5_weight", "top10_weight", "hhi"):
        limit = cfg.get(key, {})
        try:
            warn = float(limit.get("warning"))
            crit = float(limit.get("critical"))
        except Exception:  # noqa: BLE001
            continue
        out[key] = {"warning": warn, "critical": crit}
    return out


def _breach_level(value: float, limit: dict[str, float] | None) -> str | None:
    if limit is None or not np.isfinite(value):
        return None
    if value >= float(limit["critical"]):
        return "critical"
    if value >= float(limit["warning"]):
        return "warning"
    return None


def _calc_metrics(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty:
        return {
            "max_single_weight": np.nan,
            "top5_weight": np.nan,
            "top10_weight": np.nan,
            "hhi": np.nan,
            "effective_n": np.nan,
        }
    weights = (
        frame["weight"].astype(float).to_numpy()
        if "weight" in frame.columns and frame["weight"].notna().any()
        else np.array([])
    )
    if weights.size == 0:
        values = pd.to_numeric(frame["value_gbp"], errors="coerce").fillna(0.0).to_numpy()
        total = float(values.sum())
        if total <= 0:
            weights = np.zeros_like(values)
        else:
            weights = values / total
    weights = np.sort(weights)[::-1]
    hhi = float(np.square(weights).sum()) if weights.size else np.nan
    effective_n = float(1.0 / hhi) if np.isfinite(hhi) and hhi > 0 else np.nan
    return {
        "max_single_weight": float(weights[0]) if weights.size else np.nan,
        "top5_weight": float(weights[:5].sum()) if weights.size else np.nan,
        "top10_weight": float(weights[:10].sum()) if weights.size else np.nan,
        "hhi": hhi,
        "effective_n": effective_n,
    }


def compute_concentration_daily(
    asset_daily_df: pd.DataFrame,
    config_path: str | Path | None = None,
) -> pd.DataFrame:
    if asset_daily_df.empty:
        return pd.DataFrame(
            columns=[
                "scope_type",
                "scope_id",
                "d",
                "max_single_weight",
                "top5_weight",
                "top10_weight",
                "hhi",
                "effective_n",
                "breach_single",
                "breach_top5",
                "breach_top10",
                "breach_hhi",
            ]
        )
    limits = _load_limits(path=config_path)
    df = asset_daily_df.copy()
    df["d"] = _to_date(df["d"])
    df["value_gbp"] = pd.to_numeric(df["value_gbp"], errors="coerce")
    df["weight"] = pd.to_numeric(df.get("weight"), errors="coerce")
    df = df[df["d"].notna() & df["value_gbp"].notna()].copy()
    if df.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []

    for (account_id, d), grp in df.groupby(["account_id", "d"]):
        metrics = _calc_metrics(grp)
        rows.append(
            {
                "scope_type": "account",
                "scope_id": str(account_id),
                "d": d,
                **metrics,
            }
        )

    portfolio_daily = (
        df.groupby(["asset_id", "d"], as_index=False)[["value_gbp"]]
        .sum()
        .sort_values(["d", "asset_id"])
    )
    for d, grp in portfolio_daily.groupby("d"):
        metrics = _calc_metrics(grp)
        rows.append(
            {
                "scope_type": "portfolio",
                "scope_id": "PORTFOLIO",
                "d": d,
                **metrics,
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out["breach_single"] = out["max_single_weight"].apply(lambda x: _breach_level(float(x), limits.get("max_single_weight")))
    out["breach_top5"] = out["top5_weight"].apply(lambda x: _breach_level(float(x), limits.get("top5_weight")))
    out["breach_top10"] = out["top10_weight"].apply(lambda x: _breach_level(float(x), limits.get("top10_weight")))
    out["breach_hhi"] = out["hhi"].apply(lambda x: _breach_level(float(x), limits.get("hhi")))
    return out[
        [
            "scope_type",
            "scope_id",
            "d",
            "max_single_weight",
            "top5_weight",
            "top10_weight",
            "hhi",
            "effective_n",
            "breach_single",
            "breach_top5",
            "breach_top10",
            "breach_hhi",
        ]
    ].sort_values(["scope_type", "scope_id", "d"])
