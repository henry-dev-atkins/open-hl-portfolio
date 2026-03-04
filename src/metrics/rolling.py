from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


ROLLING_WINDOWS = {
    "rolling_6m_return": 183,
    "rolling_1y_return": 365,
}


def _to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def compute_rolling_returns(
    frame: pd.DataFrame,
    return_col: str = "daily_return_flow_corrected",
) -> pd.DataFrame:
    out = frame.copy()
    out["d"] = _to_date(out["d"])
    out[return_col] = pd.to_numeric(out[return_col], errors="coerce")
    out = out[out["d"].notna() & out[return_col].notna()].sort_values("d").reset_index(drop=True)
    if out.empty:
        return pd.DataFrame(columns=["d", *ROLLING_WINDOWS.keys()])

    wealth = (1.0 + out[return_col]).cumprod()
    for col_name, days in ROLLING_WINDOWS.items():
        base = wealth.shift(days)
        out[col_name] = np.where(base > 0, wealth / base - 1.0, np.nan)
    return out[["d", *ROLLING_WINDOWS.keys()]]


def compute_worst_trailing_windows(
    frame: pd.DataFrame,
    top_n: int = 10,
    return_col: str = "daily_return_flow_corrected",
) -> pd.DataFrame:
    rolling = compute_rolling_returns(frame=frame, return_col=return_col)
    if rolling.empty:
        return pd.DataFrame(columns=["window_start", "window_end", "trailing_1y_return"])
    rolling = rolling.dropna(subset=["rolling_1y_return"]).copy()
    if rolling.empty:
        return pd.DataFrame(columns=["window_start", "window_end", "trailing_1y_return"])

    rolling["window_end"] = rolling["d"]
    rolling["window_start"] = (
        pd.to_datetime(rolling["window_end"]).sub(pd.to_timedelta(364, unit="D")).dt.date
    )
    rolling["trailing_1y_return"] = rolling["rolling_1y_return"]
    out = rolling.sort_values("trailing_1y_return").head(max(int(top_n), 1))
    return out[["window_start", "window_end", "trailing_1y_return"]].reset_index(drop=True)


@dataclass
class _Episode:
    peak_date: object
    trough_date: object
    recovery_date: object | None
    max_drawdown: float
    days_to_recover: int | None


def compute_drawdown_episodes(
    frame: pd.DataFrame,
    return_col: str = "daily_return_flow_corrected",
) -> pd.DataFrame:
    base = frame.copy()
    base["d"] = _to_date(base["d"])
    base[return_col] = pd.to_numeric(base[return_col], errors="coerce")
    base = base[base["d"].notna() & base[return_col].notna()].sort_values("d").reset_index(drop=True)
    if base.empty:
        return pd.DataFrame(
            columns=[
                "peak_date",
                "trough_date",
                "recovery_date",
                "max_drawdown",
                "days_to_recover",
                "is_recovered",
            ]
        )

    base["wealth"] = (1.0 + base[return_col]).cumprod()
    base["peak_wealth"] = base["wealth"].cummax()
    base["drawdown"] = base["wealth"] / base["peak_wealth"] - 1.0
    drawdown = base["drawdown"].to_numpy()

    episodes: list[_Episode] = []
    i = 0
    while i < len(base):
        if drawdown[i] >= 0:
            i += 1
            continue
        peak_idx = i - 1 if i > 0 else 0
        end = i
        trough_idx = i
        while end < len(base) and drawdown[end] < 0:
            if drawdown[end] < drawdown[trough_idx]:
                trough_idx = end
            end += 1
        recovery_idx = end if end < len(base) and drawdown[end] >= 0 else None
        if recovery_idx is not None:
            days_to_recover = int(
                (pd.Timestamp(base.iloc[recovery_idx]["d"]) - pd.Timestamp(base.iloc[peak_idx]["d"])).days
            )
            recovery_date = base.iloc[recovery_idx]["d"]
        else:
            days_to_recover = None
            recovery_date = None
        episodes.append(
            _Episode(
                peak_date=base.iloc[peak_idx]["d"],
                trough_date=base.iloc[trough_idx]["d"],
                recovery_date=recovery_date,
                max_drawdown=float(drawdown[trough_idx]),
                days_to_recover=days_to_recover,
            )
        )
        i = end

    if not episodes:
        return pd.DataFrame(
            columns=[
                "peak_date",
                "trough_date",
                "recovery_date",
                "max_drawdown",
                "days_to_recover",
                "is_recovered",
            ]
        )
    out = pd.DataFrame([e.__dict__ for e in episodes])
    out["is_recovered"] = out["recovery_date"].notna()
    return out


def build_rolling_panel(
    portfolio_daily_df: pd.DataFrame,
    account_daily_df: pd.DataFrame,
    return_col: str = "daily_return_flow_corrected",
) -> pd.DataFrame:
    panels: list[pd.DataFrame] = []
    if not portfolio_daily_df.empty:
        p = compute_rolling_returns(portfolio_daily_df, return_col=return_col)
        if not p.empty:
            p["scope_type"] = "portfolio"
            p["scope_id"] = "PORTFOLIO"
            panels.append(p)
    if not account_daily_df.empty:
        for account_id, group in account_daily_df.groupby("account_id"):
            g = compute_rolling_returns(group, return_col=return_col)
            if g.empty:
                continue
            g["scope_type"] = "account"
            g["scope_id"] = str(account_id)
            panels.append(g)
    if not panels:
        return pd.DataFrame(columns=["scope_type", "scope_id", "d", "rolling_6m_return", "rolling_1y_return"])
    out = pd.concat(panels, ignore_index=True)
    return out[["scope_type", "scope_id", "d", "rolling_6m_return", "rolling_1y_return"]]
