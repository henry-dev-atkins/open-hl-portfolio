from __future__ import annotations

import pandas as pd


def _to_ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def build_yfinance_shaped_asset_series(
    checkpoints: pd.DataFrame,
    price_history: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build a daily asset series between report checkpoints using Yahoo return shape.
    The series is forced to match checkpoint values exactly at each checkpoint date.
    """
    required_cp = {"report_date", "value_gbp"}
    if checkpoints.empty or not required_cp.issubset(checkpoints.columns):
        return pd.DataFrame(columns=["d", "value_gbp"])

    cp = checkpoints.copy()
    cp["report_date"] = _to_ts(cp["report_date"])
    cp["value_gbp"] = pd.to_numeric(cp["value_gbp"], errors="coerce")
    cp = cp[cp["report_date"].notna() & cp["value_gbp"].notna()].copy()
    cp = cp.sort_values("report_date").drop_duplicates(subset=["report_date"], keep="last")
    if len(cp) < 2:
        return pd.DataFrame(columns=["d", "value_gbp"])

    px = price_history.copy() if not price_history.empty else pd.DataFrame(columns=["d", "px"])
    if px.empty:
        return pd.DataFrame(columns=["d", "value_gbp"])
    if "px" not in px.columns:
        if "adj_close" in px.columns:
            px["px"] = px["adj_close"]
        elif "close" in px.columns:
            px["px"] = px["close"]
        else:
            return pd.DataFrame(columns=["d", "value_gbp"])
    px["d"] = _to_ts(px["d"])
    px["px"] = pd.to_numeric(px["px"], errors="coerce")
    px = px[px["d"].notna() & px["px"].notna() & (px["px"] > 0)].copy()
    if px.empty:
        return pd.DataFrame(columns=["d", "value_gbp"])

    px = px.sort_values("d").drop_duplicates(subset=["d"], keep="last")
    all_days = pd.date_range(start=px["d"].min(), end=px["d"].max(), freq="D")
    px_daily = pd.DataFrame({"d": all_days}).merge(px[["d", "px"]], on="d", how="left")
    px_daily["px"] = px_daily["px"].ffill()
    px_map = {row["d"]: float(row["px"]) for _, row in px_daily.iterrows() if pd.notna(row["px"])}

    out_rows: list[dict[str, object]] = []
    cp_rows = list(cp[["report_date", "value_gbp"]].itertuples(index=False, name=None))
    for i in range(len(cp_rows) - 1):
        d0, v0 = cp_rows[i]
        d1, v1 = cp_rows[i + 1]
        seg_days = list(pd.date_range(start=d0, end=d1, freq="D"))
        if not seg_days:
            continue

        unadjusted: list[float] = [float(v0)]
        for j in range(1, len(seg_days)):
            prev_d = seg_days[j - 1]
            d = seg_days[j]
            p_prev = px_map.get(prev_d)
            p_cur = px_map.get(d)
            if p_prev is None or p_cur is None or p_prev <= 0:
                r = 1.0
            else:
                r = float(p_cur) / float(p_prev)
            unadjusted.append(unadjusted[-1] * r)

        if len(unadjusted) == 1:
            adjusted = [float(v0)]
        else:
            delta = float(v1) - float(unadjusted[-1])
            n = len(unadjusted) - 1
            adjusted = [float(u) + delta * (k / n) for k, u in enumerate(unadjusted)]

        for j, d in enumerate(seg_days):
            if i > 0 and j == 0:
                continue
            out_rows.append({"d": d, "value_gbp": float(adjusted[j])})

    out = pd.DataFrame(out_rows, columns=["d", "value_gbp"])
    if out.empty:
        return out
    out["d"] = _to_ts(out["d"])
    out["value_gbp"] = pd.to_numeric(out["value_gbp"], errors="coerce")
    out = out[out["d"].notna() & out["value_gbp"].notna()].copy()
    out = out.sort_values("d").drop_duplicates(subset=["d"], keep="last")

    # Re-anchor checkpoints exactly.
    anchors = cp.rename(columns={"report_date": "d"})[["d", "value_gbp"]].copy()
    out = out.merge(anchors.rename(columns={"value_gbp": "anchor"}), on="d", how="left")
    out["value_gbp"] = out["anchor"].where(out["anchor"].notna(), out["value_gbp"])
    out = out.drop(columns=["anchor"])
    return out.reset_index(drop=True)
