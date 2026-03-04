from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.prices.asset_identity import canonicalize_asset_name, make_asset_id
from src.transform.account_resolution import load_account_rules, resolve_account_id


SUSPECT_CORPORATE_ACTION_UPPER = 10.0
SUSPECT_CORPORATE_ACTION_LOWER = 0.1


@dataclass
class GapFillDiagnostics:
    accounts_total: int
    accounts_with_checkpoints: int
    accounts_modeled: int
    segments_total: int
    segments_with_price_index: int
    segments_fallback: int

    def as_dict(self) -> dict[str, int]:
        return {
            "accounts_total": int(self.accounts_total),
            "accounts_with_checkpoints": int(self.accounts_with_checkpoints),
            "accounts_modeled": int(self.accounts_modeled),
            "segments_total": int(self.segments_total),
            "segments_with_price_index": int(self.segments_with_price_index),
            "segments_fallback": int(self.segments_fallback),
        }


def _to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", dayfirst=True, format="mixed").dt.date


def _load_asset_checkpoint_weights(asset_values_csv: Path) -> pd.DataFrame:
    if not asset_values_csv.exists():
        return pd.DataFrame(columns=["account_id", "d", "asset_id", "weight"])

    frame = pd.read_csv(asset_values_csv)
    required = {"account_name", "asset_name", "value_gbp", "report_date"}
    if frame.empty or not required.issubset(frame.columns):
        return pd.DataFrame(columns=["account_id", "d", "asset_id", "weight"])

    frame = frame.copy()
    frame["d"] = _to_date(frame["report_date"])
    frame["value_gbp"] = pd.to_numeric(frame["value_gbp"], errors="coerce")
    frame["asset_name_canonical"] = frame["asset_name"].astype(str).map(canonicalize_asset_name)
    frame = frame[
        frame["d"].notna()
        & frame["value_gbp"].notna()
        & (frame["asset_name_canonical"].astype(str).str.len() > 0)
    ].copy()
    if frame.empty:
        return pd.DataFrame(columns=["account_id", "d", "asset_id", "weight"])

    account_rules = load_account_rules()
    frame["account_id"] = frame["account_name"].astype(str).apply(
        lambda name: resolve_account_id(None, str(name), account_rules)
    )
    frame["asset_id"] = frame["asset_name_canonical"].map(make_asset_id)
    frame = frame.groupby(["account_id", "d", "asset_id"], as_index=False)["value_gbp"].sum()
    frame["checkpoint_total"] = frame.groupby(["account_id", "d"])["value_gbp"].transform("sum")
    frame = frame[frame["checkpoint_total"] > 0].copy()
    frame["weight"] = frame["value_gbp"] / frame["checkpoint_total"]
    return frame[["account_id", "d", "asset_id", "weight"]]


def _load_asset_returns(conn, provider: str) -> pd.DataFrame:
    frame = conn.execute(
        """
        select
          m.asset_id,
          p.d,
          coalesce(p.adj_close, p.close) as px
        from asset_ticker_mapping m
        join raw_market_price_daily p
          on p.provider = m.provider
         and p.ticker = m.ticker
        where m.provider = ?
          and m.match_status = 'resolved'
          and coalesce(trim(m.ticker), '') <> ''
        """,
        [provider],
    ).df()
    if frame.empty:
        return pd.DataFrame(columns=["asset_id", "d", "asset_return"])

    frame = frame.copy()
    frame["d"] = _to_date(frame["d"])
    frame["px"] = pd.to_numeric(frame["px"], errors="coerce")
    frame = frame[frame["d"].notna() & frame["px"].notna() & (frame["px"] > 0)].copy()
    if frame.empty:
        return pd.DataFrame(columns=["asset_id", "d", "asset_return"])

    out: list[pd.DataFrame] = []
    for asset_id, asset_df in frame.groupby("asset_id"):
        asset_df = asset_df.sort_values("d").drop_duplicates(subset=["d"], keep="last")
        d0 = asset_df["d"].min()
        d1 = asset_df["d"].max()
        if pd.isna(d0) or pd.isna(d1):
            continue
        all_days = pd.date_range(start=d0, end=d1, freq="D").date
        reindexed = pd.DataFrame({"d": all_days}).merge(asset_df[["d", "px"]], how="left", on="d")
        reindexed["px"] = reindexed["px"].ffill()
        reindexed = reindexed[reindexed["px"].notna()].copy()
        if reindexed.empty:
            continue
        reindexed["asset_return"] = reindexed["px"] / reindexed["px"].shift(1)
        reindexed["asset_return"] = reindexed["asset_return"].fillna(1.0)
        # Yahoo series can contain split/currency-scale jumps where adjusted prices are not backfilled.
        # Treat extreme one-day jumps as non-economic corporate actions for interpolation purposes.
        reindexed.loc[
            (reindexed["asset_return"] > SUSPECT_CORPORATE_ACTION_UPPER)
            | (reindexed["asset_return"] < SUSPECT_CORPORATE_ACTION_LOWER),
            "asset_return",
        ] = 1.0
        reindexed["asset_return"] = reindexed["asset_return"].clip(lower=0.01, upper=100.0)
        reindexed["asset_id"] = asset_id
        out.append(reindexed[["asset_id", "d", "asset_return"]])

    if not out:
        return pd.DataFrame(columns=["asset_id", "d", "asset_return"])
    return pd.concat(out, ignore_index=True)


def _segment_dates(start_d: object, end_d: object) -> list:
    return list(pd.date_range(start=start_d, end=end_d, freq="D").date)


def _build_daily_basket_index(
    seg_dates: list,
    weights_df: pd.DataFrame,
    returns_df: pd.DataFrame,
) -> tuple[pd.DataFrame, bool]:
    """
    Build per-day basket return index for segment.
    Returns (frame[d,basket_return], used_price_index_flag).
    """
    if not seg_dates:
        return pd.DataFrame(columns=["d", "basket_return"]), False

    baseline = pd.DataFrame({"d": seg_dates})
    if weights_df.empty or returns_df.empty:
        baseline["basket_return"] = 1.0
        return baseline, False

    if weights_df.empty:
        baseline["basket_return"] = 1.0
        return baseline, False

    # Build full date x asset grid so missing asset returns contribute 1.0, not dropped weight.
    date_df = pd.DataFrame({"d": seg_dates})
    date_df["_join_key"] = 1
    weight_grid = weights_df.copy()
    weight_grid["_join_key"] = 1
    full = date_df.merge(weight_grid, on="_join_key", how="inner").drop(columns=["_join_key"])
    if full.empty:
        baseline["basket_return"] = 1.0
        return baseline, False
    full = full.merge(returns_df, how="left", on=["asset_id", "d"])
    full["asset_return"] = pd.to_numeric(full["asset_return"], errors="coerce").fillna(1.0)

    full["weighted_return"] = full["weight"] * full["asset_return"]
    basket = full.groupby("d", as_index=False)["weighted_return"].sum().rename(
        columns={"weighted_return": "basket_return"}
    )
    out = baseline.merge(basket, how="left", on="d")
    out["basket_return"] = pd.to_numeric(out["basket_return"], errors="coerce").fillna(1.0)
    used_prices = bool((out["basket_return"] != 1.0).any())
    return out, used_prices


def _segment_close_path(
    seg_dates: list,
    start_value: float,
    end_value: float,
    flow_map: dict,
    basket_index: pd.DataFrame,
) -> list[float]:
    # Unadjusted path U_t.
    b_map = {row["d"]: float(row["basket_return"]) for _, row in basket_index.iterrows()}
    unadjusted: list[float] = []
    current = float(start_value)
    for idx, d in enumerate(seg_dates):
        if idx == 0:
            unadjusted.append(current)
            continue
        flow_t = float(flow_map.get(d, 0.0))
        ret_t = float(b_map.get(d, 1.0))
        current = (current + flow_t) * ret_t
        unadjusted.append(current)

    if len(seg_dates) <= 1:
        return [float(start_value)]

    # Endpoint reconciliation: linearly force exact end checkpoint.
    delta = float(end_value) - float(unadjusted[-1])
    n_steps = len(seg_dates) - 1
    adjusted: list[float] = []
    for i, v in enumerate(unadjusted):
        adj = float(v) + (delta * (i / n_steps))
        adjusted.append(adj)
    return adjusted


def build_gap_filled_account_values(
    conn,
    value_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    asset_values_csv: Path,
    provider: str = "yfinance",
) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Build daily account values using checkpoint anchoring plus yfinance price-shape.
    """
    checkpoints = value_df.copy()
    checkpoints["d"] = _to_date(checkpoints["d"])
    checkpoints["close_value_gbp"] = pd.to_numeric(checkpoints["close_value_gbp"], errors="coerce")
    checkpoints = checkpoints[checkpoints["d"].notna() & checkpoints["close_value_gbp"].notna()].copy()
    if checkpoints.empty:
        empty = pd.DataFrame(columns=["account_id", "d", "close_value_gbp"])
        return empty, GapFillDiagnostics(0, 0, 0, 0, 0, 0).as_dict()

    flows = (
        flow_df.copy()
        if not flow_df.empty
        else pd.DataFrame(columns=["account_id", "d", "external_flow_gbp", "internal_flow_gbp"])
    )
    if not flows.empty:
        if "external_flow_gbp" not in flows.columns:
            flows["external_flow_gbp"] = 0.0
        if "internal_flow_gbp" not in flows.columns:
            flows["internal_flow_gbp"] = 0.0
        flows["d"] = _to_date(flows["d"])
        flows["external_flow_gbp"] = pd.to_numeric(flows["external_flow_gbp"], errors="coerce").fillna(0.0)
        flows["internal_flow_gbp"] = pd.to_numeric(flows["internal_flow_gbp"], errors="coerce").fillna(0.0)
        flows["net_flow_gbp"] = flows["external_flow_gbp"] + flows["internal_flow_gbp"]
        flows = flows[flows["d"].notna()].copy()

    asset_weights = _load_asset_checkpoint_weights(asset_values_csv=asset_values_csv)
    asset_returns = _load_asset_returns(conn=conn, provider=provider)

    flow_key = (
        flows.groupby(["account_id", "d"], as_index=False)["net_flow_gbp"].sum()
        if not flows.empty
        else pd.DataFrame(columns=["account_id", "d", "net_flow_gbp"])
    )

    out_rows: list[dict[str, object]] = []
    segments_total = 0
    segments_with_prices = 0
    segments_fallback = 0
    accounts_modeled = 0

    for account_id, account_cp in checkpoints.groupby("account_id"):
        account_cp = account_cp.sort_values("d").drop_duplicates(subset=["d"], keep="last")
        cp_dates = account_cp["d"].tolist()
        if not cp_dates:
            continue
        if len(cp_dates) == 1:
            row = account_cp.iloc[0]
            out_rows.append({"account_id": account_id, "d": row["d"], "close_value_gbp": float(row["close_value_gbp"])})
            continue

        account_flow = (
            flow_key[flow_key["account_id"] == account_id][["d", "net_flow_gbp"]]
            if not flow_key.empty
            else pd.DataFrame(columns=["d", "net_flow_gbp"])
        )
        flow_map = {r["d"]: float(r["net_flow_gbp"]) for _, r in account_flow.iterrows()}

        account_weights = asset_weights[asset_weights["account_id"] == account_id].copy()
        used_any_segment = False

        for i in range(len(cp_dates) - 1):
            d0 = cp_dates[i]
            d1 = cp_dates[i + 1]
            seg_dates = _segment_dates(d0, d1)
            v0 = float(account_cp.iloc[i]["close_value_gbp"])
            v1 = float(account_cp.iloc[i + 1]["close_value_gbp"])
            segments_total += 1

            seg_weights = account_weights[account_weights["d"] == d0][["asset_id", "weight"]].copy()
            seg_returns = asset_returns[
                (asset_returns["d"] >= d0) & (asset_returns["d"] <= d1)
            ][["asset_id", "d", "asset_return"]].copy()
            basket_index, used_prices = _build_daily_basket_index(
                seg_dates=seg_dates,
                weights_df=seg_weights,
                returns_df=seg_returns,
            )
            if used_prices:
                segments_with_prices += 1
                used_any_segment = True
            else:
                segments_fallback += 1

            seg_values = _segment_close_path(
                seg_dates=seg_dates,
                start_value=v0,
                end_value=v1,
                flow_map=flow_map,
                basket_index=basket_index,
            )
            for idx, d in enumerate(seg_dates):
                if i > 0 and idx == 0:
                    # Boundary already emitted by previous segment.
                    continue
                out_rows.append({"account_id": account_id, "d": d, "close_value_gbp": float(seg_values[idx])})

        if used_any_segment:
            accounts_modeled += 1

    out = pd.DataFrame(out_rows, columns=["account_id", "d", "close_value_gbp"])
    if out.empty:
        return out, GapFillDiagnostics(0, 0, 0, 0, 0, 0).as_dict()

    out["d"] = _to_date(out["d"])
    out["close_value_gbp"] = pd.to_numeric(out["close_value_gbp"], errors="coerce")
    out = out[out["d"].notna() & out["close_value_gbp"].notna()].copy()
    out = out.sort_values(["account_id", "d"]).drop_duplicates(subset=["account_id", "d"], keep="last")
    out["close_value_gbp"] = out["close_value_gbp"].round(2)

    # Re-anchor checkpoint dates exactly.
    anchor = checkpoints[["account_id", "d", "close_value_gbp"]].copy()
    anchor["d"] = _to_date(anchor["d"])
    out = out.merge(
        anchor.rename(columns={"close_value_gbp": "anchor_value"}),
        how="left",
        on=["account_id", "d"],
    )
    out["close_value_gbp"] = out["anchor_value"].where(out["anchor_value"].notna(), out["close_value_gbp"])
    out = out.drop(columns=["anchor_value"])

    diagnostics = GapFillDiagnostics(
        accounts_total=int(checkpoints["account_id"].nunique()),
        accounts_with_checkpoints=int(checkpoints.groupby("account_id").size().ge(2).sum()),
        accounts_modeled=int(accounts_modeled),
        segments_total=int(segments_total),
        segments_with_price_index=int(segments_with_prices),
        segments_fallback=int(segments_fallback),
    ).as_dict()
    return out.reset_index(drop=True), diagnostics
