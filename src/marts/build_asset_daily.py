from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from src.prices.asset_identity import canonicalize_asset_name, make_asset_id
from src.presentation.asset_series import build_yfinance_shaped_asset_series
from src.transform.account_resolution import load_account_rules, resolve_account_id


def _to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def _linear_daily_series(checkpoints: pd.DataFrame) -> pd.DataFrame:
    cp = checkpoints.copy()
    cp["d"] = _to_date(cp["d"])
    cp["value_gbp"] = pd.to_numeric(cp["value_gbp"], errors="coerce")
    cp = cp[cp["d"].notna() & cp["value_gbp"].notna()].sort_values("d")
    cp = cp.drop_duplicates(subset=["d"], keep="last")
    if cp.empty:
        return pd.DataFrame(columns=["d", "value_gbp"])
    if len(cp) == 1:
        return cp[["d", "value_gbp"]].reset_index(drop=True)

    rows: list[dict[str, object]] = []
    cp_rows = list(cp[["d", "value_gbp"]].itertuples(index=False, name=None))
    for i in range(len(cp_rows) - 1):
        d0, v0 = cp_rows[i]
        d1, v1 = cp_rows[i + 1]
        seg_days = list(pd.date_range(start=d0, end=d1, freq="D").date)
        if not seg_days:
            continue
        n = len(seg_days) - 1
        for j, d in enumerate(seg_days):
            if i > 0 and j == 0:
                continue
            t = 0.0 if n == 0 else (j / n)
            rows.append({"d": d, "value_gbp": float(v0) + (float(v1) - float(v0)) * t})

    out = pd.DataFrame(rows)
    if out.empty:
        return cp[["d", "value_gbp"]].reset_index(drop=True)
    out["d"] = _to_date(out["d"])
    out["value_gbp"] = pd.to_numeric(out["value_gbp"], errors="coerce")
    return out[out["d"].notna() & out["value_gbp"].notna()].drop_duplicates(subset=["d"], keep="last")


def load_checkpoints_from_report_csv(asset_values_csv: Path, run_id: str) -> pd.DataFrame:
    if not asset_values_csv.exists():
        return pd.DataFrame(
            columns=[
                "account_id",
                "asset_id",
                "asset_name_canonical",
                "d",
                "value_gbp",
                "isin",
                "sedol",
                "source_run_id",
                "source_file",
                "source_row_num",
            ]
        )
    frame = pd.read_csv(asset_values_csv)
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "account_id",
                "asset_id",
                "asset_name_canonical",
                "d",
                "value_gbp",
                "isin",
                "sedol",
                "source_run_id",
                "source_file",
                "source_row_num",
            ]
        )

    account_rules = load_account_rules()
    frame["account_id"] = frame["account_name"].apply(lambda n: resolve_account_id(None, str(n), account_rules))
    frame["asset_name_canonical"] = frame["asset_name"].astype(str).apply(canonicalize_asset_name)
    frame["asset_id"] = frame["asset_name_canonical"].apply(make_asset_id)
    frame["d"] = pd.to_datetime(frame["report_date"], errors="coerce", dayfirst=True, format="mixed").dt.date
    frame["value_gbp"] = pd.to_numeric(frame["value_gbp"], errors="coerce")
    frame["isin"] = frame.get("isin")
    frame["sedol"] = frame.get("sedol")
    frame["source_file"] = frame.get("source_pdf")
    frame["source_row_num"] = pd.to_numeric(frame.get("source_row_num"), errors="coerce")
    frame = frame[
        frame["account_id"].notna()
        & frame["asset_id"].notna()
        & frame["d"].notna()
        & frame["value_gbp"].notna()
    ].copy()
    frame["source_run_id"] = run_id

    out = frame[
        [
            "account_id",
            "asset_id",
            "asset_name_canonical",
            "d",
            "value_gbp",
            "isin",
            "sedol",
            "source_run_id",
            "source_file",
            "source_row_num",
        ]
    ]
    out = out.sort_values(["account_id", "asset_id", "d", "source_row_num"]).drop_duplicates(
        subset=["account_id", "asset_id", "d"],
        keep="last",
    )
    return out.reset_index(drop=True)


def stage_asset_checkpoints(conn, checkpoint_df: pd.DataFrame, run_id: str) -> None:
    conn.execute("delete from stg_asset_checkpoint where source_run_id = ?", [run_id])
    if checkpoint_df.empty:
        return
    conn.register("tmp_stg_asset_checkpoint_stage", checkpoint_df)
    conn.execute("insert into stg_asset_checkpoint select * from tmp_stg_asset_checkpoint_stage")
    conn.unregister("tmp_stg_asset_checkpoint_stage")


def _upsert_dim_asset_from_checkpoints(conn, checkpoint_df: pd.DataFrame) -> None:
    if checkpoint_df.empty:
        return
    dim = (
        checkpoint_df.groupby(["asset_id", "asset_name_canonical"], as_index=False)
        .agg(
            isin=("isin", "first"),
            sedol=("sedol", "first"),
            first_seen_date=("d", "min"),
            last_seen_date=("d", "max"),
        )
    )
    dim["source_priority"] = "asset_checkpoints"
    asset_ids = dim["asset_id"].astype(str).tolist()
    if asset_ids:
        placeholders = ",".join(["?"] * len(asset_ids))
        existing = conn.execute(
            f"select * from dim_asset where asset_id in ({placeholders})",  # noqa: S608
            asset_ids,
        ).df()
    else:
        existing = pd.DataFrame()
    if not existing.empty:
        merged = dim.merge(
            existing.rename(
                columns={
                    "asset_name_canonical": "asset_name_existing",
                    "isin": "isin_existing",
                    "sedol": "sedol_existing",
                    "first_seen_date": "first_seen_existing",
                    "last_seen_date": "last_seen_existing",
                    "source_priority": "source_priority_existing",
                }
            ),
            on="asset_id",
            how="left",
        )
        merged["asset_name_canonical"] = merged["asset_name_canonical"].fillna(merged["asset_name_existing"])
        merged["isin"] = merged["isin"].fillna(merged["isin_existing"])
        merged["sedol"] = merged["sedol"].fillna(merged["sedol_existing"])
        merged["first_seen_date"] = pd.to_datetime(merged["first_seen_date"], errors="coerce")
        merged["last_seen_date"] = pd.to_datetime(merged["last_seen_date"], errors="coerce")
        merged["first_seen_existing"] = pd.to_datetime(merged["first_seen_existing"], errors="coerce")
        merged["last_seen_existing"] = pd.to_datetime(merged["last_seen_existing"], errors="coerce")
        merged["first_seen_date"] = merged[["first_seen_date", "first_seen_existing"]].min(axis=1)
        merged["last_seen_date"] = merged[["last_seen_date", "last_seen_existing"]].max(axis=1)
        merged["source_priority"] = merged["source_priority_existing"].fillna(merged["source_priority"])
        dim = merged[
            [
                "asset_id",
                "asset_name_canonical",
                "isin",
                "sedol",
                "first_seen_date",
                "last_seen_date",
                "source_priority",
            ]
        ].copy()
        dim["first_seen_date"] = pd.to_datetime(dim["first_seen_date"], errors="coerce").dt.date
        dim["last_seen_date"] = pd.to_datetime(dim["last_seen_date"], errors="coerce").dt.date

    if asset_ids:
        placeholders = ",".join(["?"] * len(asset_ids))
        conn.execute(f"delete from dim_asset where asset_id in ({placeholders})", asset_ids)  # noqa: S608
    conn.register("tmp_dim_asset_from_checkpoints", dim)
    conn.execute("insert into dim_asset select * from tmp_dim_asset_from_checkpoints")
    conn.unregister("tmp_dim_asset_from_checkpoints")


def _load_price_history_for_asset(conn, asset_id: str, provider: str) -> pd.DataFrame:
    ticker_row = conn.execute(
        """
        select ticker
        from asset_ticker_mapping
        where asset_id = ?
          and provider = ?
          and match_status = 'resolved'
          and coalesce(trim(ticker), '') <> ''
        limit 1
        """,
        [asset_id, provider],
    ).df()
    if ticker_row.empty:
        return pd.DataFrame(columns=["d", "px"])
    ticker = str(ticker_row.iloc[0]["ticker"]).strip()
    return conn.execute(
        """
        select d, coalesce(adj_close, close) as px
        from raw_market_price_daily
        where provider = ?
          and ticker = ?
        order by d
        """,
        [provider, ticker],
    ).df()


def build_asset_daily_from_checkpoints(
    conn,
    checkpoint_df: pd.DataFrame,
    account_value_df: pd.DataFrame,
    source_run_id: str,
    price_provider: str = "yfinance",
) -> pd.DataFrame:
    if checkpoint_df.empty:
        conn.execute("delete from mart_asset_daily")
        return pd.DataFrame(
            columns=[
                "account_id",
                "asset_id",
                "d",
                "value_gbp",
                "daily_return",
                "weight",
                "interpolation_method",
                "source_run_id",
            ]
        )

    cp = checkpoint_df.copy()
    cp["d"] = _to_date(cp["d"])
    cp["value_gbp"] = pd.to_numeric(cp["value_gbp"], errors="coerce")
    cp = cp[cp["d"].notna() & cp["value_gbp"].notna()].copy()
    _upsert_dim_asset_from_checkpoints(conn=conn, checkpoint_df=cp)

    account_values = account_value_df.copy()
    if not account_values.empty:
        account_values["d"] = _to_date(account_values["d"])
        account_values["close_value_gbp"] = pd.to_numeric(account_values["close_value_gbp"], errors="coerce")
        account_values = account_values[account_values["d"].notna() & account_values["close_value_gbp"].notna()].copy()

    out_frames: list[pd.DataFrame] = []
    for (account_id, asset_id), group in cp.groupby(["account_id", "asset_id"]):
        group = group.sort_values("d")
        px = _load_price_history_for_asset(conn=conn, asset_id=str(asset_id), provider=price_provider)
        shaped = build_yfinance_shaped_asset_series(
            checkpoints=group.rename(columns={"d": "report_date"})[["report_date", "value_gbp"]],
            price_history=px,
        )
        if not shaped.empty:
            series = shaped.rename(columns={"d": "d", "value_gbp": "value_gbp"})
            method = "yfinance_shaped"
        else:
            series = _linear_daily_series(group[["d", "value_gbp"]])
            method = "linear"
        series["d"] = _to_date(series["d"])
        series["value_gbp"] = pd.to_numeric(series["value_gbp"], errors="coerce")
        series = series[series["d"].notna() & series["value_gbp"].notna()].copy()
        if series.empty:
            continue

        if not account_values.empty and account_id in set(account_values["account_id"].astype(str)):
            account_days = account_values[account_values["account_id"].astype(str) == str(account_id)]["d"]
            if not account_days.empty:
                all_days = pd.date_range(start=account_days.min(), end=account_days.max(), freq="D").date
            else:
                all_days = pd.date_range(start=series["d"].min(), end=series["d"].max(), freq="D").date
        else:
            all_days = pd.date_range(start=series["d"].min(), end=series["d"].max(), freq="D").date

        base = pd.DataFrame({"d": all_days})
        base = base.merge(series, on="d", how="left")
        min_d = series["d"].min()
        max_d = series["d"].max()
        boundary_mask = base["d"].lt(min_d) | base["d"].gt(max_d)
        base["value_gbp"] = base["value_gbp"].interpolate(method="linear", limit_direction="both")
        base.loc[boundary_mask, "value_gbp"] = 0.0
        base["value_gbp"] = base["value_gbp"].fillna(0.0)
        base["interpolation_method"] = np.where(boundary_mask, "boundary_zero", method)
        base["account_id"] = account_id
        base["asset_id"] = asset_id
        out_frames.append(base[["account_id", "asset_id", "d", "value_gbp", "interpolation_method"]])

    if not out_frames:
        conn.execute("delete from mart_asset_daily")
        return pd.DataFrame(
            columns=[
                "account_id",
                "asset_id",
                "d",
                "value_gbp",
                "daily_return",
                "weight",
                "interpolation_method",
                "source_run_id",
            ]
        )

    out = pd.concat(out_frames, ignore_index=True)
    out = out.sort_values(["account_id", "asset_id", "d"])
    out["prev_value"] = out.groupby(["account_id", "asset_id"])["value_gbp"].shift(1)
    out["daily_return"] = np.where(
        out["prev_value"].isna() | (out["prev_value"] <= 0),
        0.0,
        out["value_gbp"] / out["prev_value"] - 1.0,
    )

    if not account_values.empty:
        denom = account_values.rename(columns={"close_value_gbp": "denom"})[["account_id", "d", "denom"]]
        out = out.merge(denom, on=["account_id", "d"], how="left")
    else:
        out["denom"] = np.nan
    sum_denom = out.groupby(["account_id", "d"], as_index=False)["value_gbp"].sum().rename(
        columns={"value_gbp": "fallback_denom"}
    )
    out = out.merge(sum_denom, on=["account_id", "d"], how="left")
    out["denom"] = out["denom"].fillna(out["fallback_denom"])
    out["weight"] = np.where(out["denom"] > 0, out["value_gbp"] / out["denom"], 0.0)
    out["source_run_id"] = source_run_id
    out = out[
        [
            "account_id",
            "asset_id",
            "d",
            "value_gbp",
            "daily_return",
            "weight",
            "interpolation_method",
            "source_run_id",
        ]
    ]

    conn.execute("delete from mart_asset_daily")
    conn.register("tmp_mart_asset_daily", out)
    conn.execute("insert into mart_asset_daily select * from tmp_mart_asset_daily")
    conn.unregister("tmp_mart_asset_daily")
    return out.reset_index(drop=True)


def load_staged_checkpoints(conn, run_id: str) -> pd.DataFrame:
    return conn.execute(
        """
        select
          account_id,
          asset_id,
          asset_name_canonical,
          d,
          value_gbp,
          isin,
          sedol,
          source_run_id,
          source_file,
          source_row_num
        from stg_asset_checkpoint
        where source_run_id = ?
        order by account_id, asset_id, d
        """,
        [run_id],
    ).df()
