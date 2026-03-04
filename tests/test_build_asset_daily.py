from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.db import connect_db, ensure_schema
from src.marts.build_asset_daily import build_asset_daily_from_checkpoints
from src.prices.asset_identity import canonicalize_asset_name, make_asset_id


def test_build_asset_daily_linear_interpolation(tmp_path: Path) -> None:
    db_path = tmp_path / "asset_daily.duckdb"
    conn = connect_db(db_path)
    ensure_schema(conn)

    asset_name = canonicalize_asset_name("Test Fund")
    asset_id = make_asset_id(asset_name)
    checkpoints = pd.DataFrame(
        [
            {
                "account_id": "ISA",
                "asset_id": asset_id,
                "asset_name_canonical": asset_name,
                "d": pd.Timestamp("2026-01-01").date(),
                "value_gbp": 100.0,
                "isin": None,
                "sedol": None,
                "source_run_id": "RUN_1",
                "source_file": "holdings.csv",
                "source_row_num": 1,
            },
            {
                "account_id": "ISA",
                "asset_id": asset_id,
                "asset_name_canonical": asset_name,
                "d": pd.Timestamp("2026-01-03").date(),
                "value_gbp": 120.0,
                "isin": None,
                "sedol": None,
                "source_run_id": "RUN_1",
                "source_file": "holdings.csv",
                "source_row_num": 2,
            },
        ]
    )
    account_values = pd.DataFrame(
        [
            {"account_id": "ISA", "d": pd.Timestamp("2026-01-01").date(), "close_value_gbp": 100.0},
            {"account_id": "ISA", "d": pd.Timestamp("2026-01-02").date(), "close_value_gbp": 110.0},
            {"account_id": "ISA", "d": pd.Timestamp("2026-01-03").date(), "close_value_gbp": 120.0},
        ]
    )

    out = build_asset_daily_from_checkpoints(
        conn=conn,
        checkpoint_df=checkpoints,
        account_value_df=account_values,
        source_run_id="RUN_1",
    )
    assert len(out) == 3

    day2 = out[out["d"] == pd.Timestamp("2026-01-02").date()].iloc[0]
    assert round(float(day2["value_gbp"]), 6) == 110.0
    assert round(float(day2["daily_return"]), 6) == round(110.0 / 100.0 - 1.0, 6)
    assert round(float(day2["weight"]), 6) == 1.0
