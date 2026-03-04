from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pandas as pd

from src.common.config import load_yaml
from src.common.db import connect_db, ensure_schema
from src.metrics.cost_drag import build_stg_account_cost_daily, compute_cost_drag_tables
from src.transform.build_daily_flows import build_daily_flows
from src.transform.clean_holdings import clean_holdings
from src.transform.clean_transactions import classify_transaction, clean_transactions, normalize_amount
from src.transform.clean_valuations import clean_valuations


def _workspace_scratch_dir(name: str) -> Path:
    root = Path("data") / "_phase1_test_tmp" / f"{name}_{uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    return root


def test_clean_valuations_clears_stale_rows_when_raw_rows_disappear() -> None:
    conn = connect_db(_workspace_scratch_dir("valuations_cleanup") / "valuations_cleanup.duckdb")
    ensure_schema(conn)
    conn.execute(
        """
        insert into raw_valuations
          (run_id, source_file, row_num, account_name_raw, valuation_date, total_value_gbp)
        values
          ('RUN_1', 'valuations.csv', 1, 'Stocks & Shares ISA', '2026-01-01', 100.0)
        """
    )

    clean_valuations(conn=conn, run_id="RUN_1")
    before = conn.execute(
        "select count(*) from stg_account_value_daily where source_run_id = 'RUN_1'"
    ).fetchone()[0]
    assert before == 1

    conn.execute("delete from raw_valuations where run_id = 'RUN_1'")
    clean_valuations(conn=conn, run_id="RUN_1")
    after = conn.execute(
        "select count(*) from stg_account_value_daily where source_run_id = 'RUN_1'"
    ).fetchone()[0]
    assert after == 0


def test_clean_transactions_clears_stale_rows_when_raw_rows_disappear() -> None:
    conn = connect_db(_workspace_scratch_dir("transactions_cleanup") / "transactions_cleanup.duckdb")
    ensure_schema(conn)
    conn.execute(
        """
        insert into raw_transactions
          (run_id, source_file, row_num, account_name_raw, trade_date, txn_type_raw, description_raw, amount_gbp)
        values
          ('RUN_1', 'transactions.csv', 1, 'Stocks & Shares ISA', '2026-01-01', 'Deposit', 'Bank transfer in', 250.0)
        """
    )

    clean_transactions(conn=conn, run_id="RUN_1")
    before = conn.execute(
        "select count(*) from stg_transactions where source_run_id = 'RUN_1'"
    ).fetchone()[0]
    assert before == 1

    conn.execute("delete from raw_transactions where run_id = 'RUN_1'")
    clean_transactions(conn=conn, run_id="RUN_1")
    after = conn.execute(
        "select count(*) from stg_transactions where source_run_id = 'RUN_1'"
    ).fetchone()[0]
    assert after == 0


def test_clean_holdings_clears_stale_rows_when_raw_rows_disappear() -> None:
    conn = connect_db(_workspace_scratch_dir("holdings_cleanup") / "holdings_cleanup.duckdb")
    ensure_schema(conn)
    conn.execute(
        """
        insert into raw_holdings_snapshot
          (run_id, source_file, row_num, account_name_raw, as_of_date, asset_name, market_value_gbp)
        values
          ('RUN_1', 'holdings.csv', 1, 'Stocks & Shares ISA', '2026-01-01', 'Test Fund', 1000.0)
        """
    )

    clean_holdings(conn=conn, run_id="RUN_1")
    before = conn.execute(
        "select count(*) from stg_asset_checkpoint where source_run_id = 'RUN_1'"
    ).fetchone()[0]
    assert before == 1

    conn.execute("delete from raw_holdings_snapshot where run_id = 'RUN_1'")
    clean_holdings(conn=conn, run_id="RUN_1")
    after = conn.execute(
        "select count(*) from stg_asset_checkpoint where source_run_id = 'RUN_1'"
    ).fetchone()[0]
    assert after == 0


def test_build_daily_flows_clears_stale_rows_when_transactions_disappear() -> None:
    conn = connect_db(_workspace_scratch_dir("flow_cleanup") / "flow_cleanup.duckdb")
    ensure_schema(conn)
    conn.execute(
        """
        insert into stg_transactions
          (account_id, event_date, txn_type, amount_gbp, flow_class, source_run_id, source_file, source_row_num)
        values
          ('ISA', '2026-01-02', 'deposit', 200.0, 'external_in', 'RUN_1', 'transactions.csv', 1)
        """
    )

    build_daily_flows(conn=conn, run_id="RUN_1")
    before = conn.execute(
        "select count(*) from stg_account_flow_daily where source_run_id = 'RUN_1'"
    ).fetchone()[0]
    assert before == 1

    conn.execute("delete from stg_transactions where source_run_id = 'RUN_1'")
    build_daily_flows(conn=conn, run_id="RUN_1")
    after = conn.execute(
        "select count(*) from stg_account_flow_daily where source_run_id = 'RUN_1'"
    ).fetchone()[0]
    assert after == 0


def test_interest_paid_is_classified_as_cash_interest_credit() -> None:
    txn_cfg = load_yaml("config/txn_type_map.yml")
    row = pd.Series(
        {
            "txn_type_raw": "Interest paid",
            "description_raw": "Cash interest paid",
        }
    )

    txn_type, flow_class = classify_transaction(row=row, rules_config=txn_cfg)

    assert txn_type == "interest"
    assert flow_class == "cash_interest"
    assert normalize_amount(5.0, flow_class) == 5.0


def test_build_stg_account_cost_daily_excludes_dividends_from_cash_interest() -> None:
    conn = connect_db(_workspace_scratch_dir("cost_drag") / "cost_drag.duckdb")
    ensure_schema(conn)
    conn.execute(
        """
        insert into stg_transactions
          (account_id, event_date, txn_type, amount_gbp, flow_class, source_run_id, source_file, source_row_num)
        values
          ('ISA', '2026-01-02', 'dividend', 5.0, 'dividend', 'RUN_1', 'transactions.csv', 1),
          ('ISA', '2026-01-03', 'interest', 4.0, 'cash_interest', 'RUN_1', 'transactions.csv', 2)
        """
    )

    out = build_stg_account_cost_daily(conn=conn, run_id="RUN_1")

    dividend_day = out[out["d"] == pd.Timestamp("2026-01-02").date()].iloc[0]
    interest_day = out[out["d"] == pd.Timestamp("2026-01-03").date()].iloc[0]

    assert float(dividend_day["cash_interest_gbp"]) == 0.0
    assert float(interest_day["cash_interest_gbp"]) == 4.0


def test_true_cash_interest_offsets_idle_cash_drag() -> None:
    account_daily = pd.DataFrame(
        [
            {"account_id": "ISA", "d": "2026-01-01"},
            {"account_id": "ISA", "d": "2026-01-02"},
        ]
    )
    account_cost = pd.DataFrame(
        [
            {"account_id": "ISA", "d": "2026-01-02", "fee_gbp": 0.0, "tax_gbp": 0.0, "cash_interest_gbp": 5.0},
        ]
    )
    account_cash = pd.DataFrame(
        [
            {"account_id": "ISA", "d": "2026-01-01", "cash_balance_gbp": 1000.0, "source_quality": "observed"},
            {"account_id": "ISA", "d": "2026-01-02", "cash_balance_gbp": 1000.0, "source_quality": "observed"},
        ]
    )

    account_out, _ = compute_cost_drag_tables(
        account_daily_df=account_daily,
        account_cost_df=account_cost,
        account_cash_df=account_cash,
        annual_cash_rate=0.365,
        periods_per_year=365,
        source_run_id="RUN_1",
    )

    day2 = account_out[account_out["d"] == pd.Timestamp("2026-01-02").date()].iloc[0]
    assert float(day2["cash_balance_prev_gbp"]) == 1000.0
    assert float(day2["idle_cash_drag_gbp"]) == 0.0
