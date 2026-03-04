from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.parse_utils import coerce_date, coerce_decimal, load_csv_flexible, pick_column


def parse_transactions_file(path: Path, run_id: str, source_file: str | None = None) -> pd.DataFrame:
    raw_df = load_csv_flexible(path)
    if raw_df.empty:
        raise ValueError(f"Transactions file is empty: {path}")

    account_name = pick_column(raw_df, ["account", "account_name", "portfolio"], required=True)
    trade_date = pick_column(raw_df, ["date", "trade_date", "transaction_date"], required=True)
    txn_type = pick_column(raw_df, ["type", "transaction_type", "transaction"], required=True)
    settle_date = pick_column(raw_df, ["settle_date", "settlement_date", "value_date"], required=False)
    account_id = pick_column(raw_df, ["account_id", "account_number"], required=False)
    description = pick_column(raw_df, ["description", "details", "narrative"], required=False)
    amount = pick_column(raw_df, ["amount", "amount_gbp", "value", "cash_amount"], required=False)
    debit = pick_column(raw_df, ["debit", "money_out"], required=False)
    credit = pick_column(raw_df, ["credit", "money_in"], required=False)
    currency = pick_column(raw_df, ["currency"], required=False)
    instrument = pick_column(raw_df, ["instrument", "security", "stock", "fund"], required=False)
    isin = pick_column(raw_df, ["isin"], required=False)
    sedol = pick_column(raw_df, ["sedol"], required=False)
    units = pick_column(raw_df, ["units", "quantity"], required=False)
    price = pick_column(raw_df, ["price"], required=False)
    balance = pick_column(raw_df, ["balance", "running_balance", "cash_balance"], required=False)

    if amount is None and debit is None and credit is None:
        raise ValueError(f"Could not identify amount/debit/credit columns in transactions file: {path}")

    amount_numeric = coerce_decimal(amount) if amount is not None else None
    if amount_numeric is None:
        debit_numeric = coerce_decimal(debit) if debit is not None else 0
        credit_numeric = coerce_decimal(credit) if credit is not None else 0
        amount_numeric = credit_numeric.fillna(0) - debit_numeric.fillna(0)

    out = pd.DataFrame(
        {
            "run_id": run_id,
            "source_file": source_file or path.name,
            "row_num": range(1, len(raw_df) + 1),
            "account_name_raw": account_name.astype(str).str.strip(),
            "account_id": account_id.astype(str).str.strip() if account_id is not None else None,
            "trade_date": coerce_date(trade_date),
            "settle_date": coerce_date(settle_date) if settle_date is not None else None,
            "txn_type_raw": txn_type.astype(str).str.strip(),
            "description_raw": description.astype(str).str.strip() if description is not None else None,
            "amount_gbp": amount_numeric,
            "currency": currency.astype(str).str.strip() if currency is not None else "GBP",
            "instrument_name": instrument.astype(str).str.strip() if instrument is not None else None,
            "isin": isin.astype(str).str.strip() if isin is not None else None,
            "sedol": sedol.astype(str).str.strip() if sedol is not None else None,
            "units": coerce_decimal(units) if units is not None else None,
            "price": coerce_decimal(price) if price is not None else None,
            "balance_after_gbp": coerce_decimal(balance) if balance is not None else None,
        }
    )

    out = out[out["trade_date"].notna() & out["txn_type_raw"].notna()].copy()
    if out.empty:
        raise ValueError(f"No valid transaction rows found in {path}")

    return out[
        [
            "run_id",
            "source_file",
            "row_num",
            "account_name_raw",
            "account_id",
            "trade_date",
            "settle_date",
            "txn_type_raw",
            "description_raw",
            "amount_gbp",
            "currency",
            "instrument_name",
            "isin",
            "sedol",
            "units",
            "price",
            "balance_after_gbp",
        ]
    ]
