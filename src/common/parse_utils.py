from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

import pandas as pd


def normalize_col(col_name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(col_name).strip().lower())
    normalized = normalized.strip("_")
    return normalized


def coerce_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, dayfirst=True, errors="coerce").dt.date


def coerce_decimal(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("£", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.replace("(", "-", regex=False)
        .str.replace(")", "", regex=False)
        .str.strip()
    )
    cleaned = cleaned.replace({"": None, "nan": None, "None": None})
    return pd.to_numeric(cleaned, errors="coerce")


def pick_column(df: pd.DataFrame, aliases: Iterable[str], required: bool = False) -> pd.Series | None:
    normalized_cols = {normalize_col(c): c for c in df.columns}
    for alias in aliases:
        candidate = normalized_cols.get(normalize_col(alias))
        if candidate:
            return df[candidate]
    if required:
        raise ValueError(f"Missing required column alias from {list(aliases)}")
    return None


def load_csv_flexible(path: Path) -> pd.DataFrame:
    encodings = ["utf-8-sig", "utf-16", "cp1252", "latin-1"]
    last_error: Exception | None = None
    for enc in encodings:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
    raise ValueError(f"Could not parse CSV {path}: {last_error}") from last_error
