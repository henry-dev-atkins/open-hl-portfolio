from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "marts" / "hl_portfolio.duckdb"
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema.sql"


def get_db_path(db_path: str | None = None) -> Path:
    if db_path:
        return Path(db_path)
    env_path = os.getenv("HL_DB_PATH")
    if env_path:
        return Path(env_path)
    return DEFAULT_DB_PATH


def get_data_dir(data_dir: str | None = None) -> Path:
    if data_dir:
        return Path(data_dir)
    env_dir = os.getenv("HL_DATA_DIR")
    if env_dir:
        return Path(env_dir)
    return DEFAULT_DATA_DIR
