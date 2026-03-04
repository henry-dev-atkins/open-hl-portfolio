from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from src.common.paths import DEFAULT_SCHEMA_PATH


def _asset_ticker_mapping_pk_columns(conn: duckdb.DuckDBPyConnection) -> list[str]:
    rows = conn.execute(
        """
        select k.column_name
        from information_schema.table_constraints t
        join information_schema.key_column_usage k
          on t.constraint_name = k.constraint_name
         and t.table_schema = k.table_schema
         and t.table_name = k.table_name
        where t.table_schema = 'main'
          and t.table_name = 'asset_ticker_mapping'
          and t.constraint_type = 'PRIMARY KEY'
        order by k.ordinal_position
        """
    ).fetchall()
    return [str(row[0]) for row in rows]


def _migrate_asset_ticker_mapping_to_provider_key(conn: duckdb.DuckDBPyConnection) -> None:
    pk_columns = _asset_ticker_mapping_pk_columns(conn)
    if pk_columns == ["asset_id", "provider"]:
        return
    if pk_columns != ["asset_id"]:
        return

    conn.execute(
        """
        create table if not exists asset_ticker_mapping__new (
          asset_id varchar not null,
          provider varchar not null,
          ticker varchar,
          currency varchar,
          confidence_score decimal(6, 2),
          match_status varchar not null,
          mapping_source varchar,
          is_locked boolean default false,
          matched_at timestamp,
          notes varchar,
          yf_quote_url varchar,
          yf_history_url varchar,
          hl_security_url varchar,
          hl_search_url varchar,
          hl_link_source varchar,
          primary key (asset_id, provider)
        )
        """
    )
    conn.execute("insert into asset_ticker_mapping__new select * from asset_ticker_mapping")
    conn.execute("drop table asset_ticker_mapping")
    conn.execute("alter table asset_ticker_mapping__new rename to asset_ticker_mapping")


def connect_db(db_path: Path | str) -> duckdb.DuckDBPyConnection:
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_file))


def ensure_schema(
    conn: duckdb.DuckDBPyConnection, schema_path: Path | str | None = None
) -> None:
    schema_file = Path(schema_path) if schema_path else DEFAULT_SCHEMA_PATH
    sql_text = schema_file.read_text(encoding="utf-8")
    conn.execute(sql_text)
    _migrate_asset_ticker_mapping_to_provider_key(conn)


def upsert_dataframe(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    df: pd.DataFrame,
    delete_where: str | None = None,
) -> None:
    if df.empty:
        return
    temp_name = f"tmp_{table_name}"
    conn.register(temp_name, df)
    if delete_where:
        conn.execute(f"delete from {table_name} where {delete_where}")  # noqa: S608
    conn.execute(f"insert into {table_name} select * from {temp_name}")  # noqa: S608
    conn.unregister(temp_name)


def overwrite_table(conn: duckdb.DuckDBPyConnection, table_name: str, df: pd.DataFrame) -> None:
    conn.execute(f"delete from {table_name}")  # noqa: S608
    if df.empty:
        return
    temp_name = f"tmp_{table_name}"
    conn.register(temp_name, df)
    conn.execute(f"insert into {table_name} select * from {temp_name}")  # noqa: S608
    conn.unregister(temp_name)


def fetch_latest_run_id(conn: duckdb.DuckDBPyConnection) -> str | None:
    row = conn.execute(
        """
        select run_id
        from raw_import_runs
        order by imported_at desc
        limit 1
        """
    ).fetchone()
    if not row:
        return None
    return row[0]


def query_df(conn: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> pd.DataFrame:
    if params:
        return conn.execute(sql, params).df()
    return conn.execute(sql).df()
