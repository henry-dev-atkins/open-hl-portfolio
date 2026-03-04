from pathlib import Path

from src.common.db import connect_db, ensure_schema


def test_ensure_schema_migrates_asset_ticker_mapping_to_provider_key(tmp_path: Path) -> None:
    db_path = tmp_path / "schema_migration.duckdb"
    conn = connect_db(db_path)

    conn.execute(
        """
        create table asset_ticker_mapping (
          asset_id varchar primary key,
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
          hl_link_source varchar
        )
        """
    )
    conn.execute(
        """
        insert into asset_ticker_mapping
          (asset_id, provider, ticker, currency, match_status, mapping_source, is_locked)
        values
          ('ASSET_1', 'yfinance', 'AAA.L', 'GBP', 'resolved', 'manual', true)
        """
    )

    ensure_schema(conn)

    pk_columns = conn.execute(
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
    assert [row[0] for row in pk_columns] == ["asset_id", "provider"]

    conn.execute(
        """
        insert into asset_ticker_mapping
          (asset_id, provider, ticker, currency, match_status, mapping_source, is_locked)
        values
          ('ASSET_1', 'other_provider', 'AAA.OT', 'USD', 'resolved', 'manual', false)
        """
    )

    rows = conn.execute(
        """
        select asset_id, provider, ticker
        from asset_ticker_mapping
        where asset_id = 'ASSET_1'
        order by provider
        """
    ).fetchall()
    assert rows == [
        ("ASSET_1", "other_provider", "AAA.OT"),
        ("ASSET_1", "yfinance", "AAA.L"),
    ]
