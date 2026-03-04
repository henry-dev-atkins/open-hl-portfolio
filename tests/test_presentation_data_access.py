from pathlib import Path

from src.presentation import data_access


def _clear_data_access_caches() -> None:
    for fn in (
        data_access.get_connection,
        data_access.load_portfolio_daily,
        data_access.load_account_daily,
        data_access.load_accounts,
    ):
        clear = getattr(fn, "clear", None)
        if callable(clear):
            clear()


def test_connect_read_only_returns_none_for_missing_db(tmp_path: Path) -> None:
    missing_db = tmp_path / "missing.duckdb"

    assert data_access._connect_read_only(missing_db) is None


def test_load_portfolio_daily_returns_empty_for_missing_db(tmp_path: Path) -> None:
    missing_db = tmp_path / "missing.duckdb"

    _clear_data_access_caches()
    out = data_access.load_portfolio_daily(str(missing_db))

    assert out.empty
