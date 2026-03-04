from pathlib import Path
from uuid import uuid4

import pandas as pd

from src.common.db import connect_db, ensure_schema
from src.prices.resolve_asset_tickers import (
    _mapping_from_auto,
    _mapping_from_cached,
    _mapping_from_manual,
    _pick_auto_match,
    _query_variants,
    resolve_asset_tickers,
)


def _asset_row() -> pd.Series:
    return pd.Series(
        {
            "asset_id": "ASSET_ABC123",
            "asset_name_canonical": "vanguard s&p 500 ucits etf",
            "isin": None,
            "sedol": None,
        }
    )


def _workspace_scratch_dir(name: str) -> Path:
    root = Path("data") / "_phase1_test_tmp" / f"{name}_{uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    return root


def test_manual_override_hl_security_url_takes_precedence() -> None:
    row = _asset_row()
    out = _mapping_from_manual(
        asset_row=row,
        override={
            "yf_ticker": "VUSA.L",
            "hl_security_url": "https://www.hl.co.uk/example/security/vusa",
            "notes": "manual",
        },
        hl_search_url="https://online.hl.co.uk/my-accounts/stock_and_fund_search?search_data=VUSA",
        cached_hl_security_url="https://www.hl.co.uk/example/old",
        provider="yfinance",
    )
    assert out["ticker"] == "VUSA.L"
    assert out["match_status"] == "resolved"
    assert out["hl_security_url"] == "https://www.hl.co.uk/example/security/vusa"
    assert out["hl_link_source"] == "manual"
    assert out["yf_quote_url"] == "https://finance.yahoo.com/quote/VUSA.L"


def test_cached_mapping_reuses_hl_link_when_no_manual() -> None:
    row = _asset_row()
    out = _mapping_from_cached(
        asset_row=row,
        cached={
            "ticker": "VUSA.L",
            "currency": "GBP",
            "confidence_score": 97.5,
            "is_locked": True,
            "notes": "cached row",
            "hl_security_url": "https://www.hl.co.uk/example/security/vusa",
        },
        hl_search_url="https://online.hl.co.uk/my-accounts/stock_and_fund_search?search_data=VUSA",
        provider="yfinance",
    )
    assert out["mapping_source"] == "cached"
    assert out["hl_link_source"] == "cached"
    assert out["hl_security_url"] == "https://www.hl.co.uk/example/security/vusa"
    assert out["match_status"] == "resolved"


def test_unresolved_auto_still_has_hl_search_link() -> None:
    row = _asset_row()
    out = _mapping_from_auto(
        asset_row=row,
        auto_match=None,
        hl_search_url="https://online.hl.co.uk/my-accounts/stock_and_fund_search?search_data=vanguard",
        cached_hl_security_url=None,
        provider="yfinance",
    )
    assert out["match_status"] == "unresolved"
    assert out["ticker"] is None
    assert out["yf_quote_url"] is None
    assert out["hl_search_url"] == "https://online.hl.co.uk/my-accounts/stock_and_fund_search?search_data=vanguard"
    assert out["hl_link_source"] == "fallback_only"


def test_query_variants_remove_class_noise_and_keep_core_name() -> None:
    variants = _query_variants("L&G Global Technology Index (Acc)")
    assert "L&G Global Technology Index Acc" in variants
    assert "Legal & General Global Technology Index Acc" in variants
    assert any("Legal & General" in q for q in variants)
    assert any("Global Technology Index" in q for q in variants)


def test_query_variants_include_trimmed_ucits_core_name() -> None:
    variants = _query_variants("Global X Silver Miners UCITS ETF USD Acc (GBP)")
    assert any("Global X Silver Miners" in q for q in variants)


def test_pick_auto_match_accepts_strong_lse_candidate() -> None:
    candidates = [
        {"ticker": "VUSA.L", "score": 89.5, "rank": 1, "exchange": "LSE"},
        {"ticker": "VUSA.MI", "score": 82.0, "rank": 2, "exchange": "MIL"},
    ]
    out = _pick_auto_match(candidates)
    assert out is not None
    assert out["ticker"] == "VUSA.L"


def test_resolve_asset_tickers_preserves_unrelated_existing_mappings() -> None:
    scratch_dir = _workspace_scratch_dir("resolve_tickers")
    db_path = scratch_dir / "resolve_tickers.duckdb"
    asset_values_csv = scratch_dir / "asset_values.csv"
    unresolved_assets_csv = scratch_dir / "unresolved_assets.csv"
    unresolved_candidates_csv = scratch_dir / "unresolved_candidates.csv"

    conn = connect_db(db_path)
    ensure_schema(conn)
    conn.execute(
        """
        insert into asset_ticker_mapping
          (asset_id, provider, ticker, currency, confidence_score, match_status, mapping_source, is_locked)
        values
          ('OLD_ASSET', 'yfinance', 'OLD.L', 'GBP', 99.0, 'resolved', 'manual', true)
        """
    )

    pd.DataFrame(
        [
            {
                "asset_name": "New Asset",
                "report_date": "31 January 2026",
            }
        ]
    ).to_csv(asset_values_csv, index=False)

    mapping_rows, unresolved_rows, unresolved_candidate_rows = resolve_asset_tickers(
        conn=conn,
        run_date="2026-02-13",
        provider="yfinance",
        asset_values_csv=asset_values_csv,
        overrides_path="config/asset_ticker_overrides.yml",
        unresolved_assets_csv=unresolved_assets_csv,
        unresolved_candidates_csv=unresolved_candidates_csv,
        disable_auto_search=True,
    )

    assert mapping_rows == 1
    assert unresolved_rows == 1
    assert unresolved_candidate_rows == 0

    mappings = conn.execute(
        """
        select asset_id, ticker, match_status, is_locked
        from asset_ticker_mapping
        order by asset_id
        """
    ).df()
    assert len(mappings) == 2
    assert "OLD_ASSET" in set(mappings["asset_id"].tolist())
    old_row = mappings[mappings["asset_id"] == "OLD_ASSET"].iloc[0]
    new_row = mappings[mappings["asset_id"] != "OLD_ASSET"].iloc[0]

    assert old_row["ticker"] == "OLD.L"
    assert old_row["match_status"] == "resolved"
    assert bool(old_row["is_locked"]) is True
    assert pd.isna(new_row["ticker"])
    assert new_row["match_status"] == "unresolved"
