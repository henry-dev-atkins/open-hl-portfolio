from __future__ import annotations

from urllib.parse import quote, quote_plus


YF_QUOTE_BASE = "https://finance.yahoo.com/quote"
HL_SEARCH_BASE = "https://online.hl.co.uk/my-accounts/stock_and_fund_search"


def build_yf_quote_url(ticker: str | None) -> str | None:
    value = str(ticker or "").strip()
    if not value:
        return None
    return f"{YF_QUOTE_BASE}/{quote(value, safe='')}"


def build_yf_history_url(ticker: str | None) -> str | None:
    value = str(ticker or "").strip()
    if not value:
        return None
    return f"{YF_QUOTE_BASE}/{quote(value, safe='')}/history"


def choose_hl_search_query(isin: str | None, sedol: str | None, asset_name_canonical: str) -> str:
    isin_v = str(isin or "").strip()
    if isin_v:
        return isin_v
    sedol_v = str(sedol or "").strip()
    if sedol_v:
        return sedol_v
    return str(asset_name_canonical or "").strip()


def build_hl_search_url(query: str | None) -> str:
    q = str(query or "").strip()
    return f"{HL_SEARCH_BASE}?search_data={quote_plus(q)}"
