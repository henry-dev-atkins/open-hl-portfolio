from src.prices.links import (
    build_hl_search_url,
    build_yf_history_url,
    build_yf_quote_url,
    choose_hl_search_query,
)


def test_yahoo_links_for_standard_ticker() -> None:
    assert build_yf_quote_url("VUSA.L") == "https://finance.yahoo.com/quote/VUSA.L"
    assert build_yf_history_url("VUSA.L") == "https://finance.yahoo.com/quote/VUSA.L/history"


def test_hl_search_url_encodes_query() -> None:
    url = build_hl_search_url("L&G Global Technology Index (Acc)")
    assert (
        url
        == "https://online.hl.co.uk/my-accounts/stock_and_fund_search?search_data=L%26G+Global+Technology+Index+%28Acc%29"
    )


def test_choose_hl_search_query_priority() -> None:
    assert choose_hl_search_query("IE00B3XXRP09", "B3XXRP0", "fallback name") == "IE00B3XXRP09"
    assert choose_hl_search_query(None, "B3XXRP0", "fallback name") == "B3XXRP0"
    assert choose_hl_search_query(None, None, "fallback name") == "fallback name"
