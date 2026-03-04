from src.fetch.hl_investment_reports_fetch import _extract_url_from_js, _resolve_report_url


def test_extract_url_from_js_relative_path() -> None:
    source = "javascript:window.open('/my-accounts/portfolio_history/reports/download?id=123')"
    out = _extract_url_from_js(source, "https://online.hl.co.uk/my-accounts/portfolio_history")
    assert out == "https://online.hl.co.uk/my-accounts/portfolio_history/reports/download?id=123"


def test_extract_url_from_js_absolute_url() -> None:
    source = "openReport('https://online.hl.co.uk/my-accounts/reporting/file.pdf')"
    out = _extract_url_from_js(source, "https://online.hl.co.uk/my-accounts/portfolio_history")
    assert out == "https://online.hl.co.uk/my-accounts/reporting/file.pdf"


def test_resolve_report_url_from_javascript_href() -> None:
    url = _resolve_report_url(
        base_url="https://online.hl.co.uk/my-accounts/portfolio_history",
        href="javascript:window.open('/my-accounts/secure/report.pdf')",
        onclick="",
    )
    assert url == "https://online.hl.co.uk/my-accounts/secure/report.pdf"


def test_resolve_report_url_prefers_normal_href() -> None:
    url = _resolve_report_url(
        base_url="https://online.hl.co.uk/my-accounts/portfolio_history",
        href="/my-accounts/secure/report.pdf",
        onclick="window.open('/my-accounts/other.pdf')",
    )
    assert url == "https://online.hl.co.uk/my-accounts/secure/report.pdf"
