from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from src.common.paths import PROJECT_ROOT


BASE_URL = "https://online.hl.co.uk"
LOGIN_URLS = [
    f"{BASE_URL}/my-accounts/login",
    "https://www.hl.co.uk/",
]
TARGET_PATHS = [
    "/my-accounts",
]
HISTORY_RE = re.compile(r"(portfolio|investment|transaction|history|statement|valuation)", re.IGNORECASE)
DOWNLOAD_RE = re.compile(r"(download|export|csv)", re.IGNORECASE)
LOGOUT_RE = re.compile(r"(log\s*out|sign\s*out|logout)", re.IGNORECASE)
DIRECT_CSV_RE = re.compile(
    r"investment_history_csv/account/(?P<account>\d+)/view/(?P<view>[A-Z]+)",
    re.IGNORECASE,
)
FILTER_BUTTON_RE = re.compile(r"(go|apply|search|update|show)", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Automate HL historical transaction CSV downloads after manual login."
    )
    parser.add_argument("--run-date", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--output-dir", default=None, help="Output directory for downloaded CSV files.")
    parser.add_argument("--start-url", default=LOGIN_URLS[0], help="Initial URL.")
    parser.add_argument("--headless", action="store_true", help="Run browser headless (not recommended for login).")
    parser.add_argument("--max-pages", type=int, default=20, help="Max history pages to visit.")
    parser.add_argument("--click-delay-ms", type=int, default=2000, help="Delay after each click attempt.")
    parser.add_argument("--login-timeout-seconds", type=int, default=900, help="Max wait for manual login confirmation.")
    parser.add_argument("--start-date", default="2000-01-01", help="Earliest history date for export URLs.")
    parser.add_argument("--end-date", default=datetime.now(UTC).strftime("%Y-%m-%d"), help="Latest history date.")
    parser.add_argument("--manifest-path", default=None, help="Optional path for download manifest json.")
    parser.add_argument("--run-pipeline", action="store_true", help="Run marts pipeline after download completes.")
    parser.add_argument("--db-path", default="data/marts/hl_portfolio.duckdb", help="DuckDB path for pipeline run.")
    return parser.parse_args()


def _safe_filename(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_")
    return cleaned or "download.csv"


def _next_available_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    n = 2
    while True:
        candidate = parent / f"{stem}_{n}{suffix}"
        if not candidate.exists():
            return candidate
        n += 1


def _same_hl_domain(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith("hl.co.uk")


def _ensure_active_page(context, page):
    try:
        if page is not None and not page.is_closed():
            return page
    except Exception:  # noqa: BLE001
        pass

    for candidate in reversed(context.pages):
        try:
            if not candidate.is_closed():
                return candidate
        except Exception:  # noqa: BLE001
            continue
    raise RuntimeError("No active browser page found. Browser may have been closed.")


def _extract_history_links(page, base_url: str) -> list[str]:
    links: list[str] = []
    for anchor in page.query_selector_all("a[href]"):
        try:
            href = anchor.get_attribute("href") or ""
            text = (anchor.inner_text() or "").strip()
        except Exception:  # noqa: BLE001
            continue
        if not href:
            continue
        full = urljoin(base_url, href)
        target = f"{text} {full}".strip()
        if not _same_hl_domain(full):
            continue
        if LOGOUT_RE.search(target):
            continue
        if HISTORY_RE.search(target):
            links.append(full)
    return list(dict.fromkeys(links))


def _save_download(download, output_dir: Path) -> str:
    suggested = _safe_filename(download.suggested_filename)
    save_path = _next_available_path(output_dir / suggested)
    download.save_as(str(save_path))
    return str(save_path)


def _click_download_controls(page, output_dir: Path, click_delay_ms: int) -> list[str]:
    saved_paths: list[str] = []
    attempted: set[str] = set()

    elements = page.query_selector_all("a,button,[role='button'],[role='menuitem']")
    for el in elements:
        try:
            text = (el.inner_text() or "").strip()
            href = el.get_attribute("href") or ""
            visible = bool(el.is_visible())
        except Exception:  # noqa: BLE001
            continue

        signature = f"{text}|{href}"
        if signature in attempted:
            continue
        attempted.add(signature)

        candidate = f"{text} {href}".strip()
        if LOGOUT_RE.search(candidate):
            continue
        if not DOWNLOAD_RE.search(candidate):
            continue
        if not visible:
            continue

        try:
            with page.expect_download(timeout=4000) as download_info:
                el.click(timeout=3000)
            saved_paths.append(_save_download(download_info.value, output_dir))
            page.wait_for_timeout(click_delay_ms)
        except PlaywrightTimeoutError:
            continue
        except Exception:  # noqa: BLE001
            continue

    return saved_paths


def _extract_direct_csv_urls(page_html: str, start_date: str, end_date: str) -> list[str]:
    urls: list[str] = []
    for match in DIRECT_CSV_RE.finditer(page_html):
        account = match.group("account")
        view = match.group("view").upper()
        urls.append(
            f"{BASE_URL}/my-accounts/investment_history_csv/account/{account}/view/{view}/"
            f"page/1/func/download/startDate/{start_date}/endDate/{end_date}"
        )
    return list(dict.fromkeys(urls))


def _download_direct_urls(page, urls: list[str], output_dir: Path, click_delay_ms: int) -> list[str]:
    saved_paths: list[str] = []
    for url in urls:
        try:
            with page.expect_download(timeout=7000) as download_info:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
            saved_paths.append(_save_download(download_info.value, output_dir))
            page.wait_for_timeout(click_delay_ms)
        except PlaywrightTimeoutError:
            continue
        except Exception:  # noqa: BLE001
            continue
    return saved_paths


def _set_date_range_if_present(page, start_date: str, end_date: str) -> None:
    for inp in page.query_selector_all("input"):
        try:
            field_id = (inp.get_attribute("id") or "").lower()
            field_name = (inp.get_attribute("name") or "").lower()
            field_type = (inp.get_attribute("type") or "").lower()
            key = f"{field_id} {field_name}"
        except Exception:  # noqa: BLE001
            continue

        if field_type not in {"date", "text"}:
            continue
        try:
            if any(k in key for k in ["start", "from"]):
                inp.fill(start_date, timeout=1000)
            elif any(k in key for k in ["end", "to"]):
                inp.fill(end_date, timeout=1000)
        except Exception:  # noqa: BLE001
            continue


def _click_filter_buttons(page) -> None:
    for el in page.query_selector_all("button,input[type='submit'],a"):
        try:
            text = (el.inner_text() or "").strip()
        except Exception:  # noqa: BLE001
            try:
                text = (el.get_attribute("value") or "").strip()
            except Exception:  # noqa: BLE001
                text = ""
        if not text:
            continue
        if LOGOUT_RE.search(text):
            continue
        if not FILTER_BUTTON_RE.search(text):
            continue
        try:
            if el.is_visible():
                el.click(timeout=1500)
                page.wait_for_timeout(500)
        except Exception:  # noqa: BLE001
            continue


def _select_account_filters_if_present(page) -> None:
    for sel in page.query_selector_all("select"):
        try:
            sel_id = (sel.get_attribute("id") or "").lower()
            sel_name = (sel.get_attribute("name") or "").lower()
            signature = f"{sel_id} {sel_name}"
        except Exception:  # noqa: BLE001
            continue
        if not any(k in signature for k in ["account", "portfolio", "view"]):
            continue

        for opt in sel.query_selector_all("option"):
            try:
                value = (opt.get_attribute("value") or "").strip()
                label = (opt.inner_text() or "").strip()
            except Exception:  # noqa: BLE001
                continue
            if not value:
                continue
            if re.search(r"(all|select|choose|any)", label, re.IGNORECASE):
                continue
            try:
                sel.select_option(value=value, timeout=2000)
                page.wait_for_timeout(700)
            except Exception:  # noqa: BLE001
                continue


def _wait_for_manual_login(page, timeout_seconds: int) -> None:
    print()
    print("Manual login required.")
    print("1) Log in on the opened browser window (credentials + MFA).")
    print("2) Script will continue automatically once an account page is detected.")
    print()

    start = time.time()
    while True:
        if time.time() - start > timeout_seconds:
            raise TimeoutError(f"Manual login confirmation timed out after {timeout_seconds} seconds.")
        try:
            current_url = (page.url or "").lower()
            if _same_hl_domain(current_url) and "login" not in current_url:
                body = (page.inner_text("body", timeout=1000) or "").lower()
                if (
                    ("my account" in body)
                    or ("my accounts" in body)
                    or ("portfolio" in body)
                    or ("account overview" in body)
                    or ("my-accounts" in current_url)
                ):
                    print(f"Detected post-login page: {page.url}")
                    return
        except Exception:  # noqa: BLE001
            pass
        try:
            page.wait_for_timeout(1500)
        except Exception:  # noqa: BLE001
            time.sleep(1)


def _initial_target_urls(page) -> list[str]:
    queue: list[str] = []
    if _same_hl_domain(page.url):
        queue.append(page.url)
    queue.extend([urljoin(BASE_URL, p) for p in TARGET_PATHS])
    queue.extend(_extract_history_links(page, base_url=page.url))
    return list(dict.fromkeys(queue))


def _targeted_history_download(
    context,
    page,
    output_dir: Path,
    max_pages: int,
    click_delay_ms: int,
    start_date: str,
    end_date: str,
) -> tuple[list[str], list[str]]:
    visited: set[str] = set()
    queue: list[str] = _initial_target_urls(page)
    direct_urls_seen: set[str] = set()
    downloads: list[str] = []

    while queue and len(visited) < max_pages:
        try:
            page = _ensure_active_page(context, page)
        except RuntimeError:
            break

        url = queue.pop(0)
        if url in visited:
            continue
        if not _same_hl_domain(url):
            continue
        if LOGOUT_RE.search(url):
            continue
        visited.add(url)

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(1200)
        except Exception:  # noqa: BLE001
            continue
        try:
            page = _ensure_active_page(context, page)
        except RuntimeError:
            break

        _set_date_range_if_present(page, start_date=start_date, end_date=end_date)
        _click_filter_buttons(page)
        _select_account_filters_if_present(page)

        downloads.extend(_click_download_controls(page, output_dir=output_dir, click_delay_ms=click_delay_ms))

        try:
            html = page.content()
        except Exception:  # noqa: BLE001
            html = ""
        direct_urls = _extract_direct_csv_urls(html, start_date=start_date, end_date=end_date)
        direct_urls = [u for u in direct_urls if u not in direct_urls_seen]
        direct_urls_seen.update(direct_urls)
        if direct_urls:
            downloads.extend(
                _download_direct_urls(
                    page=page,
                    urls=direct_urls,
                    output_dir=output_dir,
                    click_delay_ms=click_delay_ms,
                )
            )
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
            except Exception:  # noqa: BLE001
                pass
            try:
                page = _ensure_active_page(context, page)
            except RuntimeError:
                break

        for link in _extract_history_links(page, base_url=page.url):
            if link not in visited and link not in queue:
                queue.append(link)

    return list(dict.fromkeys(downloads)), sorted(visited)


def _write_manifest(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _run_pipeline(run_date: str, db_path: str) -> int:
    cmd = [
        "powershell",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(PROJECT_ROOT / "scripts" / "run_all.ps1"),
        "-RunDate",
        run_date,
        "-DbPath",
        db_path,
    ]
    result = subprocess.run(cmd, check=False)
    return int(result.returncode)


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir) if args.output_dir else (PROJECT_ROOT / "data" / "raw" / args.run_date)
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = (
        Path(args.manifest_path)
        if args.manifest_path
        else (PROJECT_ROOT / "data" / "raw" / args.run_date / "download_manifest.json")
    )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        page.goto(args.start_url, wait_until="domcontentloaded", timeout=30000)
        _wait_for_manual_login(page, timeout_seconds=args.login_timeout_seconds)
        page = _ensure_active_page(context, page)

        downloads, visited_pages = _targeted_history_download(
            context=context,
            page=page,
            output_dir=output_dir,
            max_pages=args.max_pages,
            click_delay_ms=args.click_delay_ms,
            start_date=args.start_date,
            end_date=args.end_date,
        )

        context.close()
        browser.close()

    payload = {
        "run_date": args.run_date,
        "start_url": args.start_url,
        "generated_at": datetime.now(UTC).isoformat(),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "output_dir": str(output_dir.resolve()),
        "download_count": len(downloads),
        "downloads": downloads,
        "visited_pages": visited_pages,
    }
    _write_manifest(manifest_path, payload)

    print(f"Downloaded files: {len(downloads)}")
    print(f"Manifest: {manifest_path}")

    if len(downloads) == 0:
        raise RuntimeError(
            "No CSV downloads were captured. Try a larger --max-pages, and ensure your account has transaction history."
        )

    if args.run_pipeline:
        rc = _run_pipeline(run_date=args.run_date, db_path=args.db_path)
        if rc != 0:
            raise RuntimeError(f"Pipeline run failed with exit code {rc}")


if __name__ == "__main__":
    main()
