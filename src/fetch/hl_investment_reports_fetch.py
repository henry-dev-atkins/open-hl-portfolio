from __future__ import annotations

import argparse
import json
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

from playwright.sync_api import sync_playwright

from src.common.paths import PROJECT_ROOT


LOGIN_URL = "https://online.hl.co.uk/my-accounts/login"
PORTFOLIO_HISTORY_URL = "https://online.hl.co.uk/my-accounts/portfolio_history"
REPORT_TEXT_RE = re.compile(r"investment report", re.IGNORECASE)
ABSOLUTE_URL_RE = re.compile(r"https?://[^\s'\"<>]+", re.IGNORECASE)
RELATIVE_HL_PATH_RE = re.compile(r"/my-accounts/[^\s'\"<>]+", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download all HL Investment Report PDFs from portfolio_history."
    )
    parser.add_argument("--run-date", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for downloaded PDFs (default data/raw/<run-date>/investment_reports).",
    )
    parser.add_argument("--login-timeout-seconds", type=int, default=900)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--max-scroll-cycles", type=int, default=15)
    parser.add_argument("--manifest-path", default=None)
    return parser.parse_args()


def _safe_filename(text: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("_")
    return cleaned or "report"


def _same_hl_domain(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith("hl.co.uk")


def _ensure_active_page(context, page):
    try:
        if page is not None and not page.is_closed():
            return page
    except Exception:  # noqa: BLE001
        pass
    for p in reversed(context.pages):
        try:
            if not p.is_closed():
                return p
        except Exception:  # noqa: BLE001
            continue
    raise RuntimeError("No active browser page found.")


def _iter_frames(page) -> list:
    frames: list = []
    seen: set[int] = set()
    candidates = [page.main_frame] + list(page.frames)
    for frame in candidates:
        frame_id = id(frame)
        if frame_id in seen:
            continue
        seen.add(frame_id)
        frames.append(frame)
    return frames


def _wait_for_login(context, page, timeout_seconds: int) -> object:
    print()
    print("Manual login required.")
    print("1) Log in to HL in the opened browser (credentials + MFA).")
    print("2) Script continues automatically once account area is detected.")
    print()
    start = time.time()
    while True:
        if time.time() - start > timeout_seconds:
            raise TimeoutError(f"Login timed out after {timeout_seconds} seconds.")
        page = _ensure_active_page(context, page)
        try:
            url = (page.url or "").lower()
            body = (page.inner_text("body", timeout=1000) or "").lower()
            if (
                _same_hl_domain(url)
                and "login" not in url
                and ("my accounts" in body or "portfolio history" in body or "my-accounts" in url)
            ):
                print(f"Detected post-login page: {page.url}")
                return page
        except Exception:  # noqa: BLE001
            pass
        page.wait_for_timeout(1200)


def _ensure_portfolio_history_page(page) -> None:
    page.goto(PORTFOLIO_HISTORY_URL, wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(1500)
    if "portfolio_history" in (page.url or "").lower():
        return

    # Some sessions land on a neighbouring tab. Force click the Portfolio History tab.
    for selector in ("a[href*='portfolio_history']", "text=PORTFOLIO HISTORY", "text=Portfolio History"):
        locator = page.locator(selector)
        if locator.count() == 0:
            continue
        try:
            locator.first.click(timeout=5000)
            page.wait_for_timeout(1200)
            if "portfolio_history" in (page.url or "").lower():
                return
        except Exception:  # noqa: BLE001
            continue

    raise RuntimeError(f"Unable to reach portfolio history page. Current URL: {page.url}")


def _expand_report_toggles(page, max_clicks: int = 60) -> None:
    clicked = 0
    for frame in _iter_frames(page):
        for selector in (
            "button[aria-expanded='false']",
            "[role='button'][aria-expanded='false']",
            "summary",
            "button[title*='expand' i]",
            "button[aria-label*='expand' i]",
        ):
            if clicked >= max_clicks:
                return
            try:
                elements = frame.query_selector_all(selector)
            except Exception:  # noqa: BLE001
                continue
            for el in elements:
                if clicked >= max_clicks:
                    return
                try:
                    if not el.is_visible():
                        continue
                    el.click(timeout=700)
                    clicked += 1
                    page.wait_for_timeout(120)
                except Exception:  # noqa: BLE001
                    continue


def _scroll_report_container(page, max_cycles: int) -> None:
    for _ in range(max_cycles):
        scrolled = False
        for frame in _iter_frames(page):
            try:
                containers = frame.query_selector_all("div,ul,section")
            except Exception:  # noqa: BLE001
                continue
            for el in containers:
                try:
                    scroll_state = el.evaluate(
                        "el => ({h: el.scrollHeight, c: el.clientHeight, t: el.scrollTop})"
                    )
                except Exception:  # noqa: BLE001
                    continue
                if (
                    scroll_state["h"] > scroll_state["c"] + 8
                    and scroll_state["t"] < scroll_state["h"] - scroll_state["c"]
                ):
                    try:
                        el.evaluate("el => { el.scrollTop = el.scrollHeight; }")
                        scrolled = True
                    except Exception:  # noqa: BLE001
                        continue
        page.wait_for_timeout(250)
        if not scrolled:
            break


def _extract_url_from_js(source: str, base_url: str) -> str | None:
    text = (source or "").strip()
    if not text:
        return None

    abs_match = ABSOLUTE_URL_RE.search(text)
    if abs_match:
        return abs_match.group(0)

    rel_match = RELATIVE_HL_PATH_RE.search(text)
    if rel_match:
        return urljoin(base_url, rel_match.group(0))

    quoted = re.findall(r"""['"]([^'"]+)['"]""", text)
    for token in quoted:
        token = token.strip()
        if not token:
            continue
        if token.lower().startswith("http://") or token.lower().startswith("https://"):
            return token
        if token.startswith("/"):
            return urljoin(base_url, token)
    return None


def _resolve_report_url(base_url: str, href: str, onclick: str) -> str | None:
    href = (href or "").strip()
    if href and href not in {"#", "javascript:void(0);", "javascript:void(0)"}:
        if href.lower().startswith("javascript:"):
            maybe = _extract_url_from_js(href, base_url)
            if maybe:
                return maybe
        else:
            return urljoin(base_url, href)

    if onclick:
        return _extract_url_from_js(onclick, base_url)
    return None


def _collect_report_links(page, max_scroll_cycles: int) -> list[tuple[str, str]]:
    found: dict[str, str] = {}
    _expand_report_toggles(page)
    _scroll_report_container(page, max_cycles=max_scroll_cycles)
    for frame in _iter_frames(page):
        try:
            anchors = frame.query_selector_all("a")
        except Exception:  # noqa: BLE001
            continue
        base_url = frame.url or page.url
        for anchor in anchors:
            try:
                text = (anchor.inner_text() or "").strip()
                href = (anchor.get_attribute("href") or "").strip()
                onclick = (anchor.get_attribute("onclick") or "").strip()
            except Exception:  # noqa: BLE001
                continue
            if not (text or href or onclick):
                continue
            haystack = f"{text} {href} {onclick}".lower()
            if not REPORT_TEXT_RE.search(haystack):
                continue
            full_url = _resolve_report_url(base_url=base_url, href=href, onclick=onclick)
            if not full_url or not _same_hl_domain(full_url):
                continue
            label = text or Path(urlparse(full_url).path).name or "investment_report"
            found[full_url] = label
    return [(url, text) for url, text in found.items()]


def _dump_debug_state(page, output_dir: Path) -> str:
    debug_dir = output_dir / "_debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    anchors_file = debug_dir / "portfolio_history_anchors.txt"
    html_file = debug_dir / "portfolio_history_main.html"

    anchor_lines: list[str] = []
    for frame in _iter_frames(page):
        frame_url = frame.url
        anchor_lines.append(f"FRAME: {frame_url}")
        try:
            for a in frame.query_selector_all("a"):
                text = (a.inner_text() or "").strip()
                href = (a.get_attribute("href") or "").strip()
                onclick = (a.get_attribute("onclick") or "").strip()
                if text or href or onclick:
                    anchor_lines.append(f"- {text} | href={href} | onclick={onclick}")
        except Exception:  # noqa: BLE001
            anchor_lines.append("- <failed to inspect anchors>")
    anchors_file.write_text("\n".join(anchor_lines), encoding="utf-8")

    try:
        html_file.write_text(page.content(), encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass
    return str(debug_dir)


def _download_pdf(context, url: str, path: Path) -> bool:
    try:
        response = context.request.get(url, timeout=30000)
    except Exception:  # noqa: BLE001
        return False

    if not response.ok:
        return False
    body = response.body()
    ctype = (response.headers.get("content-type") or "").lower()
    is_pdf = body[:4] == b"%PDF" or "pdf" in ctype
    if not is_pdf:
        return False
    path.write_bytes(body)
    return True


def _download_via_fallback_navigation(context, url: str, file_path: Path) -> bool:
    dl_page = context.new_page()
    try:
        with dl_page.expect_download(timeout=8000) as dl_info:
            dl_page.goto(url, wait_until="domcontentloaded", timeout=30000)
        dl = dl_info.value
        dl.save_as(str(file_path))
        return True
    except Exception:  # noqa: BLE001
        return False
    finally:
        try:
            dl_page.close()
        except Exception:  # noqa: BLE001
            pass


def _download_reports_for_account(context, account_label: str, links: list[tuple[str, str]], output_dir: Path) -> list[str]:
    downloaded: list[str] = []
    for url, report_text in links:
        report_name = _safe_filename(report_text)
        account_name = _safe_filename(account_label)
        file_name = f"{account_name}__{report_name}.pdf"
        file_path = output_dir / file_name
        if file_path.exists():
            downloaded.append(str(file_path))
            continue
        if _download_pdf(context, url, file_path):
            downloaded.append(str(file_path))
            continue
        if _download_via_fallback_navigation(context, url, file_path):
            downloaded.append(str(file_path))
    return downloaded


def _write_manifest(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else (PROJECT_ROOT / "data" / "raw" / args.run_date / "investment_reports")
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = (
        Path(args.manifest_path)
        if args.manifest_path
        else (PROJECT_ROOT / "data" / "raw" / args.run_date / "investment_reports_manifest.json")
    )

    per_account_counts: dict[str, int] = {}
    downloaded_all: list[str] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
        page = _wait_for_login(context=context, page=page, timeout_seconds=args.login_timeout_seconds)

        _ensure_portfolio_history_page(page)
        page = _ensure_active_page(context, page)
        print(f"Collecting report links from: {page.url}")
        links = _collect_report_links(page, max_scroll_cycles=args.max_scroll_cycles)
        print(f"Found report links: {len(links)}")
        if not links:
            debug_dir = _dump_debug_state(page, output_dir=output_dir)
            print(f"No report links detected on portfolio_history. Debug artifacts: {debug_dir}")
        files = _download_reports_for_account(
            context=context,
            account_label="portfolio_history",
            links=links,
            output_dir=output_dir,
        )
        per_account_counts["portfolio_history"] = len(files)
        downloaded_all.extend(files)

        context.close()
        browser.close()

    downloaded_all = list(dict.fromkeys(downloaded_all))
    payload = {
        "run_date": args.run_date,
        "generated_at": datetime.now(UTC).isoformat(),
        "portfolio_history_url": PORTFOLIO_HISTORY_URL,
        "output_dir": str(output_dir.resolve()),
        "download_count": len(downloaded_all),
        "per_account_counts": per_account_counts,
        "files": downloaded_all,
    }
    _write_manifest(manifest_path, payload)
    print(f"Downloaded investment reports: {len(downloaded_all)}")
    print(f"Manifest: {manifest_path}")
    if len(downloaded_all) == 0:
        raise RuntimeError("No investment report PDFs were downloaded.")


if __name__ == "__main__":
    main()
