from __future__ import annotations

import argparse
import re
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from pypdf import PdfReader

from src.common.paths import PROJECT_ROOT


# Some HL PDFs extract the GBP symbol as U+00A3, U+0141, or prefixed variants.
CURRENCY_TOKEN_RE = re.compile(r"(\u00a3|\u0141|\u00c2\u00a3|\u00c2\u0141)")
MONEY_RE = re.compile(r"(?:\u00a3|\u0141|\u00c2\u00a3|\u00c2\u0141)\s*([0-9][0-9,]*\.?[0-9]{0,2})")
SEASON_RE = re.compile(r"\b(Spring|Summer|Autumn|Winter)\s+(\d{4})\b", re.IGNORECASE)
DATE_RE = re.compile(r"\b(\d{1,2}\s+[A-Za-z]+\s+\d{4})\b")
OVERVIEW_ROW_RE = re.compile(
    r"^(?P<account>[A-Za-z&' \-]+?)\s+"
    r"(?P<current>[0-9][0-9,]*)\s+"
    r"(?P<previous>[0-9][0-9,]*)\s+"
    r"(?P<change>[0-9][0-9,]*)$"
)
TXN_DATE_RE = re.compile(r"(?P<date>\d{2}/\d{2}/\d{4})")
ASSET_VALUE_LEAD_RE = re.compile(r"^\s*(?P<value>[0-9][0-9,]*)\b")
ASSET_NAME_SUFFIX_RE = re.compile(r"(?P<asset>[A-Za-z][A-Za-z0-9&'().,%/+ \-]{2,})$")
ACCOUNT_HEADER_RE = re.compile(
    r"^\-+\s*(?P<account>[A-Za-z&' \-]+?)\s*(?:CAPITAL ACCOUNT TRANSACTIONS|DETAILED VALUATION|$)",
    re.IGNORECASE,
)
DETAILED_VALUATION_HEADER_RE = re.compile(
    r"^\-+\s*(?P<account>[A-Za-z&' \-]+?)\s*DETAILED\s+VALUATION",
    re.IGNORECASE,
)
TXN_LINE_RE_LEAD_AMOUNT = re.compile(
    r"^(?P<amount>\(?[0-9][0-9,]*\.[0-9]{2}\)?)\s*"
    r"(?P<description>.+?)\s*"
    r"(?P<date>\d{2}/\d{2}/\d{4})\s*"
    r"(?P<balance>\(?[0-9][0-9,]*\.[0-9]{2}\)?)$"
)
TXN_LINE_RE_TRAIL_AMOUNT = re.compile(
    r"^(?P<description>.+?)\s+"
    r"(?P<amount>\(?[0-9][0-9,]*\.[0-9]{2}\)?)\s*"
    r"(?P<date>\d{2}/\d{2}/\d{4})\s*"
    r"(?P<balance>\(?[0-9][0-9,]*\.[0-9]{2}\)?)$"
)
KEY_PATTERNS = [
    ("total_account_value", re.compile(r"total\s+value(?:\s+of\s+your\s+account(?:s)?)?", re.IGNORECASE)),
    ("total_account_value", re.compile(r"total\s+(?:account\s+)?value", re.IGNORECASE)),
    ("portfolio_value", re.compile(r"portfolio\s+value", re.IGNORECASE)),
    ("closing_value", re.compile(r"closing\s+value", re.IGNORECASE)),
    ("cash", re.compile(r"\bcash\b", re.IGNORECASE)),
    ("money_in", re.compile(r"(?:money|cash)\s+in", re.IGNORECASE)),
    ("money_out", re.compile(r"(?:money|cash)\s+out", re.IGNORECASE)),
]

OVERVIEW_COLUMNS = [
    "account_name",
    "value_current",
    "value_previous",
    "change_value",
    "source_pdf",
    "report_label",
    "season",
    "year",
    "report_date",
    "previous_period_date",
]

CAPITAL_TX_COLUMNS = [
    "account_name",
    "event_date",
    "description",
    "amount_gbp",
    "balance_after_gbp",
    "txn_type",
    "flow_class",
    "subledger",
    "source_pdf",
    "report_label",
    "season",
    "year",
    "source_row_num",
]

ASSET_VALUE_COLUMNS = [
    "account_name",
    "asset_name",
    "value_gbp",
    "report_date",
    "source_pdf",
    "report_label",
    "season",
    "year",
    "source_row_num",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parse HL Investment Report PDFs into structured CSV outputs.")
    parser.add_argument("--run-date", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--input-dir", default=None, help="Directory containing downloaded investment report PDFs.")
    parser.add_argument("--output-csv", default=None, help="Output CSV path for report-level summary metrics.")
    parser.add_argument("--overview-csv", default=None, help="Output CSV for account-level portfolio overview rows.")
    parser.add_argument("--capital-tx-csv", default=None, help="Output CSV for parsed capital-account transactions.")
    parser.add_argument("--asset-value-csv", default=None, help="Output CSV for parsed asset valuation checkpoints.")
    parser.add_argument("--text-dir", default=None, help="Optional directory to write raw extracted text files.")
    return parser.parse_args()


def _to_float(money_text: str) -> float:
    return float(money_text.replace(",", ""))


def _parse_signed_amount(text: str) -> float:
    cleaned = text.strip().replace(",", "")
    if cleaned.startswith("(") and cleaned.endswith(")"):
        return -float(cleaned[1:-1])
    return float(cleaned)


def _normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _extract_metrics(text: str) -> dict[str, float]:
    metrics: dict[str, float] = {}
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for line in lines:
        if not CURRENCY_TOKEN_RE.search(line):
            continue
        money_match = MONEY_RE.search(line)
        if not money_match:
            continue
        for metric_name, pattern in KEY_PATTERNS:
            if pattern.search(line) and metric_name not in metrics:
                metrics[metric_name] = _to_float(money_match.group(1))
    return metrics


def _extract_season_year(label: str) -> tuple[str | None, int | None]:
    normalized = re.sub(r"[_-]+", " ", label)
    m = SEASON_RE.search(normalized)
    if not m:
        return None, None
    return m.group(1).title(), int(m.group(2))


def _extract_overview_rows(text: str) -> tuple[str | None, str | None, list[dict[str, object]]]:
    lines = [_normalize_spaces(line) for line in text.splitlines() if line.strip()]
    start_idx = None
    for i, line in enumerate(lines):
        if "PORTFOLIO OVERVIEW" in line.upper():
            start_idx = i

    report_date = None
    previous_date = None
    overview_rows: list[dict[str, object]] = []
    if start_idx is None:
        return report_date, previous_date, overview_rows

    def _norm_date_label(x: str | None) -> str:
        return _normalize_spaces(str(x or "")).lower()

    for line in lines[start_idx : min(start_idx + 20, len(lines))]:
        matches = DATE_RE.findall(line)
        if not matches:
            continue
        if report_date is None:
            report_date = matches[0]

        if len(matches) >= 2 and previous_date is None:
            for candidate in matches[1:]:
                if _norm_date_label(candidate) != _norm_date_label(report_date):
                    previous_date = candidate
                    break
        elif previous_date is None:
            candidate = matches[0]
            if _norm_date_label(candidate) != _norm_date_label(report_date):
                previous_date = candidate

    stop_tokens = ("DETAILED VALUATION", "PAGE ")
    for line in lines[start_idx + 1 :]:
        if any(token in line.upper() for token in stop_tokens):
            break
        m = OVERVIEW_ROW_RE.match(line)
        if not m:
            continue
        account = m.group("account").strip()
        if account.lower().startswith("account value"):
            continue
        overview_rows.append(
            {
                "account_name": account,
                "value_current": float(m.group("current").replace(",", "")),
                "value_previous": float(m.group("previous").replace(",", "")),
                "change_value": float(m.group("change").replace(",", "")),
            }
        )

    return report_date, previous_date, overview_rows


def _classify_capital_description(description: str, amount: float) -> tuple[str, str]:
    d = description.lower()
    if any(x in d for x in ["withholding tax", "tax", "capital gains tax"]):
        return "tax", "tax"
    if any(x in d for x in ["management fee", "fee", "charge", "commission"]):
        return "fee", "fee"
    if "interest" in d:
        return "interest", "other"
    if "transfer from income account" in d or "transfer to capital account" in d:
        return "transfer_in", "internal_in"
    if "transfer to income account" in d or "transfer from capital account" in d:
        return "transfer_out", "internal_out"
    if "product transfer" in d and " to " in d:
        return "transfer_out", "internal_out"
    if "product transfer" in d and " from " in d:
        return "transfer_in", "internal_in"
    if any(x in d for x in ["subscription", "receipt", "pay by bank", "contribution", "topup", "top up"]):
        return "deposit", "external_in"
    if any(x in d for x in ["withdrawal", "payment sent", "bank transfer out"]):
        return "withdrawal", "external_out"
    if "transfer" in d:
        return ("transfer_in", "internal_in") if amount >= 0 else ("transfer_out", "internal_out")
    return "other", "other"


def _extract_inline_asset_name(line: str) -> str | None:
    match = ASSET_NAME_SUFFIX_RE.search(line)
    if not match:
        return None
    asset = _normalize_spaces(match.group("asset"))
    asset_upper = asset.upper()
    if any(
        token in asset_upper
        for token in (
            "SUBTOTAL",
            "TOTAL",
            "STOCK TOTAL",
            "CAPITAL ACCOUNT BALANCE",
            "INCOME ACCOUNT BALANCE",
            "TRANSACTION DETAILS",
            "VALUE",
            "DATE",
            "TYPE",
            "BALANCE",
        )
    ):
        return None
    if len(asset) < 4:
        return None
    compact = asset_upper.replace(" ", "")
    if re.fullmatch(r"N/?A[0-9.,]+", compact):
        return None
    return asset


def _looks_like_asset_name_line(line: str) -> bool:
    if not line:
        return False
    if line[0].isdigit():
        return False
    upper = line.upper()
    if any(
        token in upper
        for token in (
            "SUBTOTAL",
            "STOCK TOTAL",
            "CAPITAL ACCOUNT BALANCE",
            "INCOME ACCOUNT BALANCE",
            "CAPITAL ACCOUNT TRANSACTIONS",
            "INCOME ACCOUNT TRANSACTIONS",
            "TRANSACTION DETAILS",
            "BALANCE BROUGHT FORWARD",
            "PORTFOLIO OVERVIEW",
            "PAGE ",
            "HL INVESTMENT REPORT",
        )
    ):
        return False
    return bool(re.search(r"[A-Za-z]", line))


def _extract_asset_value_rows(text: str, report_date: str | None) -> list[dict[str, object]]:
    lines = [_normalize_spaces(line) for line in text.splitlines() if line.strip()]
    rows: list[dict[str, object]] = []

    current_account: str | None = None
    in_detailed_section = False
    i = 0
    while i < len(lines):
        line = lines[i]
        upper_line = line.upper()

        detailed_header = DETAILED_VALUATION_HEADER_RE.match(line)
        if detailed_header:
            current_account = _normalize_spaces(detailed_header.group("account"))
            in_detailed_section = True
            i += 1
            continue

        if not in_detailed_section:
            i += 1
            continue

        if (
            "CAPITAL ACCOUNT TRANSACTIONS" in upper_line
            or "INCOME ACCOUNT TRANSACTIONS" in upper_line
            or (upper_line.startswith("- ") and "DETAILED VALUATION" not in upper_line)
        ):
            in_detailed_section = False
            i += 1
            continue

        value_match = ASSET_VALUE_LEAD_RE.match(line)
        if not value_match:
            i += 1
            continue

        try:
            value_gbp = float(value_match.group("value").replace(",", ""))
        except ValueError:
            i += 1
            continue

        asset_name = _extract_inline_asset_name(line)
        if asset_name is None and i + 1 < len(lines):
            next_line = lines[i + 1]
            if _looks_like_asset_name_line(next_line):
                asset_name = _normalize_spaces(next_line)
                i += 1

        if asset_name:
            rows.append(
                {
                    "account_name": current_account,
                    "asset_name": asset_name,
                    "value_gbp": value_gbp,
                    "report_date": report_date,
                }
            )

        i += 1

    if not rows:
        return rows
    return (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["account_name", "asset_name", "report_date"], keep="last")
        .to_dict(orient="records")
    )


def _extract_capital_transactions(text: str) -> list[dict[str, object]]:
    lines = [_normalize_spaces(line) for line in text.splitlines() if line.strip()]
    current_account: str | None = None
    current_subledger: str = "unknown"
    rows: list[dict[str, object]] = []

    for line in lines:
        upper_line = line.upper()
        if "CAPITAL ACCOUNT TRANSACTIONS" in upper_line:
            current_subledger = "capital"
        elif "INCOME ACCOUNT TRANSACTIONS" in upper_line:
            current_subledger = "income"
        elif upper_line.startswith("- ") and "DETAILED VALUATION" not in upper_line:
            current_subledger = "unknown"

        header_match = ACCOUNT_HEADER_RE.match(line)
        if header_match:
            current_account = _normalize_spaces(header_match.group("account"))
            continue

        if current_account is None:
            continue
        if "Balance Brought Forward" in line:
            continue
        if "CAPITAL ACCOUNT TRANSACTIONS" in upper_line:
            continue
        if "INCOME ACCOUNT TRANSACTIONS" in upper_line:
            continue
        if not TXN_DATE_RE.search(line):
            continue

        match = TXN_LINE_RE_LEAD_AMOUNT.match(line) or TXN_LINE_RE_TRAIL_AMOUNT.match(line)
        if not match:
            continue

        try:
            event_date = pd.to_datetime(match.group("date"), dayfirst=True, errors="coerce")
            if pd.isna(event_date):
                continue
            amount = _parse_signed_amount(match.group("amount"))
            balance = _parse_signed_amount(match.group("balance"))
        except Exception:  # noqa: BLE001
            continue

        description = _normalize_spaces(match.group("description"))
        txn_type, flow_class = _classify_capital_description(description, amount)
        rows.append(
            {
                "account_name": current_account,
                "event_date": event_date.date(),
                "description": description,
                "amount_gbp": amount,
                "balance_after_gbp": balance,
                "txn_type": txn_type,
                "flow_class": flow_class,
                "subledger": current_subledger,
            }
        )

    if not rows:
        return rows
    return (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["account_name", "event_date", "description", "amount_gbp", "balance_after_gbp"])
        .to_dict(orient="records")
    )


def parse_pdf(
    pdf_path: Path,
    text_dir: Path | None = None,
) -> tuple[
    dict[str, object],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    reader = PdfReader(str(pdf_path))
    text_parts: list[str] = []
    for page in reader.pages:
        text_parts.append(page.extract_text() or "")
    full_text = "\n".join(text_parts)

    if text_dir is not None:
        text_dir.mkdir(parents=True, exist_ok=True)
        (text_dir / f"{pdf_path.stem}.txt").write_text(full_text, encoding="utf-8")

    season, year = _extract_season_year(pdf_path.stem)
    metrics = _extract_metrics(full_text)
    report_date, previous_date, overview_rows = _extract_overview_rows(full_text)
    capital_rows = _extract_capital_transactions(full_text)
    asset_rows = _extract_asset_value_rows(full_text, report_date=report_date)

    record: dict[str, object] = {
        "source_pdf": str(pdf_path),
        "report_label": pdf_path.stem,
        "season": season,
        "year": year,
        "report_date": report_date,
        "previous_period_date": previous_date,
        "parsed_at_utc": datetime.now(UTC).isoformat(),
    }
    for metric_name in sorted({name for name, _ in KEY_PATTERNS}):
        record[metric_name] = None
    record.update(metrics)

    for row in overview_rows:
        row["source_pdf"] = str(pdf_path)
        row["report_label"] = pdf_path.stem
        row["season"] = season
        row["year"] = year
        row["report_date"] = report_date
        row["previous_period_date"] = previous_date

    for i, row in enumerate(capital_rows, start=1):
        row["source_pdf"] = str(pdf_path)
        row["report_label"] = pdf_path.stem
        row["season"] = season
        row["year"] = year
        row["source_row_num"] = i

    for i, row in enumerate(asset_rows, start=1):
        row["source_pdf"] = str(pdf_path)
        row["report_label"] = pdf_path.stem
        row["season"] = season
        row["year"] = year
        row["source_row_num"] = i

    return record, overview_rows, capital_rows, asset_rows


def main() -> None:
    args = parse_args()
    input_dir = (
        Path(args.input_dir)
        if args.input_dir
        else (PROJECT_ROOT / "data" / "raw" / args.run_date / "investment_reports")
    )
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")

    output_csv = (
        Path(args.output_csv)
        if args.output_csv
        else (PROJECT_ROOT / "data" / "staging" / f"investment_reports_extracted_{args.run_date}.csv")
    )
    overview_csv = (
        Path(args.overview_csv)
        if args.overview_csv
        else (PROJECT_ROOT / "data" / "staging" / f"investment_reports_overview_{args.run_date}.csv")
    )
    capital_tx_csv = (
        Path(args.capital_tx_csv)
        if args.capital_tx_csv
        else (PROJECT_ROOT / "data" / "staging" / f"investment_reports_capital_txns_{args.run_date}.csv")
    )
    asset_value_csv = (
        Path(args.asset_value_csv)
        if args.asset_value_csv
        else (PROJECT_ROOT / "data" / "staging" / f"investment_reports_asset_values_{args.run_date}.csv")
    )
    text_dir = Path(args.text_dir) if args.text_dir else None

    pdf_files = sorted(input_dir.rglob("*.pdf"))
    if not pdf_files:
        raise ValueError(f"No PDF files found in {input_dir}")

    summary_rows: list[dict[str, object]] = []
    overview_rows: list[dict[str, object]] = []
    capital_rows: list[dict[str, object]] = []
    asset_rows: list[dict[str, object]] = []
    for pdf_path in pdf_files:
        summary, overview, capital, assets = parse_pdf(pdf_path, text_dir=text_dir)
        summary_rows.append(summary)
        overview_rows.extend(overview)
        capital_rows.extend(capital)
        asset_rows.extend(assets)

    summary_df = pd.DataFrame(summary_rows)
    overview_df = pd.DataFrame(overview_rows, columns=OVERVIEW_COLUMNS)
    capital_df = pd.DataFrame(capital_rows, columns=CAPITAL_TX_COLUMNS)
    asset_df = pd.DataFrame(asset_rows, columns=ASSET_VALUE_COLUMNS)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    overview_csv.parent.mkdir(parents=True, exist_ok=True)
    capital_tx_csv.parent.mkdir(parents=True, exist_ok=True)
    asset_value_csv.parent.mkdir(parents=True, exist_ok=True)

    summary_df.to_csv(output_csv, index=False)
    overview_df.to_csv(overview_csv, index=False)
    capital_df.to_csv(capital_tx_csv, index=False)
    asset_df.to_csv(asset_value_csv, index=False)

    print(f"Parsed reports: {len(summary_df)}")
    print(f"Output CSV: {output_csv}")
    print(f"Overview CSV: {overview_csv} rows={len(overview_df)}")
    print(f"Capital transactions CSV: {capital_tx_csv} rows={len(capital_df)}")
    print(f"Asset values CSV: {asset_value_csv} rows={len(asset_df)}")


if __name__ == "__main__":
    main()
