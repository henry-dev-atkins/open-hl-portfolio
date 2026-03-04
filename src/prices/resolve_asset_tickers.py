from __future__ import annotations

import argparse
import re
from difflib import SequenceMatcher
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
try:
    from rapidfuzz import fuzz
except Exception:  # noqa: BLE001
    fuzz = None

from src.common.config import load_yaml
from src.common.db import connect_db, ensure_schema
from src.common.paths import PROJECT_ROOT, get_db_path
from src.prices.asset_identity import canonicalize_asset_name, make_asset_id
from src.prices.links import (
    build_hl_search_url,
    build_yf_history_url,
    build_yf_quote_url,
    choose_hl_search_query,
)

try:
    import yfinance as yf
except Exception:  # noqa: BLE001
    yf = None


AUTO_ACCEPT_SCORE = 92.0
AUTO_ACCEPT_GAP = 8.0
DEFAULT_PROVIDER = "yfinance"
BLOCKED_QUOTE_TYPES = {"FUTURE", "INDEX", "CURRENCY", "CRYPTOCURRENCY"}
PRIMARY_EXCHANGES = {"LSE", "IOB", "LON", "LSEIOB1", "LSEIOB2"}
SECONDARY_EXCHANGES = {"MIL", "GER", "FRA", "PAR", "EBS", "AMS", "ETR"}
PREFERRED_EXCHANGES = PRIMARY_EXCHANGES.union(SECONDARY_EXCHANGES)
QUERY_TRAILING_NOISE_TOKENS = {
    "acc",
    "inc",
    "dist",
    "class",
    "plc",
    "ord",
    "ordinary",
    "sicav",
    "oeic",
    "ucits",
    "etf",
    "etc",
    "gbp",
    "usd",
    "eur",
    "gbx",
    "trust",
    "fund",
}
QUERY_STOPWORDS = QUERY_TRAILING_NOISE_TOKENS.union(
    {
        "the",
        "and",
        "of",
        "for",
        "daily",
        "leveraged",
        "physical",
        "sustainable",
        "future",
        "global",
        "index",
    }
)
MATCH_KEYWORD_STOPWORDS = QUERY_TRAILING_NOISE_TOKENS.union(
    {
        "shares",
        "share",
        "index",
        "income",
        "investment",
        "investments",
        "limited",
        "ltd",
        "public",
        "daily",
        "leveraged",
    }
)
QUERY_ALIAS_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"\bL&G\b", "Legal & General"),
    (r"\bstd\b", "standard"),
    (r"\binv\b", "investments"),
    (r"\bco\b", "companies"),
    (r"\btst\b", "trust"),
    (r"\bmkts\b", "markets"),
    (r"\bmkt\b", "market"),
    (r"\bifsl\b", ""),
    (r"\basi\b", "abrdn"),
    (r"\bvan\s*eck\s+vectors\b", "VanEck"),
    (r"\bishares\s+iv\s+plc\b", "iShares"),
    (r"\bark\s+sustainable\s+future\s+of\s+food\b", "Rize Sustainable Future Of Food"),
    (r"\bemerging\s+markets\b", "EM"),
    (r"\bem\s+market\b", "EM"),
    (r"\bem\s+mkt\b", "EM"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve asset names to Yahoo Finance tickers.")
    parser.add_argument("--run-date", required=True, help="Run date used for report staging files.")
    parser.add_argument("--db-path", default=None, help="DuckDB file path")
    parser.add_argument("--provider", default=DEFAULT_PROVIDER, help="Ticker provider name.")
    parser.add_argument(
        "--asset-values-csv",
        default=None,
        help="Path to investment report asset values CSV",
    )
    parser.add_argument(
        "--overrides-path",
        default="config/asset_ticker_overrides.yml",
        help="Path to manual override YAML file",
    )
    parser.add_argument(
        "--unresolved-assets-csv",
        default=None,
        help="Output path for unresolved asset summary CSV",
    )
    parser.add_argument(
        "--unresolved-candidates-csv",
        default=None,
        help="Output path for unresolved candidate CSV",
    )
    parser.add_argument(
        "--disable-auto-search",
        action="store_true",
        help="Disable yfinance search and only use manual/cached mappings.",
    )
    return parser.parse_args()


def _first_non_blank(values: pd.Series) -> str | None:
    for value in values.tolist():
        text = str(value or "").strip()
        if text:
            return text
    return None


def _load_asset_values(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Asset value CSV not found: {path}")
    df = pd.read_csv(path)
    required = {"asset_name", "report_date"}
    if not required.issubset(df.columns):
        raise ValueError(f"Asset value CSV missing required columns {required}: {path}")
    if df.empty:
        raise ValueError(f"Asset value CSV is empty: {path}")
    return df


def _build_asset_universe(asset_values: pd.DataFrame) -> pd.DataFrame:
    frame = asset_values.copy()
    frame["asset_name_raw"] = frame["asset_name"].astype(str).str.strip()
    frame["asset_name_canonical"] = frame["asset_name_raw"].apply(canonicalize_asset_name)
    frame = frame[frame["asset_name_canonical"].astype(str).str.len() > 0].copy()

    if "isin" not in frame.columns:
        frame["isin"] = None
    if "sedol" not in frame.columns:
        frame["sedol"] = None

    frame["report_date"] = pd.to_datetime(
        frame["report_date"].astype(str),
        errors="coerce",
        dayfirst=True,
        format="mixed",
    ).dt.date
    frame = frame[frame["report_date"].notna()].copy()
    frame["asset_id"] = frame["asset_name_canonical"].apply(make_asset_id)

    grouped = (
        frame.sort_values(["asset_id", "report_date"])
        .groupby(["asset_id", "asset_name_canonical"], as_index=False)
        .agg(
            asset_name_search=("asset_name_raw", _first_non_blank),
            first_seen_date=("report_date", "min"),
            last_seen_date=("report_date", "max"),
            isin=("isin", _first_non_blank),
            sedol=("sedol", _first_non_blank),
        )
    )
    grouped["source_priority"] = "investment_report_pdf"
    return grouped[
        [
            "asset_id",
            "asset_name_canonical",
            "asset_name_search",
            "isin",
            "sedol",
            "first_seen_date",
            "last_seen_date",
            "source_priority",
        ]
    ]


def _load_overrides(path: str | Path) -> list[dict[str, Any]]:
    cfg = load_yaml(path)
    overrides = cfg.get("overrides", [])
    if not isinstance(overrides, list):
        return []
    return [o for o in overrides if isinstance(o, dict)]


def _override_matches(asset_row: pd.Series, override: dict[str, Any]) -> bool:
    asset_isin = str(asset_row.get("isin") or "").strip().upper()
    asset_sedol = str(asset_row.get("sedol") or "").strip().upper()
    asset_name = str(asset_row.get("asset_name_canonical") or "")

    ov_isin = str(override.get("isin") or "").strip().upper()
    if ov_isin and asset_isin and ov_isin == asset_isin:
        return True

    ov_sedol = str(override.get("sedol") or "").strip().upper()
    if ov_sedol and asset_sedol and ov_sedol == asset_sedol:
        return True

    pattern = str(override.get("asset_name_pattern") or "").strip()
    if pattern:
        try:
            return re.search(pattern, asset_name, flags=re.IGNORECASE) is not None
        except re.error:
            return pattern.lower() in asset_name.lower()
    return False


def _find_override(asset_row: pd.Series, overrides: list[dict[str, Any]]) -> dict[str, Any] | None:
    for override in overrides:
        if _override_matches(asset_row, override):
            return override
    return None


def _name_similarity(asset_name: str, candidate_name: str) -> float:
    left = canonicalize_asset_name(asset_name)
    right = canonicalize_asset_name(candidate_name)
    if not left or not right:
        return 0.0
    if fuzz is not None:
        return float(fuzz.token_set_ratio(left, right))
    return round(100.0 * SequenceMatcher(None, left, right).ratio(), 2)


def _normalize_query_text(text: str, apply_aliases: bool = True) -> str:
    out = str(text or "").replace("...", " ")
    out = out.replace("(", " ").replace(")", " ")
    out = re.sub(r"\s+", " ", out).strip(" -")
    if apply_aliases:
        for pattern, replacement in QUERY_ALIAS_PATTERNS:
            out = re.sub(pattern, replacement, out, flags=re.IGNORECASE)
    out = re.sub(r"\s+", " ", out).strip(" -")
    return out


def _token_key(token: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "", str(token or "")).lower()


def _strip_trailing_noise_tokens(tokens: list[str]) -> list[str]:
    out = list(tokens)
    while out and _token_key(out[-1]) in QUERY_TRAILING_NOISE_TOKENS:
        out = out[:-1]
    return out


def _compact_query_keywords(text: str, max_tokens: int = 6) -> str:
    words = re.findall(r"[A-Za-z0-9&]+", str(text or ""))
    keep = [w for w in words if _token_key(w) not in QUERY_STOPWORDS and len(_token_key(w)) > 1]
    if not keep:
        return ""
    return " ".join(keep[:max_tokens]).strip()


def _keyword_tokens_for_match(text: str) -> set[str]:
    words = re.findall(r"[A-Za-z0-9]+", canonicalize_asset_name(text))
    out = {
        word.lower()
        for word in words
        if len(word) > 1 and word.lower() not in MATCH_KEYWORD_STOPWORDS
    }
    return out


def _query_variants(asset_name_search: str) -> list[str]:
    raw = str(asset_name_search or "").strip()
    if not raw:
        return []

    variants: list[str] = []

    def add(value: str, apply_aliases: bool = True) -> None:
        text = _normalize_query_text(value, apply_aliases=apply_aliases)
        text = re.sub(r"\bphysical metals physical\b", "physical", text, flags=re.IGNORECASE)
        text = re.sub(r"\bishares plc\b", "ishares", text, flags=re.IGNORECASE)
        text = re.sub(r"\bem\s+market\b", "EM", text, flags=re.IGNORECASE)
        text = re.sub(r"\bem\s+mkt\b", "EM", text, flags=re.IGNORECASE)
        text = re.sub(r"\bemerging\s+markets\b", "EM", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+", " ", text).strip(" -")
        if text and text not in variants:
            variants.append(text)

    # Keep original abbreviations (e.g. "L&G") and alias-expanded form.
    add(raw, apply_aliases=False)
    add(raw, apply_aliases=True)
    no_parens = re.sub(r"\([^)]*\)", " ", raw)
    add(no_parens, apply_aliases=False)
    add(no_parens, apply_aliases=True)

    no_class = re.sub(r"\b(class\s+[a-z0-9]+|acc|inc|dist)\b", " ", no_parens, flags=re.IGNORECASE)
    add(no_class)

    no_corp_noise = re.sub(
        r"\b(ord(inary)?|plc|tst|gbp\s*[0-9.]+|\d+p)\b",
        " ",
        no_class,
        flags=re.IGNORECASE,
    )
    add(no_corp_noise)

    expanded = (
        no_corp_noise.replace("L&G", "Legal & General")
        .replace("IFSL ", "")
        .replace("VT ", "")
        .replace("ASI ", "abrdn ")
        .replace("VanEck Vectors", "VanEck")
    )
    add(expanded)

    no_vehicle = re.sub(r"\b(ucits|etf|etc)\b", " ", no_corp_noise, flags=re.IGNORECASE)
    add(no_vehicle)

    # Right-trim phrase variants so Yahoo can match human-readable fund names that include
    # portfolio-report suffix noise such as currency/class tags.
    tokens = [t for t in _normalize_query_text(raw, apply_aliases=False).split(" ") if t]
    for cut in range(len(tokens), 2, -1):
        cut_query = " ".join(tokens[:cut])
        add(cut_query, apply_aliases=False)
        add(cut_query, apply_aliases=True)

    trimmed_tokens = _strip_trailing_noise_tokens(tokens)
    if trimmed_tokens:
        trimmed_query = " ".join(trimmed_tokens)
        add(trimmed_query, apply_aliases=False)
        add(trimmed_query, apply_aliases=True)
        core_tokens = [
            t for t in trimmed_tokens
            if _token_key(t) not in QUERY_TRAILING_NOISE_TOKENS and not re.fullmatch(r"\d+(?:\.\d+)?", t)
        ]
        if core_tokens:
            core_query = " ".join(core_tokens)
            add(core_query, apply_aliases=False)
            add(core_query, apply_aliases=True)
            add(re.sub(r"\bemerging\s+markets\b", "EM", core_query, flags=re.IGNORECASE), apply_aliases=False)
            add(re.sub(r"\bem\s+market\b", "EM", core_query, flags=re.IGNORECASE), apply_aliases=False)
            add(re.sub(r"\bem\s+mkt\b", "EM", core_query, flags=re.IGNORECASE), apply_aliases=False)

    compact = _compact_query_keywords(raw, max_tokens=6)
    if compact:
        add(compact, apply_aliases=False)
        add(compact, apply_aliases=True)
        compact_short = _compact_query_keywords(raw, max_tokens=4)
        if compact_short:
            add(compact_short, apply_aliases=False)
            add(compact_short, apply_aliases=True)

    return variants[:14]


def _asset_style(asset_name: str) -> str:
    n = canonicalize_asset_name(asset_name)
    if any(t in n for t in ["ucits", "etf", "etc", "leveraged"]):
        return "etf"
    if any(t in n for t in ["ord", "plc", "trust", "tst"]):
        return "equity"
    if any(t in n for t in ["acc", "inc", "dist", "oeic", "fund", "index", "class"]):
        return "fund"
    return "unknown"


def _extract_class_token(text: str) -> str | None:
    n = canonicalize_asset_name(text)
    match = re.search(r"\b(acc|inc|dist)\b", n)
    if not match:
        return None
    return match.group(1)


def _score_candidate(asset_name: str, quote: dict[str, Any], query_text: str | None = None) -> float:
    longname = str(quote.get("longname") or quote.get("shortname") or quote.get("name") or "").strip()
    ticker = str(quote.get("symbol") or "").strip()
    exchange = str(quote.get("exchange") or quote.get("exchDisp") or "").strip().upper()
    quote_type = str(quote.get("quoteType") or "").strip().upper()
    currency = str(quote.get("currency") or "").strip().upper()
    canonical_asset = canonicalize_asset_name(asset_name)

    base_name = _name_similarity(asset_name, longname or ticker)
    base_query = _name_similarity(query_text or "", longname or ticker) if query_text else 0.0
    score = max(base_name, base_query)

    asset_keywords = _keyword_tokens_for_match(asset_name)
    candidate_keywords = _keyword_tokens_for_match(longname or ticker)
    if asset_keywords:
        coverage = len(asset_keywords.intersection(candidate_keywords)) / len(asset_keywords)
        if coverage >= 0.8:
            score += 8
        elif coverage >= 0.5:
            score += 4
        elif coverage < 0.3:
            score -= 14
        else:
            score -= 6

    style = _asset_style(asset_name)
    asset_class = _extract_class_token(asset_name)
    cand_class = _extract_class_token(longname)

    if style == "etf":
        if quote_type == "ETF":
            score += 8
        elif quote_type == "EQUITY":
            score += 2
        elif quote_type == "MUTUALFUND":
            score -= 4
    elif style == "equity":
        if quote_type == "EQUITY":
            score += 8
        elif quote_type == "ETF":
            score -= 2
        elif quote_type == "MUTUALFUND":
            score -= 6
    elif style == "fund":
        if quote_type == "MUTUALFUND":
            score += 8
        elif quote_type == "ETF":
            score += 2

    if asset_class:
        if cand_class == asset_class:
            score += 6
        elif cand_class is not None and cand_class != asset_class:
            score -= 5

    if exchange in PRIMARY_EXCHANGES:
        score += 6
    elif exchange in SECONDARY_EXCHANGES:
        score += 2
    elif exchange in {"NMS", "NGM", "ASE", "NYQ", "NCM", "PNK"}:
        score -= 2
    elif style in {"etf", "equity"}:
        score -= 4

    if (
        quote_type in {"ETF", "EQUITY"}
        and any(token in canonical_asset for token in ["ucits", "sicav", "oeic", " plc "])
        and exchange in {"PCX", "NGM", "NMS", "NYQ", "ASE", "BTS", "NEO"}
    ):
        # Most HL UCITS/SICAV holdings are European listings; de-prioritize US-centric exchanges.
        score -= 8

    if currency == "GBP":
        score += 4
    elif currency == "GBX":
        score += 2

    if ticker.endswith(".L"):
        score += 3
    if "=" in ticker:
        score -= 20

    return round(float(score), 2)


def _yfinance_search_candidates(asset_name: str, hl_search_url: str) -> list[dict[str, Any]]:
    if yf is None:
        return []

    best_by_ticker: dict[str, dict[str, Any]] = {}
    for query in _query_variants(asset_name):
        try:
            search = yf.Search(query=query, max_results=12, enable_fuzzy_query=False)
            quotes = list(getattr(search, "quotes", []) or [])
        except Exception:  # noqa: BLE001
            continue

        for quote in quotes:
            ticker = str(quote.get("symbol") or "").strip()
            if not ticker or "=" in ticker:
                continue

            quote_type = str(quote.get("quoteType") or "").strip().upper()
            if quote_type in BLOCKED_QUOTE_TYPES:
                continue

            longname = str(quote.get("longname") or quote.get("shortname") or quote.get("name") or "").strip()
            score = _score_candidate(asset_name, quote, query_text=query)
            row = {
                "ticker": ticker,
                "longname": longname,
                "exchange": str(quote.get("exchange") or quote.get("exchDisp") or "").strip(),
                "quote_type": str(quote.get("quoteType") or "").strip(),
                "currency": str(quote.get("currency") or "").strip(),
                "score": score,
                "query_used": query,
                "yf_quote_url": build_yf_quote_url(ticker),
                "hl_search_url": hl_search_url,
            }
            existing = best_by_ticker.get(ticker)
            if existing is None or float(row["score"]) > float(existing["score"]):
                best_by_ticker[ticker] = row

    out = sorted(best_by_ticker.values(), key=lambda r: (r["score"], r["ticker"]), reverse=True)
    for idx, row in enumerate(out, start=1):
        row["rank"] = idx
    return out


def _pick_auto_match(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not candidates:
        return None
    ordered = sorted(candidates, key=lambda r: (r["score"], r["rank"]), reverse=True)
    top = ordered[0]
    second = ordered[1] if len(ordered) > 1 else None
    gap = float(top["score"]) - float(second["score"]) if second else float(top["score"])
    top_score = float(top["score"])
    top_exchange = str(top.get("exchange") or "").upper()
    top_quote_type = str(top.get("quote_type") or "").upper()
    top_ticker = str(top.get("ticker") or "").upper()

    if top_score >= AUTO_ACCEPT_SCORE and gap >= AUTO_ACCEPT_GAP:
        return top
    if top_score >= 100.0 and top_exchange in PRIMARY_EXCHANGES and top_quote_type in {"MUTUALFUND", "ETF", "EQUITY"}:
        return top
    if top_score >= 88.0 and gap >= 5.0 and top_exchange in PREFERRED_EXCHANGES:
        return top
    if top_score >= 94.0 and top_exchange in PREFERRED_EXCHANGES and top_quote_type in {"ETF", "EQUITY"}:
        return top
    if top_score >= 84.0 and gap >= 7.0 and top_ticker.endswith(".L"):
        return top
    if top_score >= 86.0 and top_exchange in PREFERRED_EXCHANGES and top_quote_type in {"ETF", "EQUITY"} and gap >= 1.5:
        return top
    if top_score >= 86.0 and top_exchange in PRIMARY_EXCHANGES and top_quote_type in {"ETF", "EQUITY"} and top_ticker.endswith(".L"):
        return top
    if top_score >= 84.0 and gap >= 2.0 and top_quote_type == "MUTUALFUND":
        return top

    preferred = [
        row
        for row in ordered
        if str(row.get("exchange") or "").upper() in PREFERRED_EXCHANGES
    ]
    if preferred:
        best_preferred = preferred[0]
        best_preferred_score = float(best_preferred["score"])
        best_preferred_type = str(best_preferred.get("quote_type") or "").upper()
        if (
            best_preferred_score >= 78.0
            and top_score >= 82.0
            and (top_score - best_preferred_score) <= 5.0
            and best_preferred_type in {"ETF", "EQUITY"}
        ):
            return best_preferred
        if (
            best_preferred_score >= 90.0
            and top_score >= 95.0
            and (top_score - best_preferred_score) <= 8.0
            and str(best_preferred.get("ticker") or "").upper().endswith(".L")
            and best_preferred_type in {"ETF", "EQUITY"}
        ):
            return best_preferred
        if (
            best_preferred_score >= 78.0
            and (top_score - best_preferred_score) <= 3.0
            and str(best_preferred.get("ticker") or "").upper().endswith(".L")
            and best_preferred_type in {"ETF", "EQUITY", "MUTUALFUND"}
        ):
            return best_preferred

    if top_score >= 74.0 and second is None and top_quote_type == "MUTUALFUND":
        return top
    return None


def _cached_mapping_lookup(conn, provider: str) -> dict[str, dict[str, Any]]:
    frame = conn.execute(
        """
        select *
        from asset_ticker_mapping
        where provider = ?
        """,
        [provider],
    ).df()
    if frame.empty:
        return {}
    return {str(r["asset_id"]): r.to_dict() for _, r in frame.iterrows()}


def _mapping_from_manual(
    asset_row: pd.Series,
    override: dict[str, Any],
    hl_search_url: str,
    cached_hl_security_url: str | None,
    provider: str,
) -> dict[str, Any]:
    ticker = str(override.get("yf_ticker") or override.get("ticker") or "").strip() or None
    hl_security_url = str(override.get("hl_security_url") or "").strip() or cached_hl_security_url
    hl_link_source = "manual" if str(override.get("hl_security_url") or "").strip() else (
        "cached" if cached_hl_security_url else "fallback_only"
    )
    status = "resolved" if ticker else "unresolved"
    return {
        "asset_id": asset_row["asset_id"],
        "provider": provider,
        "ticker": ticker,
        "currency": str(override.get("currency") or "").strip() or None,
        "confidence_score": 100.0 if ticker else None,
        "match_status": status,
        "mapping_source": "manual",
        "is_locked": True,
        "matched_at": datetime.now(UTC),
        "notes": str(override.get("notes") or "").strip() or None,
        "yf_quote_url": build_yf_quote_url(ticker) if ticker else None,
        "yf_history_url": build_yf_history_url(ticker) if ticker else None,
        "hl_security_url": hl_security_url,
        "hl_search_url": hl_search_url,
        "hl_link_source": hl_link_source if hl_security_url else "fallback_only",
    }


def _mapping_from_cached(
    asset_row: pd.Series,
    cached: dict[str, Any],
    hl_search_url: str,
    provider: str,
) -> dict[str, Any]:
    ticker = str(cached.get("ticker") or "").strip() or None
    status = "resolved" if ticker else "unresolved"
    hl_security_url = str(cached.get("hl_security_url") or "").strip() or None
    return {
        "asset_id": asset_row["asset_id"],
        "provider": provider,
        "ticker": ticker,
        "currency": str(cached.get("currency") or "").strip() or None,
        "confidence_score": float(cached.get("confidence_score")) if pd.notna(cached.get("confidence_score")) else None,
        "match_status": status,
        "mapping_source": "cached",
        "is_locked": bool(cached.get("is_locked")) if pd.notna(cached.get("is_locked")) else False,
        "matched_at": datetime.now(UTC),
        "notes": str(cached.get("notes") or "").strip() or None,
        "yf_quote_url": build_yf_quote_url(ticker) if ticker else None,
        "yf_history_url": build_yf_history_url(ticker) if ticker else None,
        "hl_security_url": hl_security_url,
        "hl_search_url": hl_search_url,
        "hl_link_source": "cached" if hl_security_url else "fallback_only",
    }


def _mapping_from_auto(
    asset_row: pd.Series,
    auto_match: dict[str, Any] | None,
    hl_search_url: str,
    cached_hl_security_url: str | None,
    provider: str,
) -> dict[str, Any]:
    ticker = str(auto_match.get("ticker") or "").strip() if auto_match else ""
    ticker_or_none = ticker or None
    status = "resolved" if ticker_or_none else "unresolved"
    return {
        "asset_id": asset_row["asset_id"],
        "provider": provider,
        "ticker": ticker_or_none,
        "currency": str(auto_match.get("currency") or "").strip() if auto_match else None,
        "confidence_score": float(auto_match["score"]) if auto_match and pd.notna(auto_match.get("score")) else None,
        "match_status": status,
        "mapping_source": "auto_search",
        "is_locked": False,
        "matched_at": datetime.now(UTC),
        "notes": None,
        "yf_quote_url": build_yf_quote_url(ticker_or_none) if ticker_or_none else None,
        "yf_history_url": build_yf_history_url(ticker_or_none) if ticker_or_none else None,
        "hl_security_url": cached_hl_security_url,
        "hl_search_url": hl_search_url,
        "hl_link_source": "cached" if cached_hl_security_url else "fallback_only",
    }


def resolve_asset_tickers(
    conn,
    run_date: str,
    provider: str,
    asset_values_csv: Path,
    overrides_path: str | Path,
    unresolved_assets_csv: Path,
    unresolved_candidates_csv: Path,
    disable_auto_search: bool,
) -> tuple[int, int, int]:
    asset_values = _load_asset_values(asset_values_csv)
    asset_universe = _build_asset_universe(asset_values)
    if asset_universe.empty:
        raise ValueError("No canonical assets found for ticker resolution.")

    overrides = _load_overrides(overrides_path)
    cached = _cached_mapping_lookup(conn=conn, provider=provider)

    mapping_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []

    for _, asset_row in asset_universe.iterrows():
        asset_id = str(asset_row["asset_id"])
        asset_name_search = str(asset_row.get("asset_name_search") or asset_row["asset_name_canonical"])
        isin = str(asset_row.get("isin") or "").strip() or None
        sedol = str(asset_row.get("sedol") or "").strip() or None
        search_query = choose_hl_search_query(isin=isin, sedol=sedol, asset_name_canonical=asset_row["asset_name_canonical"])
        hl_search_url = build_hl_search_url(search_query)

        cached_row = cached.get(asset_id)
        cached_hl_security_url = None
        if cached_row:
            cached_hl_security_url = str(cached_row.get("hl_security_url") or "").strip() or None

        override = _find_override(asset_row=asset_row, overrides=overrides)
        if override is not None:
            mapping_rows.append(
                _mapping_from_manual(
                    asset_row=asset_row,
                    override=override,
                    hl_search_url=hl_search_url,
                    cached_hl_security_url=cached_hl_security_url,
                    provider=provider,
                )
            )
            continue

        if cached_row:
            is_locked = bool(cached_row.get("is_locked")) if pd.notna(cached_row.get("is_locked")) else False
            if is_locked or disable_auto_search:
                mapping_rows.append(
                    _mapping_from_cached(
                        asset_row=asset_row,
                        cached=cached_row,
                        hl_search_url=hl_search_url,
                        provider=provider,
                    )
                )
                continue

        auto_candidates: list[dict[str, Any]] = []
        if not disable_auto_search:
            auto_candidates = _yfinance_search_candidates(
                asset_name=asset_name_search,
                hl_search_url=hl_search_url,
            )
            for candidate in auto_candidates:
                candidate_rows.append(
                    {
                        "asset_id": asset_id,
                        "provider": provider,
                        "query": str(candidate.get("query_used") or asset_name_search),
                        "ticker": candidate["ticker"],
                        "longname": candidate["longname"],
                        "exchange": candidate["exchange"],
                        "quote_type": candidate["quote_type"],
                        "currency": candidate["currency"],
                        "score": candidate["score"],
                        "rank": candidate["rank"],
                        "evaluated_at": datetime.now(UTC),
                        "yf_quote_url": candidate["yf_quote_url"],
                        "hl_search_url": candidate["hl_search_url"],
                    }
                )

        auto_match = _pick_auto_match(auto_candidates)
        mapping_rows.append(
            _mapping_from_auto(
                asset_row=asset_row,
                auto_match=auto_match,
                hl_search_url=hl_search_url,
                cached_hl_security_url=cached_hl_security_url,
                provider=provider,
            )
        )

    dim_asset_df = asset_universe[
        [
            "asset_id",
            "asset_name_canonical",
            "isin",
            "sedol",
            "first_seen_date",
            "last_seen_date",
            "source_priority",
        ]
    ].copy()
    mapping_df = pd.DataFrame(mapping_rows)
    candidate_df = pd.DataFrame(
        candidate_rows,
        columns=[
            "asset_id",
            "provider",
            "query",
            "ticker",
            "longname",
            "exchange",
            "quote_type",
            "currency",
            "score",
            "rank",
            "evaluated_at",
            "yf_quote_url",
            "hl_search_url",
        ],
    )

    asset_ids = dim_asset_df["asset_id"].astype(str).tolist()
    if asset_ids:
        placeholders = ",".join(["?"] * len(asset_ids))
        conn.execute(
            f"delete from dim_asset where asset_id in ({placeholders})",  # noqa: S608
            asset_ids,
        )
    if not dim_asset_df.empty:
        conn.register("tmp_dim_asset", dim_asset_df)
        conn.execute("insert into dim_asset select * from tmp_dim_asset")
        conn.unregister("tmp_dim_asset")

    if asset_ids:
        placeholders = ",".join(["?"] * len(asset_ids))
        conn.execute(
            f"delete from asset_ticker_mapping where provider = ? and asset_id in ({placeholders})",  # noqa: S608
            [provider, *asset_ids],
        )
    if not mapping_df.empty:
        conn.register("tmp_asset_ticker_mapping", mapping_df)
        conn.execute("insert into asset_ticker_mapping select * from tmp_asset_ticker_mapping")
        conn.unregister("tmp_asset_ticker_mapping")

    if asset_ids:
        placeholders = ",".join(["?"] * len(asset_ids))
        conn.execute(
            f"delete from asset_ticker_candidates where provider = ? and asset_id in ({placeholders})",  # noqa: S608
            [provider, *asset_ids],
        )
    if not candidate_df.empty:
        conn.register("tmp_asset_ticker_candidates", candidate_df)
        conn.execute("insert into asset_ticker_candidates select * from tmp_asset_ticker_candidates")
        conn.unregister("tmp_asset_ticker_candidates")

    unresolved_assets = mapping_df[mapping_df["match_status"] != "resolved"].copy()
    unresolved_assets = unresolved_assets.merge(
        dim_asset_df[["asset_id", "asset_name_canonical"]],
        on="asset_id",
        how="left",
    )
    unresolved_assets["ticker_candidate"] = unresolved_assets["ticker"]
    unresolved_assets = unresolved_assets[
        [
            "asset_id",
            "asset_name_canonical",
            "ticker_candidate",
            "yf_quote_url",
            "hl_security_url",
            "hl_search_url",
            "match_status",
            "confidence_score",
        ]
    ].sort_values(["asset_name_canonical", "asset_id"])

    unresolved_candidates = candidate_df.merge(
        dim_asset_df[["asset_id", "asset_name_canonical"]],
        on="asset_id",
        how="left",
    )
    unresolved_candidate_asset_ids = set(unresolved_assets["asset_id"].astype(str).tolist())
    unresolved_candidates = unresolved_candidates[
        unresolved_candidates["asset_id"].astype(str).isin(unresolved_candidate_asset_ids)
    ].copy()
    if not unresolved_candidates.empty:
        unresolved_candidates = unresolved_candidates.merge(
            mapping_df[["asset_id", "hl_security_url", "match_status"]],
            on="asset_id",
            how="left",
        )
        unresolved_candidates = unresolved_candidates.rename(
            columns={
                "ticker": "ticker_candidate",
                "score": "confidence_score",
            }
        )
        unresolved_candidates = unresolved_candidates[
            [
                "asset_id",
                "asset_name_canonical",
                "ticker_candidate",
                "yf_quote_url",
                "hl_security_url",
                "hl_search_url",
                "match_status",
                "confidence_score",
            ]
        ].sort_values(["asset_name_canonical", "confidence_score"], ascending=[True, False])
    else:
        unresolved_candidates = pd.DataFrame(
            columns=[
                "asset_id",
                "asset_name_canonical",
                "ticker_candidate",
                "yf_quote_url",
                "hl_security_url",
                "hl_search_url",
                "match_status",
                "confidence_score",
            ]
        )

    unresolved_assets_csv.parent.mkdir(parents=True, exist_ok=True)
    unresolved_candidates_csv.parent.mkdir(parents=True, exist_ok=True)
    unresolved_assets.to_csv(unresolved_assets_csv, index=False)
    unresolved_candidates.to_csv(unresolved_candidates_csv, index=False)

    return len(mapping_df), len(unresolved_assets), len(unresolved_candidates)


def main() -> None:
    args = parse_args()
    db_path = get_db_path(args.db_path)
    asset_values_csv = (
        Path(args.asset_values_csv)
        if args.asset_values_csv
        else PROJECT_ROOT / "data" / "staging" / f"investment_reports_asset_values_{args.run_date}.csv"
    )
    unresolved_assets_csv = (
        Path(args.unresolved_assets_csv)
        if args.unresolved_assets_csv
        else PROJECT_ROOT / "data" / "staging" / f"unresolved_assets_{args.run_date}.csv"
    )
    unresolved_candidates_csv = (
        Path(args.unresolved_candidates_csv)
        if args.unresolved_candidates_csv
        else PROJECT_ROOT / "data" / "staging" / f"unresolved_ticker_candidates_{args.run_date}.csv"
    )

    conn = connect_db(db_path)
    ensure_schema(conn)

    mapping_rows, unresolved_rows, unresolved_candidate_rows = resolve_asset_tickers(
        conn=conn,
        run_date=args.run_date,
        provider=args.provider,
        asset_values_csv=asset_values_csv,
        overrides_path=args.overrides_path,
        unresolved_assets_csv=unresolved_assets_csv,
        unresolved_candidates_csv=unresolved_candidates_csv,
        disable_auto_search=args.disable_auto_search,
    )
    print(
        f"Ticker resolution complete provider={args.provider} run_date={args.run_date} "
        f"mapping_rows={mapping_rows} unresolved_assets={unresolved_rows} "
        f"unresolved_candidates={unresolved_candidate_rows}"
    )
    print(f"Unresolved assets CSV: {unresolved_assets_csv}")
    print(f"Unresolved candidates CSV: {unresolved_candidates_csv}")


if __name__ == "__main__":
    main()
