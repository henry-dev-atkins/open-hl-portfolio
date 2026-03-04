from __future__ import annotations

import hashlib
import re


NON_ASSET_PATTERNS = (
    r"\bpage\s+\d+\b",
    r"\bhl investment report\b",
    r"\bportfolio overview\b",
    r"\bcapital account transactions\b",
    r"\bincome account transactions\b",
    r"\baccount balance\b",
    r"\b(stocks\s*&?\s*shares\s+isa|lifetime\s+isa|sipp)\s+\d{1,2}\s+[a-z]+\s+\d{4}\b",
    r"\bsubtotal\b",
    r"\bstock total\b",
    r"\btotal\b",
)


def canonicalize_asset_name(asset_name: str) -> str:
    text = str(asset_name or "").strip()
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\.\.\.+$", "", text)
    text = re.sub(r"[^A-Za-z0-9&'().,%/+ \-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip().lower()
    for pattern in NON_ASSET_PATTERNS:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"\s+", " ", text).strip()
    return text


def make_asset_id(canonical_name: str) -> str:
    digest = hashlib.sha1(canonical_name.encode("utf-8")).hexdigest()[:16]
    return f"ASSET_{digest.upper()}"
