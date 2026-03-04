from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd

from src.common.config import load_yaml


@dataclass
class AccountRule:
    account_id: str
    account_name: str
    account_type: str | None
    tax_wrapper: str | None
    name_patterns: list[str]


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.strip().lower()).strip("_")


def load_account_rules(config_path: str = "config/accounts.yml") -> list[AccountRule]:
    config = load_yaml(config_path)
    rules: list[AccountRule] = []
    for row in config.get("accounts", []):
        rules.append(
            AccountRule(
                account_id=str(row.get("account_id", "")).strip(),
                account_name=str(row.get("account_name", "")).strip(),
                account_type=row.get("account_type"),
                tax_wrapper=row.get("tax_wrapper"),
                name_patterns=[str(x).lower().strip() for x in row.get("name_patterns", []) if str(x).strip()],
            )
        )
    return [r for r in rules if r.account_id]


def resolve_account_id(raw_account_id: str | None, raw_account_name: str, rules: list[AccountRule]) -> str:
    if raw_account_id and str(raw_account_id).strip():
        return str(raw_account_id).strip()

    lower_name = str(raw_account_name or "").lower()
    for rule in rules:
        if any(pattern in lower_name for pattern in rule.name_patterns):
            return rule.account_id

    fallback_name = raw_account_name if str(raw_account_name).strip() else "unknown"
    return _slug(fallback_name).upper()


def build_dim_account_df(
    raw_accounts: pd.DataFrame,
    rules: list[AccountRule],
) -> pd.DataFrame:
    if raw_accounts.empty:
        return pd.DataFrame(
            columns=["account_id", "account_name", "account_type", "tax_wrapper", "opened_date", "closed_date"]
        )

    rows: dict[str, dict[str, str | None]] = {}
    rule_by_id = {r.account_id: r for r in rules}

    for _, record in raw_accounts.iterrows():
        aid = str(record["account_id"])
        if aid not in rows:
            rule = rule_by_id.get(aid)
            rows[aid] = {
                "account_id": aid,
                "account_name": rule.account_name if rule else str(record["account_name_raw"]),
                "account_type": rule.account_type if rule else None,
                "tax_wrapper": rule.tax_wrapper if rule else None,
                "opened_date": None,
                "closed_date": None,
            }

    return pd.DataFrame(rows.values())
