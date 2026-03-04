from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from src.common.paths import PROJECT_ROOT


def load_yaml(path: str | Path) -> dict[str, Any]:
    raw_path = Path(path)
    if not raw_path.is_absolute():
        raw_path = PROJECT_ROOT / raw_path
    if not raw_path.exists():
        return {}
    with raw_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        return {}
    return data
