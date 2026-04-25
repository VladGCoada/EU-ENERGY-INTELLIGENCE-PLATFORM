"""Simple local checkpoint helpers for ingestion state."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_checkpoint(path: str) -> dict[str, Any]:
    checkpoint_path = Path(path)
    if not checkpoint_path.exists():
        return {}
    return json.loads(checkpoint_path.read_text(encoding="utf-8"))


def save_checkpoint(path: str, state: dict[str, Any]) -> str:
    checkpoint_path = Path(path)
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    return str(checkpoint_path)
