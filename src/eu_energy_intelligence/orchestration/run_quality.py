"""Quality orchestration helpers."""

from __future__ import annotations

from typing import Any

from eu_energy_intelligence.quality import DQValidator
from eu_energy_intelligence.settings import PlatformConfig


def run_quality_validation(
    rows: list[dict[str, Any]],
    rule_set_name: str,
    target_table: str,
    run_id: str,
    config: PlatformConfig | None = None,
) -> tuple[list[dict[str, Any]], float]:
    """Run record-level DQ validation against a rule set."""
    validator = DQValidator(config)
    return validator.validate_records(rows, rule_set_name, target_table, run_id)
