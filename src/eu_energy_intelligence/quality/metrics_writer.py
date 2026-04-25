"""Helpers for quality metric payloads."""

from __future__ import annotations

from datetime import UTC, datetime


def build_dq_metric_record(
    rule_set_name: str,
    target_table: str,
    total_rows: int,
    passed_rows: int,
) -> dict[str, object]:
    """Build a serializable DQ metric record."""
    failed_rows = total_rows - passed_rows
    pass_rate = passed_rows / total_rows if total_rows else 1.0
    return {
        "rule_set_name": rule_set_name,
        "target_table": target_table,
        "total_rows": total_rows,
        "passed_rows": passed_rows,
        "failed_rows": failed_rows,
        "pass_rate": pass_rate,
        "validated_at": datetime.now(UTC).isoformat(),
    }
