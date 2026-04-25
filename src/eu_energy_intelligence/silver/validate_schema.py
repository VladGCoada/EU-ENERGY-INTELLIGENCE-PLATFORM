"""Schema validation helpers for Silver datasets."""

from __future__ import annotations


def validate_required_columns(
    rows: list[dict[str, object]],
    required_columns: list[str],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Split rows by whether all required columns are present."""
    passed: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []
    for row in rows:
        if all(column in row for column in required_columns):
            passed.append(row)
        else:
            failed.append(row)
    return passed, failed
