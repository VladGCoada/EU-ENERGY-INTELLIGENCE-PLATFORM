from __future__ import annotations


def expect_non_negative(
    rows: list[dict[str, object]],
    column_name: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Expectation: column must be present and non-negative."""
    passed: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []

    for row in rows:
        value = row.get(column_name)
        if isinstance(value, (int, float)) and value >= 0:
            passed.append(row)
        else:
            failed.append(row)

    return passed, failed


def expect_not_null(
    rows: list[dict[str, object]],
    column_name: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Expectation: column must not be null."""
    passed: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []

    for row in rows:
        if row.get(column_name) is None:
            failed.append(row)
        else:
            passed.append(row)

    return passed, failed
