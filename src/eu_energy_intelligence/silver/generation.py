from __future__ import annotations


def deduplicate_generation_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Deduplicate simple generation rows by country, position, and source file."""
    seen: set[tuple[object, object, object]] = set()
    deduplicated: list[dict[str, object]] = []

    for row in rows:
        key = (row.get("country_code"), row.get("position"), row.get("source_file"))
        if key in seen:
            continue
        seen.add(key)
        deduplicated.append(row)

    return deduplicated


def split_valid_and_quarantine_rows(
    rows: list[dict[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Split valid and invalid generation measurements."""
    valid_rows: list[dict[str, object]] = []
    quarantined_rows: list[dict[str, object]] = []

    for row in rows:
        quantity = row.get("quantity")
        if isinstance(quantity, (int, float)) and quantity >= 0:
            valid_rows.append(row)
        else:
            quarantined_rows.append(row)

    return valid_rows, quarantined_rows


def build_generation_silver(
    rows: list[dict[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Build Silver valid and quarantine row sets from Bronze rows."""
    deduplicated_rows = deduplicate_generation_rows(rows)
    return split_valid_and_quarantine_rows(deduplicated_rows)
