"""Late-arrival tagging helpers."""

from __future__ import annotations

from datetime import UTC, datetime


def tag_late_arrivals(
    rows: list[dict[str, object]],
    timestamp_field: str,
    watermark_iso: str,
) -> list[dict[str, object]]:
    """Annotate rows that arrive before a configured watermark."""
    watermark = datetime.fromisoformat(watermark_iso.replace("Z", "+00:00")).astimezone(UTC)
    tagged: list[dict[str, object]] = []
    for row in rows:
        timestamp_value = row.get(timestamp_field)
        is_late = False
        if isinstance(timestamp_value, str):
            parsed = datetime.fromisoformat(timestamp_value.replace("Z", "+00:00")).astimezone(UTC)
            is_late = parsed < watermark
        tagged.append({**row, "is_late_arrival": is_late})
    return tagged
