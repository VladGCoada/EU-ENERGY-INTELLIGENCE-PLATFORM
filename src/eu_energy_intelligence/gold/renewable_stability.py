from __future__ import annotations

from collections import defaultdict
from datetime import datetime


def _extract_event_date(row: dict[str, object]) -> str:
    value = (
        row.get("event_timestamp_utc")
        or row.get("timestamp_utc")
        or row.get("datetime")
        or row.get("timestamp")
    )

    if value is None:
        return "UNKNOWN_DATE"

    text = str(value)
    return text[:10]


def build_renewable_stability(rows: list[dict[str, object]]) -> list[dict[str, float | str]]:
    """Aggregate generation metrics by country and event date."""
    grouped: dict[tuple[str, str], list[float]] = defaultdict(list)

    for row in rows:
        country_code = str(row.get("country_code") or "UNKNOWN")
        event_date = _extract_event_date(row)

        quantity = row.get("quantity")
        if quantity is None:
            quantity = row.get("generation_mwh")
        if quantity is None:
            quantity = row.get("generation_mw")

        if isinstance(quantity, (int, float)):
            grouped[(country_code, event_date)].append(float(quantity))

    results: list[dict[str, float | str]] = []

    for (country_code, event_date), quantities in grouped.items():
        total_generation = sum(quantities)
        avg_generation = total_generation / len(quantities)
        max_generation = max(quantities)
        min_generation = min(quantities)

        results.append(
            {
                "country_code": country_code,
                "event_date": event_date,
                "total_generation": total_generation,
                "avg_generation": avg_generation,
                "max_generation": max_generation,
                "min_generation": min_generation,
                "volatility_index": max_generation - min_generation,
                "interval_count": float(len(quantities)),
            }
        )

    return sorted(results, key=lambda r: (str(r["country_code"]), str(r["event_date"])))
