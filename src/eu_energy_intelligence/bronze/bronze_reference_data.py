"""Reference-data Bronze helpers."""

from __future__ import annotations

from eu_energy_intelligence.ingestion.extract_reference_data import extract_reference_data


def build_reference_data_bronze() -> dict[str, object]:
    """Build the static Bronze reference-data payload."""
    return extract_reference_data()
