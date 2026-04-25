"""Backfill orchestration helpers."""

from __future__ import annotations

from eu_energy_intelligence.orchestration.run_bronze import build_bronze_run_plan


def build_backfill_plan(raw_files: list[str], country_code: str) -> list[dict[str, object]]:
    """Build a simple per-file backfill plan."""
    return [build_bronze_run_plan(raw_file, country_code) for raw_file in raw_files]
