"""Observability orchestration helpers."""

from __future__ import annotations

from datetime import UTC, datetime

from eu_energy_intelligence.observability import build_pipeline_run_record


def run_observability_snapshot(
    pipeline_name: str,
    task_name: str,
) -> dict[str, object]:
    """Build a standard observability snapshot payload."""
    now = datetime.now(UTC)
    return build_pipeline_run_record(
        pipeline_name=pipeline_name,
        layer="ops",
        task_name=task_name,
        started_at=now,
        finished_at=now,
        rows_read=0,
        rows_written=0,
        rows_quarantined=0,
        dq_pass_rate=None,
        status="SUCCESS",
        error_message=None,
    )
