from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from eu_energy_intelligence.settings import get_env
from eu_energy_intelligence.utils.identifiers import generate_run_id


def build_pipeline_run_record(
    pipeline_name: str,
    run_id: str,
    layer: str,
    status: str,
    rows_read: int = 0,
    rows_written: int = 0,
    rows_quarantined: int = 0,
    error_message: str | None = None,
) -> dict[str, object]:
    """Build an observability record for one pipeline execution."""
    now = datetime.now(UTC).isoformat()
    return {
        "pipeline_name": pipeline_name,
        "run_id": run_id,
        "layer": layer,
        "status": status,
        "start_ts": now,
        "end_ts": now,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "rows_quarantined": rows_quarantined,
        "error_message": error_message,
        "environment": get_env("APP_ENV", "dev"),
    }


def log_run(pipeline: str, status: str) -> dict[str, str]:
    """Return a lightweight log payload for simple call sites."""
    return {
        "pipeline": pipeline,
        "status": status,
        "timestamp": datetime.now(UTC).isoformat(),
    }


def run_pipeline_with_logging(
    pipeline_name: str,
    layer: str,
    fn: Callable[[str], dict[str, int]],
) -> dict[str, int]:
    """Run a callable and return its result while shaping a standard failure payload."""
    run_id = generate_run_id()
    try:
        result = fn(run_id)
        build_pipeline_run_record(
            pipeline_name=pipeline_name,
            run_id=run_id,
            layer=layer,
            status="SUCCESS",
            rows_read=result.get("rows_read", 0),
            rows_written=result.get("rows_written", 0),
            rows_quarantined=result.get("rows_quarantined", 0),
        )
        return result
    except Exception as exc:
        build_pipeline_run_record(
            pipeline_name=pipeline_name,
            run_id=run_id,
            layer=layer,
            status="FAILED",
            error_message=str(exc),
        )
        raise
