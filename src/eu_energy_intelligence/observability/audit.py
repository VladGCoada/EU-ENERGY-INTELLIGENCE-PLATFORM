from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from eu_energy_intelligence.tasks.base import BaseTask


class AuditLogTask(BaseTask):
    """Collect pipeline run records without making the logger itself a hard dependency."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.records: list[dict[str, Any]] = []

    def run(self) -> dict[str, Any]:
        return self.empty_metrics()

    def log_run(
        self,
        run_id: str,
        pipeline_name: str,
        task_name: str,
        started_at: datetime,
        finished_at: datetime | None,
        rows_read: int,
        rows_written: int,
        rows_quarantined: int,
        dq_pass_rate: float | None,
        status: str,
        error_message: str | None = None,
    ) -> None:
        self.records.append(
            {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "task_name": task_name,
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat()
                if finished_at
                else datetime.now(UTC).isoformat(),
                "rows_read": rows_read,
                "rows_written": rows_written,
                "rows_quarantined": rows_quarantined,
                "dq_pass_rate": dq_pass_rate,
                "status": status,
                "error_message": error_message,
            }
        )
