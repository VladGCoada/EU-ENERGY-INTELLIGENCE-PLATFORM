from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from eu_energy_intelligence.compliance import DoraIncidentClassifier
from eu_energy_intelligence.observability.audit import AuditLogTask
from eu_energy_intelligence.quality.validator import DQCriticalFailure, DQValidator
from eu_energy_intelligence.settings import PlatformConfig
from eu_energy_intelligence.tasks.base import BaseTask


class PipelineRunner(BaseTask):
    """High-level orchestrator adapted from the extension file."""

    def __init__(self, config: PlatformConfig | None = None) -> None:
        super().__init__(config)
        self.audit = AuditLogTask(self.config)
        self.dora = DoraIncidentClassifier(self.config)
        self.dq = DQValidator(self.config)

    def run(self) -> dict[str, Any]:
        run_id = str(uuid.uuid4())
        started = datetime.now(UTC)
        totals = self.empty_metrics()

        for task_name, task in self._task_plan():
            t0 = datetime.now(UTC)
            try:
                metrics = task.run()
                for key in totals:
                    totals[key] += int(metrics.get(key, 0))
                self.audit.log_run(
                    run_id,
                    "eu_energy_intelligence",
                    task_name,
                    t0,
                    datetime.now(UTC),
                    int(metrics.get("rows_read", 0)),
                    int(metrics.get("rows_written", 0)),
                    int(metrics.get("rows_quarantined", 0)),
                    None,
                    "SUCCESS",
                )
            except DQCriticalFailure as exc:
                self.audit.log_run(
                    run_id,
                    "eu_energy_intelligence",
                    task_name,
                    t0,
                    datetime.now(UTC),
                    0,
                    0,
                    0,
                    0.0,
                    "FAILED",
                    str(exc),
                )
                self.dora.classify(
                    pipeline_run_id=run_id,
                    error_message=str(exc),
                    duration_minutes=int((datetime.now(UTC) - started).total_seconds() / 60),
                )
                raise
            except Exception as exc:  # pragma: no cover - defensive orchestration branch
                self.audit.log_run(
                    run_id,
                    "eu_energy_intelligence",
                    task_name,
                    t0,
                    datetime.now(UTC),
                    0,
                    0,
                    0,
                    None,
                    "FAILED",
                    str(exc),
                )
                raise

        return totals

    def _task_plan(self) -> list[tuple[str, BaseTask]]:
        return []
