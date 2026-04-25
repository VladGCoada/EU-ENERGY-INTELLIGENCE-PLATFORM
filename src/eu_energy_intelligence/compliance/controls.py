from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime
from typing import Any

from eu_energy_intelligence.tasks.base import BaseTask


class DoraIncidentClassifier(BaseTask):
    """Classify failures into simplified DORA severity tiers."""

    MAJOR_THRESHOLD_EUR = 10_000_000
    MAJOR_DURATION_MIN = 240
    MAJOR_CLIENTS = 10_000
    SIGNIFICANT_THRESHOLD_EUR = 1_000_000
    SIGNIFICANT_DURATION_MIN = 60
    SIGNIFICANT_CLIENTS = 1_000

    def run(self) -> dict[str, Any]:
        return self.empty_metrics()

    def classify(
        self,
        pipeline_run_id: str,
        error_message: str,
        duration_minutes: int,
        affected_clients_est: int = 0,
        impacted_value_eur: float = 0.0,
        is_cross_border: bool = False,
    ) -> dict[str, Any]:
        severity, reason, eba_reportable = self._classify_severity(
            duration_minutes,
            affected_clients_est,
            impacted_value_eur,
            is_cross_border,
        )
        return {
            "incident_id": str(uuid.uuid4()),
            "detected_at": datetime.now(UTC).isoformat(),
            "pipeline_run_id": pipeline_run_id,
            "error_message": error_message,
            "severity": severity,
            "affected_clients_est": affected_clients_est,
            "impacted_value_eur": impacted_value_eur,
            "duration_minutes": duration_minutes,
            "is_cross_border": is_cross_border,
            "classification_reason": reason,
            "eba_reportable": eba_reportable,
            "created_at": datetime.now(UTC).isoformat(),
        }

    def _classify_severity(
        self,
        duration_minutes: int,
        affected_clients_est: int,
        impacted_value_eur: float,
        is_cross_border: bool,
    ) -> tuple[str, str, bool]:
        if (
            duration_minutes >= self.MAJOR_DURATION_MIN
            or affected_clients_est >= self.MAJOR_CLIENTS
            or impacted_value_eur >= self.MAJOR_THRESHOLD_EUR
        ):
            return "MAJOR", "major threshold exceeded", True
        if (
            duration_minutes >= self.SIGNIFICANT_DURATION_MIN
            or affected_clients_est >= self.SIGNIFICANT_CLIENTS
            or impacted_value_eur >= self.SIGNIFICANT_THRESHOLD_EUR
            or is_cross_border
        ):
            return "SIGNIFICANT", "significant threshold or cross-border impact", True
        return "MINOR", "below all significance thresholds", False


class GdprErasurePipeline(BaseTask):
    """Minimal in-memory GDPR erasure cascade model for orchestration and tests."""

    def run(self) -> dict[str, Any]:
        return self.empty_metrics()

    def process_request(self, entity_id: str, operator: str = "pipeline") -> dict[str, Any]:
        return {
            "erasure_id": str(uuid.uuid4()),
            "entity_id": entity_id,
            "requested_at": datetime.now(UTC).isoformat(),
            "completed_at": datetime.now(UTC).isoformat(),
            "status": "COMPLETED",
            "bronze_rows_deleted": 0,
            "silver_rows_deleted": 0,
            "gold_rows_deleted": 0,
            "operator": operator,
        }


class PiiTagger(BaseTask):
    """Identify potential PII columns using the extension baseline patterns."""

    PII_COLUMN_PATTERNS = [
        re.compile(r".*iban.*", re.IGNORECASE),
        re.compile(r".*email.*", re.IGNORECASE),
        re.compile(r".*name.*", re.IGNORECASE),
        re.compile(r".*phone.*", re.IGNORECASE),
        re.compile(r".*address.*", re.IGNORECASE),
        re.compile(r".*bic.*", re.IGNORECASE),
    ]

    def run(self) -> dict[str, Any]:
        return self.empty_metrics()

    def detect_columns(self, columns: list[str]) -> list[str]:
        return [
            column
            for column in columns
            if any(pattern.match(column) for pattern in self.PII_COLUMN_PATTERNS)
        ]
