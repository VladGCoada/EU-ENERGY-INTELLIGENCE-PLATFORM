from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass(slots=True)
class RegimeModel:
    """Metadata container for a trained or stubbed regime detector."""

    isolation_forest: Any = None
    scaler: Any = None
    feature_cols: list[str] = field(
        default_factory=lambda: [
            "price_eur_mwh",
            "price_z_score",
            "renewable_share_pct",
            "abs_forecast_error_mw",
        ]
    )
    mlflow_run_id: str = "local_stub"
    model_version: str = "1"
    training_data_version: int = 0
    trained_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class RegimeDetector:
    """Rule-based subset of the extension's regime detection behavior."""

    def __init__(self, model: RegimeModel | None = None) -> None:
        self.model = model or RegimeModel()

    def classify_point(self, price_eur_mwh: float, anomaly_score: float = 0.0) -> str:
        if price_eur_mwh < 0:
            return "NEGATIVE"
        if price_eur_mwh > 200:
            return "SPIKE"
        if anomaly_score > 0.6:
            return "STRESS"
        return "NORMAL"

    def score_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        scored_rows: list[dict[str, Any]] = []
        for row in rows:
            anomaly_score = float(row.get("anomaly_score", 0.0) or 0.0)
            price = float(row.get("price_eur_mwh", 0.0) or 0.0)
            scored_rows.append(
                {
                    **row,
                    "regime_label": self.classify_point(price, anomaly_score),
                    "regime_confidence": row.get("regime_confidence", 0.85),
                    "model_version": self.model.model_version,
                    "model_run_id": self.model.mlflow_run_id,
                }
            )
        return scored_rows
