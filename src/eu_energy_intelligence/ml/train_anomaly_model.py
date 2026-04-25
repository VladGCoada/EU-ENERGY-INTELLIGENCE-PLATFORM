"""Anomaly model training helpers."""

from __future__ import annotations

from typing import Any

from eu_energy_intelligence.intelligence import AnomalyScorer


def train_anomaly_model(
    rows: list[dict[str, Any]],
    feature_cols: list[str],
) -> AnomalyScorer:
    """Fit and return a simple anomaly scorer."""
    scorer = AnomalyScorer()
    scorer.fit(rows, feature_cols)
    return scorer
