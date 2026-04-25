"""Anomaly scoring helpers."""

from __future__ import annotations

from typing import Any

from eu_energy_intelligence.intelligence import AnomalyScorer


def score_anomalies(
    rows: list[dict[str, Any]],
    feature_cols: list[str],
    scorer: AnomalyScorer | None = None,
) -> list[dict[str, Any]]:
    """Score rows with an anomaly model."""
    model = scorer or AnomalyScorer()
    return model.score(rows, feature_cols)
