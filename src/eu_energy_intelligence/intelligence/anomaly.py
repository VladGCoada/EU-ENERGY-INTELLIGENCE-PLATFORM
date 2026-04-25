from __future__ import annotations

from typing import Any

from sklearn.ensemble import IsolationForest


class AnomalyScorer:
    """Isolation-forest wrapper for extension-style anomaly scoring."""

    def __init__(self, contamination: float = 0.1, random_state: int = 42) -> None:
        self.model = IsolationForest(contamination=contamination, random_state=random_state)
        self._is_fit = False

    def fit(self, rows: list[dict[str, Any]], feature_cols: list[str]) -> None:
        matrix = _to_matrix(rows, feature_cols)
        if not matrix:
            raise ValueError("Cannot fit anomaly model on empty rows")
        self.model.fit(matrix)
        self._is_fit = True

    def score(self, rows: list[dict[str, Any]], feature_cols: list[str]) -> list[dict[str, Any]]:
        matrix = _to_matrix(rows, feature_cols)
        if not matrix:
            return []

        if not self._is_fit:
            self.model.fit(matrix)
            self._is_fit = True

        raw_scores = self.model.decision_function(matrix)
        predictions = self.model.predict(matrix)
        scored_rows: list[dict[str, Any]] = []
        for row, raw_score, prediction in zip(rows, raw_scores, predictions, strict=False):
            scored_rows.append(
                {
                    **row,
                    "anomaly_score": round(float(-raw_score), 6),
                    "is_anomaly": prediction == -1,
                }
            )
        return scored_rows


def _to_matrix(rows: list[dict[str, Any]], feature_cols: list[str]) -> list[list[float]]:
    return [[float(row.get(column, 0.0) or 0.0) for column in feature_cols] for row in rows]
