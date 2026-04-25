from __future__ import annotations

from typing import Any


class RollingWindowForecaster:
    """Simple forecasting baseline when heavier extension models are unavailable."""

    def __init__(self, window: int = 3) -> None:
        if window <= 0:
            raise ValueError("window must be positive")
        self.window = window

    def forecast(self, rows: list[dict[str, Any]], value_field: str) -> list[dict[str, Any]]:
        history: list[float] = []
        forecast_rows: list[dict[str, Any]] = []
        for row in rows:
            current_value = float(row.get(value_field, 0.0) or 0.0)
            trailing = history[-self.window :]
            prediction = sum(trailing) / len(trailing) if trailing else current_value
            forecast_rows.append(
                {
                    **row,
                    "forecast_value": round(prediction, 6),
                    "forecast_error": round(current_value - prediction, 6),
                }
            )
            history.append(current_value)
        return forecast_rows
