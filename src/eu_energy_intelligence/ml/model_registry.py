"""Model registry metadata helpers."""

from __future__ import annotations

from datetime import UTC, datetime


def register_model_metadata(model_name: str, version: str, run_id: str) -> dict[str, str]:
    """Return model-registration metadata for logging or persistence."""
    return {
        "model_name": model_name,
        "version": version,
        "run_id": run_id,
        "registered_at": datetime.now(UTC).isoformat(),
    }
