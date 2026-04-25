"""Alert payload helpers."""

from __future__ import annotations


def build_alert(message: str, severity: str = "warning") -> dict[str, str]:
    """Build a simple alert payload."""
    return {"severity": severity.upper(), "message": message}
