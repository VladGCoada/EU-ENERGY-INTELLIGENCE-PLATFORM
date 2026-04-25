"""Very small cost-estimation helpers."""

from __future__ import annotations


def estimate_job_cost(dbus: float, rate_per_dbu: float) -> float:
    """Estimate execution cost from DBU consumption."""
    return round(dbus * rate_per_dbu, 4)
