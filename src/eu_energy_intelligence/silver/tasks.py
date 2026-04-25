"""Production Silver tasks sourced from the extension monolith."""

from eu_energy_intelligence.extension_bridge import (
    SilverFlowsTask,
    SilverGenerationTask,
    SilverLoadTask,
    SilverPricesTask,
)

__all__ = [
    "SilverFlowsTask",
    "SilverGenerationTask",
    "SilverLoadTask",
    "SilverPricesTask",
]
