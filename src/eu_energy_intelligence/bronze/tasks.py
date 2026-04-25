"""Production Bronze tasks sourced from the extension monolith."""

from eu_energy_intelligence.extension_bridge import (
    FlowsBronzeTask,
    GenerationBronzeTask,
    LoadBronzeTask,
    PricesBronzeTask,
)

__all__ = [
    "FlowsBronzeTask",
    "GenerationBronzeTask",
    "LoadBronzeTask",
    "PricesBronzeTask",
]
