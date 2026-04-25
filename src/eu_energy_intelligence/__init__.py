"""EU Energy Intelligence Platform package."""

from eu_energy_intelligence.settings import PlatformConfig
from eu_energy_intelligence.scaffold import generate_production_scaffold

__all__ = ["PlatformConfig", "__version__", "generate_production_scaffold"]

__version__ = "0.1.0"
