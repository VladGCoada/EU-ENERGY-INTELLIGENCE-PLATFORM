"""Production Gold tasks sourced from the extension monolith."""

from eu_energy_intelligence.extension_bridge import (
    FactPowerPricesTask,
    MartDailyMarketTask,
    MartPriceSpreadsTask,
    MartRegimeSignalsTask,
)

__all__ = [
    "FactPowerPricesTask",
    "MartDailyMarketTask",
    "MartPriceSpreadsTask",
    "MartRegimeSignalsTask",
]
