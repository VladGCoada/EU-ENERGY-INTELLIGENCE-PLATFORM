"""Silver orchestration entrypoints."""

from __future__ import annotations

from eu_energy_intelligence.settings import PlatformConfig
from eu_energy_intelligence.silver.tasks import (
    SilverFlowsTask,
    SilverGenerationTask,
    SilverLoadTask,
    SilverPricesTask,
)


def run_silver_tasks(config: PlatformConfig | None = None) -> list[dict[str, object]]:
    platform_config = config or PlatformConfig()
    return [
        SilverPricesTask(platform_config).run(),
        SilverGenerationTask(platform_config).run(),
        SilverLoadTask(platform_config).run(),
        SilverFlowsTask(platform_config).run(),
    ]
