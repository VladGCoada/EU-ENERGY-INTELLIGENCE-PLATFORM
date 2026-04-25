from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

from eu_energy_intelligence.settings import PlatformConfig
from eu_energy_intelligence.utils.spark import get_spark


class BaseTask(ABC):
    """Shared task interface adapted from the production extension baseline."""

    def __init__(self, config: PlatformConfig | None = None) -> None:
        self.config = config or PlatformConfig()
        self._logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def run(self) -> dict[str, Any]:
        """Execute the task and return metrics."""

    def spark(self):
        return get_spark(self.__class__.__name__)

    def log(self, message: str, level: str = "info") -> None:
        getattr(self._logger, level)(message)

    def table(self, schema: str, name: str) -> str:
        return f"{self.config.catalog}.{schema}.{name}"

    def empty_metrics(self) -> dict[str, int]:
        return {"rows_read": 0, "rows_written": 0, "rows_quarantined": 0}
