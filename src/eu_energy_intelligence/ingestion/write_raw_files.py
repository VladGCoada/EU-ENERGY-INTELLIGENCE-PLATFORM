from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from eu_energy_intelligence.logging_config import get_logger
from eu_energy_intelligence.utils.io import ensure_dir

logger = get_logger(__name__)


def write_raw_xml(dataset: str, country: str, payload: str, base_dir: str) -> str:
    """Write raw XML payload to a local raw-zone style folder structure."""
    date_str = datetime.now(UTC).strftime("%Y-%m-%d")
    target_dir = Path(base_dir) / dataset / country / f"ingestion_date={date_str}"
    ensure_dir(str(target_dir))

    file_path = target_dir / f"{dataset}_{country}_{datetime.now(UTC).strftime('%H%M%S')}.xml"
    file_path.write_text(payload, encoding="utf-8")

    logger.info("Wrote raw %s file to %s", dataset, file_path)
    return str(file_path)


def write_raw(dataset: str, country: str, payload: str, base_dir: str) -> str:
    """Compatibility alias for earlier snippets."""
    return write_raw_xml(dataset, country, payload, base_dir)
