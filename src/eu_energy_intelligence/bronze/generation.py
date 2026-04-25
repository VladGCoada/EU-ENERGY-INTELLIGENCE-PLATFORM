from __future__ import annotations

from pathlib import Path

from eu_energy_intelligence.ingestion.parsers import parse_generation_xml


def build_bronze_generation(
    raw_file: str,
    country_code: str | None = None,
) -> list[dict[str, object]]:
    """Build Bronze-ready rows from a raw generation XML file."""
    xml_text = Path(raw_file).read_text(encoding="utf-8")
    return parse_generation_xml(xml_text, source_file=raw_file, country_code=country_code)


def build_bronze(file_path: str) -> list[dict[str, object]]:
    """Compatibility alias for Bronze generation parsing."""
    return build_bronze_generation(file_path)
