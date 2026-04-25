"""XML normalization compatibility helpers."""

from __future__ import annotations

from eu_energy_intelligence.ingestion.parsers import parse_generation_xml, parse_price_xml


def normalize_generation_xml(xml_text: str) -> list[dict[str, object]]:
    """Normalize generation XML payloads into row dictionaries."""
    return parse_generation_xml(xml_text)


def normalize_price_xml(xml_text: str) -> list[dict[str, object]]:
    """Normalize price XML payloads into row dictionaries."""
    return parse_price_xml(xml_text)
