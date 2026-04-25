from __future__ import annotations

import xml.etree.ElementTree as ET


def parse_generation_xml(
    xml_text: str,
    source_file: str | None = None,
    country_code: str | None = None,
) -> list[dict[str, object]]:
    """Parse ENTSO-E generation XML into simple row dictionaries."""
    root = ET.fromstring(xml_text)
    rows: list[dict[str, object]] = []

    for point in root.findall(".//{*}Point"):
        position = point.findtext("{*}position")
        quantity = point.findtext("{*}quantity")
        rows.append(
            {
                "position": int(position) if position else None,
                "quantity": float(quantity) if quantity else None,
                "source_file": source_file,
                "country_code": country_code,
                "dataset_name": "generation",
            }
        )

    return rows


def parse_price_xml(
    xml_text: str,
    source_file: str | None = None,
    country_code: str | None = None,
) -> list[dict[str, object]]:
    """Parse ENTSO-E price XML into simple row dictionaries."""
    root = ET.fromstring(xml_text)
    rows: list[dict[str, object]] = []

    for point in root.findall(".//{*}Point"):
        position = point.findtext("{*}position")
        price = point.findtext("{*}price.amount")
        rows.append(
            {
                "position": int(position) if position else None,
                "price_eur_mwh": float(price) if price else None,
                "source_file": source_file,
                "country_code": country_code,
                "dataset_name": "prices",
            }
        )

    return rows
