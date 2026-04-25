from __future__ import annotations

from pathlib import Path

import yaml


TYPE_MAP = {
    "string": str,
    "double": (int, float),
    "integer": int,
    "boolean": bool,
}


def load_contract(path: str) -> dict:
    """Load a YAML data contract."""
    with Path(path).open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def validate_contract_columns(rows: list[dict[str, object]], contract: dict) -> None:
    """Validate row keys against a simple data contract."""
    if not rows:
        return

    expected = {field["name"] for field in contract["fields"]}
    actual = set(rows[0].keys())
    missing = expected - actual
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")


def validate_contract_rows(rows: list[dict[str, object]], contract: dict) -> None:
    """Validate row values against nullable and type constraints in a contract."""
    if not rows:
        return

    for index, row in enumerate(rows):
        for field in contract["fields"]:
            field_name = field["name"]
            value = row.get(field_name)

            if value is None:
                if not field.get("nullable", True):
                    raise ValueError(f"Row {index} field '{field_name}' must not be null")
                continue

            expected_type = TYPE_MAP.get(field["type"])
            if expected_type is None:
                raise ValueError(f"Unsupported contract type '{field['type']}' for field '{field_name}'")

            if field["type"] == "integer":
                is_valid = isinstance(value, int) and not isinstance(value, bool)
            elif field["type"] == "double":
                is_valid = isinstance(value, expected_type) and not isinstance(value, bool)
            else:
                is_valid = isinstance(value, expected_type)

            if not is_valid:
                raise ValueError(
                    f"Row {index} field '{field_name}' expected type {field['type']}, "
                    f"got {type(value).__name__}"
                )
