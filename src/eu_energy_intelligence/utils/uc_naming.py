"""Unity Catalog naming helpers."""

from __future__ import annotations


def build_uc_table_name(catalog: str, schema: str, table: str) -> str:
    """Build a Unity Catalog fully qualified table name."""
    return f"{catalog}.{schema}.{table}"
