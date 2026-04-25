from __future__ import annotations

import uuid
from datetime import UTC, datetime


def generate_run_id() -> str:
    """Generate a unique pipeline run identifier."""
    timestamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    return f"{timestamp}_{uuid.uuid4().hex[:8]}"


def build_table_name(catalog: str, schema: str, table: str) -> str:
    """Build a Unity Catalog style table name."""
    return f"{catalog}.{schema}.{table}"
