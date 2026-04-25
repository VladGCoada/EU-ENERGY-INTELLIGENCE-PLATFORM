"""Lightweight SQL template helpers."""

from __future__ import annotations


def create_or_replace_view_sql(view_name: str, query_sql: str) -> str:
    """Build a CREATE OR REPLACE VIEW statement."""
    return f"CREATE OR REPLACE VIEW {view_name} AS\n{query_sql}\n"
