"""Import dependency analytics."""

from __future__ import annotations


def build_import_dependency(
    rows: list[dict[str, object]],
    import_field: str = "imports_mw",
    demand_field: str = "demand_mw",
) -> list[dict[str, object]]:
    """Compute import dependency ratios."""
    output: list[dict[str, object]] = []
    for row in rows:
        imports = row.get(import_field)
        demand = row.get(demand_field)
        ratio = None
        if isinstance(imports, (int, float)) and isinstance(demand, (int, float)) and demand:
            ratio = float(imports) / float(demand)
        output.append({**row, "import_dependency_ratio": ratio})
    return output
