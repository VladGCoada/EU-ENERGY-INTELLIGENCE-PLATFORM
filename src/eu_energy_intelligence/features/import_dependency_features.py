"""Import-dependency feature engineering helpers."""

from __future__ import annotations


def build_import_dependency_features(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Build model features from import dependency ratios."""
    output: list[dict[str, object]] = []
    for row in rows:
        ratio = row.get("import_dependency_ratio")
        stress_bucket = "UNKNOWN"
        if isinstance(ratio, (int, float)):
            if ratio >= 0.5:
                stress_bucket = "HIGH"
            elif ratio >= 0.2:
                stress_bucket = "MEDIUM"
            else:
                stress_bucket = "LOW"
        output.append({**row, "import_dependency_bucket": stress_bucket})
    return output
