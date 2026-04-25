from __future__ import annotations

import json
from pathlib import Path

from eu_energy_intelligence.gold.renewable_stability import build_renewable_stability


def main() -> None:
    silver_path = Path("data/processed/silver/generation/records.json")
    gold_dir = Path("data/processed/gold/renewable_stability")
    gold_dir.mkdir(parents=True, exist_ok=True)

    if not silver_path.exists():
        raise FileNotFoundError(f"Silver file not found: {silver_path}")

    rows = json.loads(silver_path.read_text(encoding="utf-8"))

    print(f"Gold input rows: {len(rows)}")
    if rows:
        print(f"Gold sample row: {rows[0]}")

    gold_rows = build_renewable_stability(rows)

    output_path = gold_dir / "records.json"
    output_path.write_text(json.dumps(gold_rows, indent=2), encoding="utf-8")

    print(f"Gold rows written: {len(gold_rows)}")
    print(f"Gold output: {output_path}")


if __name__ == "__main__":
    main()
