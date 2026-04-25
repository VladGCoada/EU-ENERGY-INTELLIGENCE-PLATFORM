from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from eu_energy_intelligence.bronze.generation import build_bronze_generation
from eu_energy_intelligence.gold.renewable_stability import build_renewable_stability
from eu_energy_intelligence.ingestion.extract_generation import run_generation_extract
from eu_energy_intelligence.observability.runs import run_pipeline_with_logging
from eu_energy_intelligence.quality import (
    load_contract,
    validate_contract_columns,
    validate_contract_rows,
)
from eu_energy_intelligence.settings import load_config
from eu_energy_intelligence.silver.build_energy_timeseries import add_event_timestamps
from eu_energy_intelligence.silver.generation import build_generation_silver
from eu_energy_intelligence.utils.spark import get_spark

GOLD_RENEWABLE_STABILITY_CONTRACT = "conf/data_contracts/gold_renewable_stability.yaml"


def _validate_gold_renewable_stability(rows: list[dict[str, Any]]) -> None:
    """Enforce the gold renewable stability contract before writing outputs."""
    contract = load_contract(GOLD_RENEWABLE_STABILITY_CONTRACT)
    validate_contract_columns(rows, contract)
    validate_contract_rows(rows, contract)


def _write_rows_as_dataset(
    rows: list[dict[str, Any]],
    output_path: str,
    app_name: str,
) -> str | None:
    """Write row dictionaries to parquet when possible, else fall back to JSON."""
    if not rows:
        return None

    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        spark = get_spark(app_name)
        spark.createDataFrame(rows).write.mode("overwrite").parquet(str(output_dir / "parquet"))
    except Exception:
        (output_dir / "records.json").write_text(
            json.dumps(rows, indent=2),
            encoding="utf-8",
        )
    return str(output_dir)


def _write_manifest(summary: dict[str, Any], output_path: str) -> str:
    manifest_path = Path(output_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return str(manifest_path)


def run_local_spark_bronze(raw_file: str, country_code: str = "DE") -> dict[str, int]:
    """Run a local Bronze flow using pure Python row normalization."""
    rows = build_bronze_generation(raw_file, country_code=country_code)
    return {"rows_read": len(rows), "rows_written": len(rows), "rows_quarantined": 0}


def run_local_silver_generation(raw_file: str, country_code: str = "DE") -> dict[str, int]:
    """Run a local Silver flow from one raw XML file."""
    bronze_rows = build_bronze_generation(raw_file, country_code=country_code)
    valid_rows, quarantine_rows = build_generation_silver(bronze_rows)
    return {
        "rows_read": len(bronze_rows),
        "rows_written": len(valid_rows),
        "rows_quarantined": len(quarantine_rows),
    }


def run_local_gold_renewable_stability(raw_file: str, country_code: str = "DE") -> dict[str, int]:
    """Run a local Gold flow from one raw XML file."""
    bronze_rows = build_bronze_generation(raw_file, country_code=country_code)
    silver_rows, quarantine_rows = build_generation_silver(bronze_rows)
    gold_rows = build_renewable_stability(silver_rows)
    _validate_gold_renewable_stability(gold_rows)
    return {
        "rows_read": len(bronze_rows),
        "rows_written": len(gold_rows),
        "rows_quarantined": len(quarantine_rows),
    }


def run_local_generation_pipeline(
    raw_file: str,
    country_code: str = "DE",
    processed_base_dir: str = "data/processed",
    period_start: str = "202401010000",
) -> dict[str, Any]:
    """Run local generation Bronze, Silver, and Gold and persist local outputs."""
    bronze_rows = build_bronze_generation(raw_file, country_code=country_code)
    silver_rows, quarantine_rows = build_generation_silver(bronze_rows)
    silver_rows = add_event_timestamps(silver_rows, period_start)
    gold_rows = build_renewable_stability(silver_rows)
    _validate_gold_renewable_stability(gold_rows)

    base_path = Path(processed_base_dir)
    bronze_path = _write_rows_as_dataset(
        bronze_rows,
        str(base_path / "bronze" / "generation"),
        "eu-energy-intelligence-bronze",
    )
    silver_path = _write_rows_as_dataset(
        silver_rows,
        str(base_path / "silver" / "generation"),
        "eu-energy-intelligence-silver",
    )
    quarantine_path = _write_rows_as_dataset(
        quarantine_rows,
        str(base_path / "silver" / "quarantine_generation"),
        "eu-energy-intelligence-quarantine",
    )
    gold_path = _write_rows_as_dataset(
        gold_rows,
        str(base_path / "gold" / "renewable_stability"),
        "eu-energy-intelligence-gold",
    )

    summary = {
        "raw_file": raw_file,
        "country_code": country_code,
        "rows_read": len(bronze_rows),
        "bronze_rows_written": len(bronze_rows),
        "silver_rows_written": len(silver_rows),
        "gold_rows_written": len(gold_rows),
        "rows_quarantined": len(quarantine_rows),
        "bronze_path": bronze_path,
        "silver_path": silver_path,
        "quarantine_path": quarantine_path,
        "gold_path": gold_path,
    }
    summary["manifest_path"] = _write_manifest(
        summary,
        str(base_path / "manifests" / f"generation_{country_code.lower()}.json"),
    )
    return summary


def run_live_local_generation_pipeline(
    country_code: str = "NL",
    period_start: str = "202401010000",
    period_end: str = "202401020000",
    processed_base_dir: str | None = None,
) -> dict[str, Any]:
    """Fetch a live raw generation payload and run the local persistence pipeline."""
    config = load_config("dev")
    from eu_energy_intelligence.constants import COUNTRY_EIC_CODES

    normalized_country = country_code.upper().replace("-", "")
    if normalized_country not in COUNTRY_EIC_CODES:
        raise ValueError(f"Unknown country code '{country_code}'")

    raw_file = run_generation_extract(
        COUNTRY_EIC_CODES[normalized_country],
        period_start,
        period_end,
    )
    target_processed_dir = processed_base_dir or config["processed_data_dir"]
    return run_local_generation_pipeline(
        raw_file=raw_file,
        country_code=country_code.upper(),
        processed_base_dir=target_processed_dir,
        period_start=period_start,
    )


def run_local_full_pipeline(raw_file: str, country_code: str = "DE") -> list[dict[str, int]]:
    """Run Bronze, Silver, and Gold sequentially with shared observability wrapping."""
    steps = [
        ("bronze_generation", "bronze", lambda _: run_local_spark_bronze(raw_file, country_code)),
        (
            "silver_generation",
            "silver",
            lambda _: run_local_silver_generation(raw_file, country_code),
        ),
        (
            "gold_renewable_stability",
            "gold",
            lambda _: run_local_gold_renewable_stability(raw_file, country_code),
        ),
    ]

    return [
        run_pipeline_with_logging(pipeline_name=name, layer=layer, fn=runner)
        for name, layer, runner in steps
    ]


def resolve_latest_raw_generation_file(base_dir: str = "data/raw/generation") -> str:
    """Resolve the newest local raw generation XML file."""
    raw_candidates = sorted(Path(base_dir).rglob("*.xml"))
    if not raw_candidates:
        raise FileNotFoundError(f"No raw generation XML files found under {base_dir}")
    return str(raw_candidates[-1])
