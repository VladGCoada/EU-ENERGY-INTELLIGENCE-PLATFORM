from pathlib import Path

from eu_energy_intelligence.orchestration.local import (
    run_local_full_pipeline,
    run_local_generation_pipeline,
    run_local_gold_renewable_stability,
    run_local_silver_generation,
    run_local_spark_bronze,
)
from eu_energy_intelligence.orchestration.run_bronze import (
    _default_period_range,
    build_bronze_run_plan,
)

RAW_FIXTURE = "tests/fixtures/entsoe_generation_sample.xml"


def test_build_bronze_run_plan_uses_fixture() -> None:
    run_plan = build_bronze_run_plan(RAW_FIXTURE, "NL")

    assert run_plan == {
        "env": "dev",
        "raw_file": RAW_FIXTURE,
        "row_count": 2,
        "country_code": "NL",
    }


def test_local_orchestration_helpers_return_counts() -> None:
    bronze_result = run_local_spark_bronze(RAW_FIXTURE, country_code="NL")
    silver_result = run_local_silver_generation(RAW_FIXTURE, country_code="NL")
    gold_result = run_local_gold_renewable_stability(RAW_FIXTURE, country_code="NL")
    full_result = run_local_full_pipeline(RAW_FIXTURE, country_code="NL")

    assert bronze_result == {"rows_read": 2, "rows_written": 2, "rows_quarantined": 0}
    assert silver_result == {"rows_read": 2, "rows_written": 2, "rows_quarantined": 0}
    assert gold_result == {"rows_read": 2, "rows_written": 1, "rows_quarantined": 0}
    assert len(full_result) == 3


def test_local_generation_pipeline_persists_outputs(tmp_path: Path) -> None:
    result = run_local_generation_pipeline(
        RAW_FIXTURE,
        country_code="NL",
        processed_base_dir=str(tmp_path),
    )

    assert result["rows_read"] == 2
    assert result["bronze_rows_written"] == 2
    assert result["silver_rows_written"] == 2
    assert result["gold_rows_written"] == 1
    assert Path(result["bronze_path"]).exists()
    assert Path(result["silver_path"]).exists()
    assert Path(result["gold_path"]).exists()
    assert Path(result["manifest_path"]).exists()


def test_default_period_range_is_one_day_window() -> None:
    period_start, period_end = _default_period_range()

    assert len(period_start) == 12
    assert len(period_end) == 12
    assert period_start.endswith("0000")
    assert period_end.endswith("0000")
