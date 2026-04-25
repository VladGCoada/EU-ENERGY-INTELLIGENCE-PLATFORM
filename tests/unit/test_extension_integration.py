from eu_energy_intelligence.bronze import PricesBronzeTask
from eu_energy_intelligence import production_extension
from eu_energy_intelligence.gold import FactPowerPricesTask
from eu_energy_intelligence.orchestration import (
    LocalPipelineRunner,
    PipelineRunner,
    ProductionPipelineRunner,
)
from eu_energy_intelligence.schemas import ENTSOE_PRICE_SCHEMA
from eu_energy_intelligence.scaffold import generate_production_scaffold
from eu_energy_intelligence.silver import SilverPricesTask


def test_extension_tasks_are_exposed_through_package_modules() -> None:
    assert PricesBronzeTask.__name__ == "PricesBronzeTask"
    assert SilverPricesTask.__name__ == "SilverPricesTask"
    assert FactPowerPricesTask.__name__ == "FactPowerPricesTask"


def test_extension_schema_is_available_from_package() -> None:
    assert ENTSOE_PRICE_SCHEMA is None or any(
        field.name == "price_eur_mwh" for field in ENTSOE_PRICE_SCHEMA.fields
    )


def test_both_local_and_production_pipeline_runners_are_exposed() -> None:
    assert LocalPipelineRunner.__name__ == "PipelineRunner"
    assert PipelineRunner.__name__ == "PipelineRunner"
    assert ProductionPipelineRunner.__name__ == "PipelineRunner"


def test_scaffold_function_is_importable() -> None:
    assert callable(generate_production_scaffold)


def test_package_native_production_extension_module_is_available() -> None:
    assert production_extension.PlatformConfig.__name__ == "PlatformConfig"
    assert production_extension.PipelineRunner.__name__ == "PipelineRunner"
