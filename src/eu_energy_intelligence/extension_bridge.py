from __future__ import annotations

from importlib import import_module
from types import ModuleType


def _load_extension() -> ModuleType:
    """Return the package-native production extension module."""
    return import_module("eu_energy_intelligence.production_extension")


_EXTENSION = _load_extension()

PlatformConfig = _EXTENSION.PlatformConfig
BaseTask = _EXTENSION.BaseTask
ProductionEntsoeClient = _EXTENSION.ProductionEntsoeClient

ENTSOE_PRICE_SCHEMA = _EXTENSION.ENTSOE_PRICE_SCHEMA
ENTSOE_GENERATION_SCHEMA = _EXTENSION.ENTSOE_GENERATION_SCHEMA
ENTSOE_LOAD_SCHEMA = _EXTENSION.ENTSOE_LOAD_SCHEMA
ENTSOE_FLOW_SCHEMA = _EXTENSION.ENTSOE_FLOW_SCHEMA
DQ_STATS_SCHEMA = _EXTENSION.DQ_STATS_SCHEMA
PIPELINE_RUN_SCHEMA = _EXTENSION.PIPELINE_RUN_SCHEMA
DORA_INCIDENT_SCHEMA = _EXTENSION.DORA_INCIDENT_SCHEMA
GDPR_ERASURE_SCHEMA = _EXTENSION.GDPR_ERASURE_SCHEMA
REGIME_SIGNAL_SCHEMA = _EXTENSION.REGIME_SIGNAL_SCHEMA

ZONE_EIC = _EXTENSION.ZONE_EIC
FLOW_CORRIDORS = _EXTENSION.FLOW_CORRIDORS
RENEWABLE_PSR_TYPES = _EXTENSION.RENEWABLE_PSR_TYPES

PricesBronzeTask = _EXTENSION.PricesBronzeTask
GenerationBronzeTask = _EXTENSION.GenerationBronzeTask
LoadBronzeTask = _EXTENSION.LoadBronzeTask
FlowsBronzeTask = _EXTENSION.FlowsBronzeTask

SilverPricesTask = _EXTENSION.SilverPricesTask
SilverGenerationTask = _EXTENSION.SilverGenerationTask
SilverLoadTask = _EXTENSION.SilverLoadTask
SilverFlowsTask = _EXTENSION.SilverFlowsTask

RegimeModel = _EXTENSION.RegimeModel
RegimeDetector = _EXTENSION.RegimeDetector

FactPowerPricesTask = _EXTENSION.FactPowerPricesTask
MartDailyMarketTask = _EXTENSION.MartDailyMarketTask
MartPriceSpreadsTask = _EXTENSION.MartPriceSpreadsTask
MartRegimeSignalsTask = _EXTENSION.MartRegimeSignalsTask

AuditLogTask = _EXTENSION.AuditLogTask
DQCriticalFailure = _EXTENSION.DQCriticalFailure
DQValidator = _EXTENSION.DQValidator
DoraIncidentClassifier = _EXTENSION.DoraIncidentClassifier
GdprErasurePipeline = _EXTENSION.GdprErasurePipeline
PiiTagger = _EXTENSION.PiiTagger
PipelineRunner = _EXTENSION.PipelineRunner
generate_production_scaffold = _EXTENSION.generate_production_scaffold
main = _EXTENSION.main
