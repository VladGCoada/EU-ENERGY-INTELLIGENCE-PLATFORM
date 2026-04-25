# EU Energy Intelligence Platform

Modular Python and PySpark codebase for ingesting, normalizing, validating, and serving EU energy intelligence datasets.

## Package structure

- `src/eu_energy_intelligence/ingestion`: ENTSO-E client, raw writers, XML parsers, and extraction functions
- `src/eu_energy_intelligence/bronze`: raw-to-Bronze normalization
- `src/eu_energy_intelligence/silver`: Silver validation and quarantine logic
- `src/eu_energy_intelligence/gold`: Gold-level metrics and aggregations
- `src/eu_energy_intelligence/platinum`: serving-friendly marts that blend Gold with exogenous signals
- `src/eu_energy_intelligence/quality`: reusable data quality and contract helpers
- `src/eu_energy_intelligence/compliance`: DORA incident classification, GDPR erasure, and PII tagging helpers
- `src/eu_energy_intelligence/intelligence`: regime detection, anomaly scoring, and rolling forecast helpers
- `src/eu_energy_intelligence/streaming`: local streaming-compatible event models and batch buffers
- `src/eu_energy_intelligence/observability`: pipeline run records and logging helpers
- `src/eu_energy_intelligence/tasks`: shared task base class for orchestration
- `src/eu_energy_intelligence/orchestration`: local execution entrypoints
- `src/eu_energy_intelligence/utils`: shared filesystem, dates, identifiers, and Spark helpers

## Local workflow

1. Create and activate a virtual environment.
2. Install the project in editable mode with dev dependencies.
3. Set `ENTSOE_API_KEY` in your shell or a local `.env`.
4. Run `ruff check .`
5. Run `pytest`

## Extension-derived additions

- `PlatformConfig` support for the `EMIT_*` environment model from `EU_ENERGY_PLATFORM_EXTENSION.py`
- `ProductionEntsoeClient` metadata for bidding zones, corridors, and time-resolution handling
- DQ rule registry and validator structure
- compliance and intelligence modules ready for the next Spark-heavy phase
- external weather, carbon-intensity, and ECB FX clients for exogenous feature ingestion
- `platinum` market summary builder plus local CLI and streaming helpers
- `PipelineRunner`, `databricks.yml`, and GitHub Actions CI scaffold

## Documentation

- Architecture overview: `docs/architecture.md`
- Environment config: `conf/base.yml`, `conf/dev.yml`
