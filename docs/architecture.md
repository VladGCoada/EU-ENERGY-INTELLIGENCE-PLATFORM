# Architecture Overview

The baseline monolith in `ALL_CODE_BASELINE.py` has been decomposed into a production-style package layout:

- `settings.py`: environment loading, YAML config resolution, and `PlatformConfig`
- `constants.py`: ENTSO-E and domain constants
- `ingestion/`: API clients, raw landing helpers, XML parsers, and ENTSO-E zone metadata
- `bronze/`: raw XML to normalized row models
- `silver/`: validation, deduplication, and quarantine logic
- `gold/`: country-level analytical aggregates
- `platinum/`: serving marts that combine Gold metrics with exogenous drivers
- `quality/`: expectations, rule registry, and validator scaffolding
- `compliance/`: DORA incident classification, GDPR erasure flow, and PII detection
- `intelligence/`: regime detection, anomaly scoring, and rolling forecast interfaces
- `streaming/`: local event envelopes and micro-batch buffering
- `observability/`: run metadata helpers and audit logging
- `tasks/`: shared base class for task-oriented orchestration
- `orchestration/`: local execution entrypoints and pipeline runner
- `utils/`: generic helpers for filesystem, dates, identifiers, and Spark

Supporting deployment scaffolding from `EU_ENERGY_PLATFORM_EXTENSION.py` is also now represented with:

- `databricks.yml`
- `.github/workflows/ci.yml`
- `conf/staging.yml`
- `conf/prod.yml`

This keeps the code notebook-independent and testable from normal Python entrypoints while preserving the direction of the larger production extension.
