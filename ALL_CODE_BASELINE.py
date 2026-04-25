"""
EU ENERGY INTELLIGENCE PLATFORM — MONOLITHIC BASELINE SCRIPT

This file consolidates the code/scripts discussed in the conversation into one
Codex-friendly baseline file.

Purpose:
- Provide a single source file for Codex to refactor into a production repo.
- Include ingestion, raw persistence, XML parsing, Bronze/Silver/Gold logic,
  Spark/Delta helpers, quality checks, observability, orchestration, and tests-like
  helper functions in one place.

Recommended Codex prompt:
"Refactor ALL_CODE_BASELINE.py into a production-ready src/ Python project with
ingestion/, bronze/, silver/, gold/, quality/, observability/, orchestration/,
utils/, tests/, conf/, databricks/, infra/. Preserve behavior and add tests."

NOTE:
This script is intentionally monolithic. It is not the final architecture.
"""

# =============================================================================
# STANDARD LIBRARY IMPORTS
# =============================================================================

import os
import json
import uuid
import yaml
import logging
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Callable

# =============================================================================
# OPTIONAL THIRD-PARTY IMPORTS
# =============================================================================

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv(*args, **kwargs):
        return None

try:
    from lxml import etree
except ImportError:  # pragma: no cover
    etree = None

try:
    from pyspark.sql import SparkSession, DataFrame, Window
    from pyspark.sql.functions import (
        col,
        to_timestamp,
        avg,
        stddev,
        when,
        sum as spark_sum,
        max as spark_max,
        min as spark_min,
    )
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        DoubleType,
        IntegerType,
    )
except ImportError:  # pragma: no cover
    SparkSession = None
    DataFrame = Any
    Window = None
    col = None
    to_timestamp = None
    avg = None
    stddev = None
    when = None
    spark_sum = None
    spark_max = None
    spark_min = None
    StructType = None
    StructField = None
    StringType = None
    DoubleType = None
    IntegerType = None

try:
    from delta.tables import DeltaTable
except ImportError:  # pragma: no cover
    DeltaTable = None


# =============================================================================
# ENV + SETTINGS
# =============================================================================

load_dotenv()

def get_env(name: str, default: Any = None) -> Any:
    """Read an environment variable."""
    return os.getenv(name, default)


def get_env_var(name: str, default: str | None = None) -> str | None:
    """Alias used in earlier snippets."""
    return os.getenv(name, default)


def load_config(env: str = "dev") -> dict:
    """Load YAML config from conf/<env>.yml."""
    config_path = Path("conf") / f"{env}.yml"
    if not config_path.exists():
        return {
            "env": env,
            "catalog": "energy_dev",
            "schemas": {
                "bronze": "bronze",
                "silver": "silver",
                "gold": "gold",
                "ops": "ops",
            },
            "countries": ["DE", "NL", "DK"],
            "raw_data_dir": get_env("RAW_DATA_DIR", "./data/raw"),
            "processed_data_dir": get_env("PROCESSED_DATA_DIR", "./data/processed"),
        }

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# =============================================================================
# CONSTANTS
# =============================================================================

RENEWABLE_TYPES = {
    "Solar",
    "Wind Offshore",
    "Wind Onshore",
    "Hydro Water Reservoir",
    "Hydro Run-of-river and poundage",
    "Biomass",
    "Geothermal",
}

ENTSOE_BASE_URL = "https://web-api.tp.entsoe.eu/api"

COUNTRY_EIC_CODES = {
    "DE": "10Y1001A1001A83F",
    "NL": "10YNL----------L",
    "DK1": "10YDK-1--------W",
    "DK2": "10YDK-2--------M",
}


# =============================================================================
# LOGGING
# =============================================================================

def get_logger(name: str) -> logging.Logger:
    """Create a basic logger."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logging.getLogger(name)


logger = get_logger(__name__)


# =============================================================================
# BASIC UTILITIES
# =============================================================================

def ensure_dir(path: str) -> None:
    """Create a directory if it does not exist."""
    Path(path).mkdir(parents=True, exist_ok=True)


def date_range_days(start: datetime, end: datetime):
    """Yield each date between start and end inclusive."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def generate_run_id() -> str:
    """Generate a unique run id."""
    return f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"


def build_table_name(catalog: str, schema: str, table: str) -> str:
    """Build Unity Catalog style table name."""
    return f"{catalog}.{schema}.{table}"


# =============================================================================
# SPARK SESSION + DELTA HELPERS
# =============================================================================

def get_spark(app_name: str = "eu-energy-intelligence") -> SparkSession:
    """Create a local SparkSession with Delta configs when available."""
    if SparkSession is None:
        raise ImportError("pyspark is not installed. Install pyspark and delta-spark.")

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def write_delta(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Write a DataFrame to Delta."""
    df.write.format("delta").mode(mode).save(path)


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """Read a Delta table by path."""
    return spark.read.format("delta").load(path)


def write_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Write a DataFrame to Parquet."""
    df.write.mode(mode).parquet(path)


def merge_on_keys(spark: SparkSession, df: DataFrame, target_path: str, condition: str) -> None:
    """Delta merge/upsert helper."""
    if DeltaTable is None:
        raise ImportError("delta-spark is not installed or DeltaTable is unavailable.")

    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        (
            target.alias("t")
            .merge(df.alias("s"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(target_path)


# =============================================================================
# ENTSO-E CLIENT + RAW WRITERS
# =============================================================================

class EntsoeClient:
    """Simple ENTSO-E Transparency Platform API client."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("ENTSOE_API_KEY")
        if not self.api_key:
            raise ValueError("ENTSOE_API_KEY not set")

        if requests is None:
            raise ImportError("requests is not installed.")

    def get(self, params: dict) -> str:
        params = {**params, "securityToken": self.api_key}
        response = requests.get(ENTSOE_BASE_URL, params=params, timeout=60)
        response.raise_for_status()
        logger.info("Fetched ENTSO-E data with params=%s", {k: v for k, v in params.items() if k != "securityToken"})
        return response.text


def write_raw_xml(dataset: str, country: str, payload: str, base_dir: str) -> str:
    """Write raw XML payload to local raw zone."""
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    target_dir = Path(base_dir) / dataset / country / f"ingestion_date={date_str}"
    ensure_dir(str(target_dir))

    file_path = target_dir / f"{dataset}_{country}_{datetime.utcnow().strftime('%H%M%S')}.xml"
    file_path.write_text(payload, encoding="utf-8")

    print(f"Saved {file_path} | size={len(payload)} bytes")
    logger.info("Wrote raw %s file to %s", dataset, file_path)
    return str(file_path)


def write_raw(dataset: str, country: str, payload: str, base_dir: str) -> str:
    """Alias for earlier snippets."""
    return write_raw_xml(dataset, country, payload, base_dir)


# =============================================================================
# ENTSO-E EXTRACTION FUNCTIONS
# =============================================================================

def run_generation_extract(country: str, period_start: str, period_end: str) -> str:
    """Extract generation data from ENTSO-E and save raw XML."""
    config = load_config(get_env("ENV", "dev"))
    client = EntsoeClient()
    payload = client.get({
        "documentType": "A75",
        "processType": "A16",
        "in_Domain": country,
        "periodStart": period_start,
        "periodEnd": period_end,
    })
    return write_raw_xml("generation", country, payload, config["raw_data_dir"])


def run_load_extract(country: str, period_start: str, period_end: str) -> str:
    """Extract load data from ENTSO-E and save raw XML."""
    config = load_config(get_env("ENV", "dev"))
    client = EntsoeClient()
    payload = client.get({
        "documentType": "A65",
        "processType": "A16",
        "outBiddingZone_Domain": country,
        "periodStart": period_start,
        "periodEnd": period_end,
    })
    return write_raw_xml("load", country, payload, config["raw_data_dir"])


def run_prices_extract(country: str, period_start: str, period_end: str) -> str:
    """Extract day-ahead prices from ENTSO-E and save raw XML."""
    config = load_config(get_env("ENV", "dev"))
    client = EntsoeClient()
    payload = client.get({
        "documentType": "A44",
        "in_Domain": country,
        "out_Domain": country,
        "periodStart": period_start,
        "periodEnd": period_end,
    })
    return write_raw_xml("prices", country, payload, config["raw_data_dir"])


def run_crossborder_extract(
    in_domain: str,
    out_domain: str,
    period_start: str,
    period_end: str,
) -> str:
    """Extract cross-border flow data from ENTSO-E and save raw XML.

    This endpoint/query is a placeholder pattern. Parameters may need adjusting
    depending on the exact ENTSO-E data item.
    """
    config = load_config(get_env("ENV", "dev"))
    client = EntsoeClient()
    payload = client.get({
        "documentType": "A11",
        "in_Domain": in_domain,
        "out_Domain": out_domain,
        "periodStart": period_start,
        "periodEnd": period_end,
    })
    country_pair = f"{in_domain}_to_{out_domain}".replace("-", "")
    return write_raw_xml("crossborder", country_pair, payload, config["raw_data_dir"])


# =============================================================================
# XML NORMALIZATION / PARSING
# =============================================================================

def parse_generation_xml(
    xml_text: str,
    source_file: str | None = None,
    country_code: str | None = None,
) -> list[dict]:
    """Parse ENTSO-E generation XML into simple rows.

    This parser intentionally starts simple. ENTSO-E XML has nested periods,
    positions, time intervals, and production types. A production-grade parser
    should extract period start/end, resolution, production type, and timestamps.
    """
    if etree is None:
        raise ImportError("lxml is not installed.")

    root = etree.fromstring(xml_text.encode("utf-8"))
    rows = []

    for point in root.findall(".//{*}Point"):
        position = point.findtext("{*}position")
        quantity = point.findtext("{*}quantity")

        rows.append({
            "position": int(position) if position else None,
            "quantity": float(quantity) if quantity else None,
            "source_file": source_file,
            "country_code": country_code,
            "dataset_name": "generation",
        })

    return rows


def parse_generation(xml_text: str) -> list[dict]:
    """Short alias from earlier snippets."""
    return parse_generation_xml(xml_text)


def parse_price_xml(
    xml_text: str,
    source_file: str | None = None,
    country_code: str | None = None,
) -> list[dict]:
    """Parse ENTSO-E price XML into simple rows.

    Placeholder parser. It searches for Point/price.amount. Real parser should
    derive event timestamps from Period/timeInterval + resolution.
    """
    if etree is None:
        raise ImportError("lxml is not installed.")

    root = etree.fromstring(xml_text.encode("utf-8"))
    rows = []

    for point in root.findall(".//{*}Point"):
        position = point.findtext("{*}position")
        price = point.findtext("{*}price.amount")

        rows.append({
            "position": int(position) if position else None,
            "price_eur_mwh": float(price) if price else None,
            "source_file": source_file,
            "country_code": country_code,
            "dataset_name": "prices",
        })

    return rows


# =============================================================================
# BRONZE LAYER
# =============================================================================

if StructType is not None:
    BRONZE_GENERATION_SCHEMA = StructType([
        StructField("position", IntegerType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("source_file", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("dataset_name", StringType(), True),
        StructField("event_timestamp_utc", StringType(), True),
    ])
else:
    BRONZE_GENERATION_SCHEMA = None


def build_bronze_generation(raw_file: str, country_code: str | None = None) -> list[dict]:
    """Build Bronze rows from raw generation XML."""
    xml_text = Path(raw_file).read_text(encoding="utf-8")
    return parse_generation_xml(xml_text, source_file=raw_file, country_code=country_code)


def build_bronze(file_path: str) -> list[dict]:
    """Short alias from earlier snippets."""
    xml = Path(file_path).read_text(encoding="utf-8")
    return parse_generation(xml)


def write_bronze_generation(rows: list[dict], output_path: str, use_delta: bool = True) -> None:
    """Write Bronze generation rows as Delta or Parquet."""
    spark = get_spark("bronze-generation-writer")
    df = spark.createDataFrame(rows)

    if use_delta:
        write_delta(df, output_path)
    else:
        write_parquet(df, output_path)


def write_bronze(rows: list[dict], path: str) -> None:
    """Alias from earlier snippets."""
    write_bronze_generation(rows, path, use_delta=True)


# =============================================================================
# SILVER LAYER
# =============================================================================

def validate_generation_schema(df: DataFrame) -> DataFrame:
    """Basic schema validation for generation rows."""
    return df.filter(col("position").isNotNull())


def deduplicate_generation(df: DataFrame) -> DataFrame:
    """Deduplicate generation records."""
    dedupe_cols = [c for c in ["country_code", "position", "source_file"] if c in df.columns]
    return df.dropDuplicates(dedupe_cols) if dedupe_cols else df.dropDuplicates()


def split_valid_and_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split valid and invalid generation measurements."""
    valid_df = df.filter(col("quantity").isNotNull() & (col("quantity") >= 0))
    quarantine_df = df.filter(col("quantity").isNull() | (col("quantity") < 0))
    return valid_df, quarantine_df


def build_generation_silver(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Build Silver generation and quarantine DataFrames."""
    df = validate_generation_schema(df)
    df = deduplicate_generation(df)
    return split_valid_and_quarantine(df)


def build_silver(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Alias from earlier snippets."""
    return build_generation_silver(df)


def write_silver(df: DataFrame, output_path: str, use_delta: bool = True) -> None:
    """Write Silver DataFrame."""
    if use_delta:
        write_delta(df, output_path)
    else:
        write_parquet(df, output_path)


def write_quarantine(df: DataFrame, output_path: str, use_delta: bool = True) -> None:
    """Write quarantine DataFrame."""
    if use_delta:
        write_delta(df, output_path)
    else:
        write_parquet(df, output_path)


def standardize_mwh(value: float, unit: str) -> float:
    """Standardize energy values to MWh."""
    if unit == "MWh":
        return value
    if unit == "kWh":
        return value / 1000
    return value


def is_valid_measurement(value: float | None) -> bool:
    """Basic measurement validity rule."""
    return value is not None and value >= 0


def normalize_timestamps(df: DataFrame) -> DataFrame:
    """Normalize event_timestamp_utc to Spark timestamp."""
    return df.withColumn("event_timestamp_utc", to_timestamp(col("event_timestamp_utc")))


# =============================================================================
# GOLD LAYER
# =============================================================================

def aggregate_generation_metrics(df: DataFrame) -> DataFrame:
    """Aggregate generation metrics by country if available, otherwise globally."""
    group_cols = ["country_code"] if "country_code" in df.columns else []
    return df.groupBy(*group_cols).agg(
        spark_sum("quantity").alias("total_generation"),
        avg("quantity").alias("avg_generation"),
        spark_max("quantity").alias("max_generation"),
        spark_min("quantity").alias("min_generation"),
    )


def build_renewable_stability(df: DataFrame) -> DataFrame:
    """Build a basic renewable stability table."""
    metrics_df = aggregate_generation_metrics(df)
    return metrics_df.withColumn(
        "volatility_index",
        col("max_generation") - col("min_generation"),
    )


def build_gold(df: DataFrame) -> DataFrame:
    """Alias from earlier snippets."""
    return build_renewable_stability(df)


def write_gold(df: DataFrame, output_path: str, use_delta: bool = True) -> None:
    """Write Gold DataFrame."""
    if use_delta:
        write_delta(df, output_path)
    else:
        write_parquet(df, output_path)


def add_rolling_metrics(df: DataFrame) -> DataFrame:
    """Add rolling average/stddev/dip flag to time-series data.

    Requires event_timestamp_utc column.
    """
    window_spec = (
        Window.partitionBy("country_code")
        .orderBy("event_timestamp_utc")
        .rowsBetween(-24, 0)
    )
    return (
        df.withColumn("rolling_avg_generation", avg("quantity").over(window_spec))
          .withColumn("rolling_std_generation", stddev("quantity").over(window_spec))
          .withColumn("renewable_dip_flag", col("quantity") < col("rolling_avg_generation"))
    )


def build_renewable_stability_advanced(df: DataFrame) -> DataFrame:
    """Build advanced renewable stability with rolling volatility index."""
    df = add_rolling_metrics(df)
    return df.withColumn(
        "renewable_volatility_index",
        when(
            col("rolling_avg_generation") != 0,
            col("rolling_std_generation") / col("rolling_avg_generation"),
        ).otherwise(0.0),
    )


def build_price_spike_analysis(df: DataFrame) -> DataFrame:
    """Build price spike analysis using rolling z-score-like logic."""
    window_spec = (
        Window.partitionBy("country_code")
        .orderBy("event_timestamp_utc")
        .rowsBetween(-24, 0)
    )
    df = (
        df.withColumn("price_24h_avg", avg("price_eur_mwh").over(window_spec))
          .withColumn("price_24h_stddev", stddev("price_eur_mwh").over(window_spec))
    )
    return df.withColumn(
        "price_spike_flag",
        when(
            col("price_eur_mwh") > col("price_24h_avg") + 2 * col("price_24h_stddev"),
            True,
        ).otherwise(False),
    )


def build_import_dependency(flows_df: DataFrame, load_df: DataFrame) -> DataFrame:
    """Build import dependency metrics.

    Expects flows_df: country_code, net_import_mwh
    Expects load_df: country_code, quantity
    """
    flow_agg = flows_df.groupBy("country_code").agg(
        spark_sum("net_import_mwh").alias("total_net_import_mwh")
    )
    load_agg = load_df.groupBy("country_code").agg(
        spark_sum("quantity").alias("total_load_mwh")
    )
    joined = flow_agg.join(load_agg, "country_code", "inner")
    return joined.withColumn(
        "import_dependency_pct",
        when(
            col("total_load_mwh") != 0,
            col("total_net_import_mwh") / col("total_load_mwh"),
        ).otherwise(0.0),
    )


# =============================================================================
# QUALITY CHECKS + DATA CONTRACTS
# =============================================================================

def expect_non_negative(df: DataFrame, column_name: str) -> tuple[DataFrame, DataFrame]:
    """Expectation: column must be >= 0."""
    passed = df.filter(col(column_name) >= 0)
    failed = df.filter(col(column_name) < 0)
    return passed, failed


def expect_not_null(df: DataFrame, column_name: str) -> tuple[DataFrame, DataFrame]:
    """Expectation: column must not be null."""
    passed = df.filter(col(column_name).isNotNull())
    failed = df.filter(col(column_name).isNull())
    return passed, failed


def build_quality_metric(
    table_name: str,
    check_name: str,
    check_result: str,
    failed_rows: int,
    run_id: str,
) -> dict:
    """Create a data quality metric row."""
    return {
        "table_name": table_name,
        "check_name": check_name,
        "check_result": check_result,
        "failed_rows": failed_rows,
        "run_id": run_id,
        "event_ts": datetime.utcnow().isoformat(),
        "environment": get_env("ENV", "dev"),
    }


def load_contract(path: str) -> dict:
    """Load a YAML data contract."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def validate_contract_columns(df: DataFrame, contract: dict) -> None:
    """Validate DataFrame columns against a simple data contract."""
    expected = {field["name"] for field in contract["fields"]}
    actual = set(df.columns)
    missing = expected - actual
    if missing:
        raise ValueError(f"Missing columns: {missing}")


# =============================================================================
# OBSERVABILITY
# =============================================================================

def build_pipeline_run_record(
    pipeline_name: str,
    run_id: str,
    layer: str,
    status: str,
    rows_read: int = 0,
    rows_written: int = 0,
    rows_quarantined: int = 0,
    error_message: str | None = None,
) -> dict:
    """Build one ops.pipeline_runs style record."""
    now = datetime.utcnow().isoformat()
    return {
        "pipeline_name": pipeline_name,
        "run_id": run_id,
        "layer": layer,
        "status": status,
        "start_ts": now,
        "end_ts": now,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "rows_quarantined": rows_quarantined,
        "error_message": error_message,
        "environment": get_env("ENV", "dev"),
    }


def log_run(pipeline: str, status: str) -> dict:
    """Small logger dict from earlier snippets."""
    return {
        "pipeline": pipeline,
        "status": status,
        "timestamp": datetime.utcnow().isoformat(),
    }


def write_ops_records(records: list[dict], path: str) -> None:
    """Write ops records to Delta append."""
    spark = get_spark("ops-writer")
    df = spark.createDataFrame(records)
    write_delta(df, path, mode="append")


def get_table_max_timestamp(df: DataFrame, column_name: str):
    """Return max timestamp for freshness checks."""
    return df.select(spark_max(column_name).alias("max_ts")).collect()[0]["max_ts"]


def run_pipeline_with_logging(
    pipeline_name: str,
    layer: str,
    fn: Callable[[str], dict],
    ops_path: str = "data/processed/ops/pipeline_runs",
) -> dict:
    """Run a callable with SUCCESS/FAILED ops logging."""
    run_id = generate_run_id()
    try:
        result = fn(run_id)
        record = build_pipeline_run_record(
            pipeline_name=pipeline_name,
            run_id=run_id,
            layer=layer,
            status="SUCCESS",
            rows_read=result.get("rows_read", 0),
            rows_written=result.get("rows_written", 0),
            rows_quarantined=result.get("rows_quarantined", 0),
        )
        write_ops_records([record], ops_path)
        return result
    except Exception as exc:
        record = build_pipeline_run_record(
            pipeline_name=pipeline_name,
            run_id=run_id,
            layer=layer,
            status="FAILED",
            error_message=str(exc),
        )
        try:
            write_ops_records([record], ops_path)
        except Exception:
            logger.exception("Failed to write failure ops record.")
        raise


# =============================================================================
# ORCHESTRATION: LOCAL PIPELINE RUNNERS
# =============================================================================

def run_local_spark_bronze(
    raw_file: str | None = None,
    country_code: str = "DE",
    output_path: str = "data/processed/bronze/generation",
) -> dict:
    """Run local Bronze generation pipeline."""
    if raw_file is None:
        raw_candidates = sorted(Path("data/raw/generation").rglob("*.xml"))
        if not raw_candidates:
            raise FileNotFoundError("No raw generation XML files found under data/raw/generation")
        raw_file = str(raw_candidates[-1])

    rows = build_bronze_generation(raw_file, country_code=country_code)
    write_bronze_generation(rows, output_path, use_delta=True)

    return {"rows_read": len(rows), "rows_written": len(rows), "rows_quarantined": 0}


def run_local_silver_generation(
    bronze_path: str = "data/processed/bronze/generation",
    silver_path: str = "data/processed/silver/generation",
    quarantine_path: str = "data/processed/quarantine/generation",
) -> dict:
    """Run local Silver generation pipeline."""
    spark = get_spark("silver-generation")
    bronze_df = read_delta(spark, bronze_path)
    rows_read = bronze_df.count()

    valid_df, quarantine_df = build_generation_silver(bronze_df)
    rows_written = valid_df.count()
    rows_quarantined = quarantine_df.count()

    write_silver(valid_df, silver_path, use_delta=True)
    write_quarantine(quarantine_df, quarantine_path, use_delta=True)

    return {
        "rows_read": rows_read,
        "rows_written": rows_written,
        "rows_quarantined": rows_quarantined,
    }


def run_local_gold_renewable_stability(
    silver_path: str = "data/processed/silver/generation",
    gold_path: str = "data/processed/gold/renewable_stability",
) -> dict:
    """Run local Gold renewable stability pipeline."""
    spark = get_spark("gold-renewable-stability")
    silver_df = read_delta(spark, silver_path)
    rows_read = silver_df.count()

    gold_df = build_renewable_stability(silver_df)
    rows_written = gold_df.count()

    write_gold(gold_df, gold_path, use_delta=True)

    return {
        "rows_read": rows_read,
        "rows_written": rows_written,
        "rows_quarantined": 0,
    }


def run_local_full_pipeline() -> None:
    """Run Bronze -> Silver -> Gold locally."""
    run_pipeline_with_logging(
        "bronze_generation",
        "bronze",
        lambda run_id: run_local_spark_bronze(),
    )
    run_pipeline_with_logging(
        "silver_generation",
        "silver",
        lambda run_id: run_local_silver_generation(),
    )
    run_pipeline_with_logging(
        "gold_renewable_stability",
        "gold",
        lambda run_id: run_local_gold_renewable_stability(),
    )


def run_demo_local_pipeline() -> None:
    """Run old list-based demo pipeline from earlier snippets."""
    raw_file = sorted(Path("data/raw/generation").rglob("*.xml"))[-1]
    bronze_rows = build_bronze(str(raw_file))

    spark = get_spark("demo-local")
    bronze_df = spark.createDataFrame(bronze_rows)
    silver_rows_df, quarantined_rows_df = build_silver(bronze_df)
    gold_df = build_gold(silver_rows_df)

    output_dir = Path("data/processed/demo")
    output_dir.mkdir(parents=True, exist_ok=True)

    gold_rows = [row.asDict() for row in gold_df.collect()]
    quarantine_rows = [row.asDict() for row in quarantined_rows_df.collect()]

    with open(output_dir / "gold_renewable_stability.json", "w", encoding="utf-8") as f:
        json.dump(gold_rows, f, indent=2)

    with open(output_dir / "quarantine.json", "w", encoding="utf-8") as f:
        json.dump(quarantine_rows, f, indent=2)

    print("Demo pipeline complete")
    print(gold_rows)


def run_module(module: str) -> None:
    """Run a Python module in a subprocess."""
    subprocess.check_call([sys.executable, "-m", module])


def run_job(job_name: str) -> None:
    """Config-driven job runner using conf/jobs.yml."""
    with open("conf/jobs.yml", "r", encoding="utf-8") as f:
        jobs = yaml.safe_load(f)["jobs"]

    module_name = jobs[job_name]["module"]
    run_module(module_name)


# =============================================================================
# SCAFFOLD GENERATOR HELPERS
# =============================================================================

def write_text_file(path: str, content: str) -> None:
    """Write text file with parent dirs."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")


def generate_basic_project_scaffold(root: str = ".") -> None:
    """Generate the folder structure discussed in the conversation."""
    root_path = Path(root)

    dirs = [
        "conf/data_contracts",
        "databricks/resources",
        "databricks/targets",
        "databricks/assets/sql",
        "databricks/assets/dashboards",
        "infra/terraform",
        "infra/docs",
        "src/eu_energy_intelligence/ingestion",
        "src/eu_energy_intelligence/bronze",
        "src/eu_energy_intelligence/silver",
        "src/eu_energy_intelligence/gold",
        "src/eu_energy_intelligence/quality",
        "src/eu_energy_intelligence/observability",
        "src/eu_energy_intelligence/features",
        "src/eu_energy_intelligence/ml",
        "src/eu_energy_intelligence/orchestration",
        "src/eu_energy_intelligence/utils",
        "notebooks/exploration",
        "notebooks/demos",
        "tests/unit",
        "tests/integration",
        "tests/fixtures",
        "docs",
        ".github/workflows",
        "data/raw",
        "data/processed",
    ]

    for d in dirs:
        (root_path / d).mkdir(parents=True, exist_ok=True)

    init_dirs = [
        "src/eu_energy_intelligence",
        "src/eu_energy_intelligence/ingestion",
        "src/eu_energy_intelligence/bronze",
        "src/eu_energy_intelligence/silver",
        "src/eu_energy_intelligence/gold",
        "src/eu_energy_intelligence/quality",
        "src/eu_energy_intelligence/observability",
        "src/eu_energy_intelligence/features",
        "src/eu_energy_intelligence/ml",
        "src/eu_energy_intelligence/orchestration",
        "src/eu_energy_intelligence/utils",
    ]

    for d in init_dirs:
        (root_path / d / "__init__.py").touch()

    write_text_file(str(root_path / ".env.example"), """ENTSOE_API_KEY=your_key_here
ENV=dev
CATALOG=energy_dev
BRONZE_SCHEMA=bronze
SILVER_SCHEMA=silver
GOLD_SCHEMA=gold
OPS_SCHEMA=ops
RAW_DATA_DIR=./data/raw
PROCESSED_DATA_DIR=./data/processed
""")

    write_text_file(str(root_path / ".gitignore"), """.venv/
__pycache__/
.pytest_cache/
.mypy_cache/
.ruff_cache/
*.pyc
.env
dist/
build/
*.egg-info/
.vscode/settings.json
data/
""")

    write_text_file(str(root_path / "pyproject.toml"), """[project]
name = "eu-energy-intelligence"
version = "0.1.0"
description = "EU Energy Intelligence Platform"
requires-python = ">=3.10"

[tool.black]
line-length = 100

[tool.ruff]
line-length = 100

[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests"]
""")

    write_text_file(str(root_path / "conf/dev.yml"), """env: dev
catalog: energy_dev
schemas:
  bronze: bronze
  silver: silver
  gold: gold
  ops: ops

countries:
  - DE
  - NL
  - DK

raw_data_dir: ./data/raw
processed_data_dir: ./data/processed
""")

    write_text_file(str(root_path / "conf/jobs.yml"), """jobs:
  bronze_generation:
    module: eu_energy_intelligence.orchestration.run_local_spark_bronze
  silver_generation:
    module: eu_energy_intelligence.orchestration.run_local_silver_generation
  gold_renewable_stability:
    module: eu_energy_intelligence.orchestration.run_local_gold_renewable_stability
""")

    write_text_file(str(root_path / "conf/data_contracts/gold_renewable_stability.yaml"), """name: gold_renewable_stability
owner: vlad
sla_minutes: 120
fields:
  - name: country_code
    type: string
    nullable: false
  - name: total_generation
    type: double
    nullable: true
  - name: volatility_index
    type: double
    nullable: true
""")

    write_text_file(str(root_path / "README.md"), """# EU Energy Intelligence Platform

A production-style Databricks/PySpark lakehouse project for analyzing European energy data.

Run local demo:

```powershell
python ALL_CODE_BASELINE.py extract-generation
python ALL_CODE_BASELINE.py full-local
```
""")


# =============================================================================
# TEST-LIKE HELPER FUNCTIONS
# =============================================================================

def test_parse_generation_xml_fixture(fixture_path: str = "tests/fixtures/entsoe_generation_sample.xml") -> None:
    """Simple parser smoke test."""
    xml_text = Path(fixture_path).read_text(encoding="utf-8")
    rows = parse_generation_xml(xml_text)
    assert isinstance(rows, list)
    assert len(rows) > 0


def test_split_valid_and_quarantine() -> None:
    """Simple quarantine smoke test requiring Spark."""
    spark = get_spark("test-quarantine")
    df = spark.createDataFrame([
        {"quantity": 10.0},
        {"quantity": None},
        {"quantity": -5.0},
    ])
    valid_df, quarantine_df = split_valid_and_quarantine(df)
    assert valid_df.count() == 1
    assert quarantine_df.count() == 2


def test_build_renewable_stability() -> None:
    """Simple Gold metric smoke test requiring Spark."""
    spark = get_spark("test-gold")
    df = spark.createDataFrame([
        {"country_code": "DE", "quantity": 10.0},
        {"country_code": "DE", "quantity": 20.0},
        {"country_code": "DE", "quantity": 30.0},
    ])
    result = build_renewable_stability(df).collect()[0].asDict()
    assert result["volatility_index"] == 20.0


# =============================================================================
# CLI ENTRYPOINT
# =============================================================================

def print_usage() -> None:
    print(
        """
EU Energy Intelligence Platform — Monolithic Baseline

Commands:
  scaffold
      Create the recommended folder/config skeleton in current directory.

  extract-generation
      Call ENTSO-E generation API and save raw XML.
      Requires ENTSOE_API_KEY in environment or .env.

  bronze
      Build local Bronze Delta table from latest raw generation XML.

  silver
      Build local Silver Delta table + quarantine from Bronze.

  gold
      Build local Gold renewable stability table from Silver.

  full-local
      Run Bronze -> Silver -> Gold with ops logging.

  demo-local
      Run list-based local demo and write JSON outputs.

Examples:
  python ALL_CODE_BASELINE.py scaffold
  python ALL_CODE_BASELINE.py extract-generation
  python ALL_CODE_BASELINE.py full-local
        """.strip()
    )


def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "help"

    if command == "help":
        print_usage()

    elif command == "scaffold":
        generate_basic_project_scaffold(".")
        print("Scaffold created.")

    elif command == "extract-generation":
        country = get_env("ENTSOE_COUNTRY", COUNTRY_EIC_CODES["NL"])
        start = get_env("ENTSOE_START", "202401010000")
        end = get_env("ENTSOE_END", "202401020000")
        path = run_generation_extract(country, start, end)
        print(f"Raw generation file written: {path}")

    elif command == "bronze":
        result = run_local_spark_bronze(country_code="NL")
        print(json.dumps(result, indent=2))

    elif command == "silver":
        result = run_local_silver_generation()
        print(json.dumps(result, indent=2))

    elif command == "gold":
        result = run_local_gold_renewable_stability()
        print(json.dumps(result, indent=2))

    elif command == "full-local":
        run_local_full_pipeline()
        print("Full local pipeline completed.")

    elif command == "demo-local":
        run_demo_local_pipeline()

    else:
        print(f"Unknown command: {command}")
        print_usage()


if __name__ == "__main__":
    main()
