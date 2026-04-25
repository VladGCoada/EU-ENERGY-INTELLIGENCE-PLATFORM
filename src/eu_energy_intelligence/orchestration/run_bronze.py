from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta

from eu_energy_intelligence.bronze.generation import build_bronze_generation
from eu_energy_intelligence.constants import COUNTRY_EIC_CODES
from eu_energy_intelligence.orchestration.local import (
    run_live_local_generation_pipeline,
    run_local_generation_pipeline,
)
from eu_energy_intelligence.settings import get_env, load_config


def build_bronze_run_plan(raw_file: str, country_code: str | None = None) -> dict[str, object]:
    """Build a simple Bronze execution plan from a raw file."""
    config = load_config(get_env("APP_ENV", "dev"))
    rows = build_bronze_generation(raw_file, country_code=country_code)
    return {
        "env": config["env"],
        "raw_file": raw_file,
        "row_count": len(rows),
        "country_code": country_code,
    }


def _default_period_range() -> tuple[str, str]:
    yesterday = datetime.now(UTC).date() - timedelta(days=1)
    today = yesterday + timedelta(days=1)
    return yesterday.strftime("%Y%m%d0000"), today.strftime("%Y%m%d0000")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a local Bronze or full generation pipeline from raw or live ENTSO-E data."
    )
    parser.add_argument("--raw-file", help="Path to the raw XML file.")
    parser.add_argument("--country-code", default="NL", help="Country code tag, e.g. NL or DE.")
    parser.add_argument(
        "--live",
        action="store_true",
        help="Fetch live ENTSO-E generation data before running the local pipeline.",
    )
    parser.add_argument(
        "--period-start",
        default=None,
        help="ENTSO-E period start in YYYYMMDDHHMM format.",
    )
    parser.add_argument(
        "--period-end",
        default=None,
        help="ENTSO-E period end in YYYYMMDDHHMM format.",
    )
    parser.add_argument(
        "--processed-base-dir",
        default=None,
        help="Override the processed data output directory.",
    )
    args = parser.parse_args()

    if args.live:
        period_start, period_end = (
            (args.period_start, args.period_end)
            if args.period_start and args.period_end
            else _default_period_range()
        )
        result = run_live_local_generation_pipeline(
            country_code=args.country_code,
            period_start=period_start,
            period_end=period_end,
            processed_base_dir=args.processed_base_dir,
        )
    else:
        if not args.raw_file:
            parser.error("--raw-file is required unless --live is used.")
        processed_base_dir = args.processed_base_dir or load_config(get_env("APP_ENV", "dev"))[
            "processed_data_dir"
        ]
        result = run_local_generation_pipeline(
            raw_file=args.raw_file,
            country_code=args.country_code,
            processed_base_dir=processed_base_dir,
        )

    result["country_eic_code"] = COUNTRY_EIC_CODES.get(args.country_code.upper().replace("-", ""))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
