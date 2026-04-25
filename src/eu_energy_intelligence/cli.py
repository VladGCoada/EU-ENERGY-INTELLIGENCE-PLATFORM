from __future__ import annotations

import argparse
import json
from typing import Sequence

from eu_energy_intelligence.compliance import GdprErasurePipeline
from eu_energy_intelligence.bronze.tasks import (
    FlowsBronzeTask,
    GenerationBronzeTask,
    LoadBronzeTask,
    PricesBronzeTask,
)
from eu_energy_intelligence.gold import build_renewable_stability
from eu_energy_intelligence.gold.tasks import (
    FactPowerPricesTask,
    MartDailyMarketTask,
    MartPriceSpreadsTask,
    MartRegimeSignalsTask,
)
from eu_energy_intelligence.ingestion.weather_client import WeatherClient
from eu_energy_intelligence.orchestration.local import run_local_generation_pipeline
from eu_energy_intelligence.orchestration.production import ProductionPipelineRunner
from eu_energy_intelligence.scaffold import generate_production_scaffold
from eu_energy_intelligence.settings import PlatformConfig
from eu_energy_intelligence.silver.tasks import (
    SilverFlowsTask,
    SilverGenerationTask,
    SilverLoadTask,
    SilverPricesTask,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="eu-energy-intelligence")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_local = subparsers.add_parser("run-local-generation")
    run_local.add_argument("--raw-file", required=True)
    run_local.add_argument("--country-code", default="DE")
    run_local.add_argument("--processed-base-dir", default="data/processed")

    gold_demo = subparsers.add_parser("gold-demo")
    gold_demo.add_argument("--country-code", default="DE")
    gold_demo.add_argument("--quantities", nargs="+", type=float, required=True)

    weather_demo = subparsers.add_parser("weather-params")
    weather_demo.add_argument("--latitude", type=float, required=True)
    weather_demo.add_argument("--longitude", type=float, required=True)
    weather_demo.add_argument("--start-date", required=True)
    weather_demo.add_argument("--end-date", required=True)

    subparsers.add_parser("scaffold-prod")
    subparsers.add_parser("run-pipeline")
    subparsers.add_parser("run-bronze")
    subparsers.add_parser("run-silver")
    subparsers.add_parser("run-gold")
    subparsers.add_parser("run-erasure")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "run-local-generation":
        result = run_local_generation_pipeline(
            raw_file=args.raw_file,
            country_code=args.country_code,
            processed_base_dir=args.processed_base_dir,
        )
        print(json.dumps(result, indent=2))
        return 0

    if args.command == "gold-demo":
        rows = [{"country_code": args.country_code, "quantity": quantity} for quantity in args.quantities]
        print(json.dumps(build_renewable_stability(rows), indent=2))
        return 0

    if args.command == "weather-params":
        client = WeatherClient()
        print(
            json.dumps(
                client.build_params(
                    latitude=args.latitude,
                    longitude=args.longitude,
                    start_date=args.start_date,
                    end_date=args.end_date,
                ),
                indent=2,
            )
        )
        return 0

    if args.command == "scaffold-prod":
        generate_production_scaffold(".")
        return 0

    if args.command == "run-pipeline":
        print(json.dumps(ProductionPipelineRunner(PlatformConfig()).run(), indent=2, default=str))
        return 0

    if args.command == "run-bronze":
        config = PlatformConfig()
        for task_class in [PricesBronzeTask, GenerationBronzeTask, LoadBronzeTask, FlowsBronzeTask]:
            print(f"{task_class.__name__}: {task_class(config).run()}")
        return 0

    if args.command == "run-silver":
        config = PlatformConfig()
        for task_class in [SilverPricesTask, SilverGenerationTask, SilverLoadTask, SilverFlowsTask]:
            print(f"{task_class.__name__}: {task_class(config).run()}")
        return 0

    if args.command == "run-gold":
        config = PlatformConfig()
        for task_class in [
            FactPowerPricesTask,
            MartDailyMarketTask,
            MartPriceSpreadsTask,
            MartRegimeSignalsTask,
        ]:
            print(f"{task_class.__name__}: {task_class(config).run()}")
        return 0

    if args.command == "run-erasure":
        print(
            json.dumps(
                GdprErasurePipeline(PlatformConfig()).process_pending_requests(),
                indent=2,
                default=str,
            )
        )
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
