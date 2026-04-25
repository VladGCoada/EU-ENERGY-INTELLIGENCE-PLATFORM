from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_expected_project_directories_exist() -> None:
    expected_directories = [
        PROJECT_ROOT / "src" / "eu_energy_intelligence",
        PROJECT_ROOT / "src" / "eu_energy_intelligence" / "ingestion",
        PROJECT_ROOT / "src" / "eu_energy_intelligence" / "bronze",
        PROJECT_ROOT / "src" / "eu_energy_intelligence" / "silver",
        PROJECT_ROOT / "src" / "eu_energy_intelligence" / "gold",
        PROJECT_ROOT / "src" / "eu_energy_intelligence" / "platinum",
        PROJECT_ROOT / "src" / "eu_energy_intelligence" / "quality",
        PROJECT_ROOT / "src" / "eu_energy_intelligence" / "observability",
        PROJECT_ROOT / "src" / "eu_energy_intelligence" / "compliance",
        PROJECT_ROOT / "src" / "eu_energy_intelligence" / "intelligence",
        PROJECT_ROOT / "src" / "eu_energy_intelligence" / "streaming",
        PROJECT_ROOT / "src" / "eu_energy_intelligence" / "features",
        PROJECT_ROOT / "src" / "eu_energy_intelligence" / "ml",
        PROJECT_ROOT / "src" / "eu_energy_intelligence" / "orchestration",
        PROJECT_ROOT / "src" / "eu_energy_intelligence" / "tasks",
        PROJECT_ROOT / "src" / "eu_energy_intelligence" / "utils",
        PROJECT_ROOT / "tests" / "unit",
        PROJECT_ROOT / "tests" / "integration",
        PROJECT_ROOT / "conf",
        PROJECT_ROOT / "docs",
        PROJECT_ROOT / "notebooks",
        PROJECT_ROOT / "infra",
        PROJECT_ROOT / "databricks",
    ]

    for directory in expected_directories:
        assert directory.is_dir(), f"Missing expected directory: {directory}"


def test_expected_scaffold_files_exist() -> None:
    expected_files = [
        PROJECT_ROOT / "pyproject.toml",
        PROJECT_ROOT / "requirements.txt",
        PROJECT_ROOT / ".env.example",
        PROJECT_ROOT / ".gitignore",
        PROJECT_ROOT / "LICENSE",
        PROJECT_ROOT / "Makefile",
        PROJECT_ROOT / ".pre-commit-config.yaml",
        PROJECT_ROOT / "README.md",
        PROJECT_ROOT / "docs" / "architecture.md",
        PROJECT_ROOT / "docs" / "data_model.md",
        PROJECT_ROOT / "docs" / "operations_runbook.md",
        PROJECT_ROOT / "docs" / "data_dictionary.md",
        PROJECT_ROOT / "conf" / "base.yml",
        PROJECT_ROOT / "conf" / "test.yml",
        PROJECT_ROOT / "conf" / "logging.yml",
        PROJECT_ROOT / "conf" / "jobs.yml",
        PROJECT_ROOT / "conf" / "staging.yml",
        PROJECT_ROOT / "conf" / "prod.yml",
        PROJECT_ROOT / "conf" / "data_contracts" / "bronze_generation_load.yaml",
        PROJECT_ROOT / "conf" / "data_contracts" / "silver_energy_timeseries.yaml",
        PROJECT_ROOT / "conf" / "data_contracts" / "gold_price_spike_analysis.yaml",
        PROJECT_ROOT / "conf" / "data_contracts" / "gold_renewable_stability.yaml",
        PROJECT_ROOT / "conf" / "data_contracts" / "gold_import_dependency.yaml",
        PROJECT_ROOT / "databricks.yml",
        PROJECT_ROOT / "databricks" / "databricks.yml",
        PROJECT_ROOT / "databricks" / "resources" / "jobs.yml",
        PROJECT_ROOT / "databricks" / "resources" / "pipelines.yml",
        PROJECT_ROOT / "databricks" / "resources" / "dashboards.yml",
        PROJECT_ROOT / "databricks" / "resources" / "clusters.yml",
        PROJECT_ROOT / "databricks" / "targets" / "dev.yml",
        PROJECT_ROOT / "databricks" / "targets" / "test.yml",
        PROJECT_ROOT / "databricks" / "targets" / "prod.yml",
        PROJECT_ROOT / "infra" / "terraform" / "main.tf",
        PROJECT_ROOT / ".github" / "workflows" / "ci.yml",
        PROJECT_ROOT / ".github" / "workflows" / "deploy-dev.yml",
        PROJECT_ROOT / ".github" / "workflows" / "deploy-prod.yml",
        PROJECT_ROOT / ".github" / "workflows" / "run-tests.yml",
    ]

    for file_path in expected_files:
        assert file_path.is_file(), f"Missing expected file: {file_path}"
