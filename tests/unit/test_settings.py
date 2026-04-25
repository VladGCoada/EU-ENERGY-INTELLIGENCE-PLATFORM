from pathlib import Path

from eu_energy_intelligence.settings import load_config, resolve_config_path


def test_resolve_config_path_for_named_environment() -> None:
    config_path = resolve_config_path("dev")
    assert config_path.name == "dev.yml"
    assert config_path.is_absolute()


def test_load_config_reads_yaml_mapping() -> None:
    config = load_config("dev")
    assert config["env"] == "dev"
    assert config["schemas"]["bronze"] == "bronze"


def test_load_config_falls_back_when_config_is_missing() -> None:
    config = load_config("missing-env")
    assert config["env"] == "missing-env"
    assert config["processed_data_dir"] == "./data/processed"


def test_resolve_config_path_accepts_relative_yaml_path() -> None:
    config_path = resolve_config_path("conf/dev.yml")
    assert config_path == Path.cwd() / "conf" / "dev.yml"
