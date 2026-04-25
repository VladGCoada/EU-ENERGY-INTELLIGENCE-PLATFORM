from eu_energy_intelligence.quality.contracts import load_contract


def test_target_data_contract_files_are_loadable() -> None:
    contract_names = [
        "bronze_generation_load",
        "silver_energy_timeseries",
        "gold_price_spike_analysis",
        "gold_renewable_stability",
        "gold_import_dependency",
    ]

    for contract_name in contract_names:
        contract = load_contract(f"conf/data_contracts/{contract_name}.yaml")
        assert contract["name"] == contract_name
