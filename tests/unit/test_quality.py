import pytest

from eu_energy_intelligence.quality import (
    expect_non_negative,
    expect_not_null,
    load_contract,
    validate_contract_columns,
    validate_contract_rows,
)


def test_expect_non_negative_splits_failed_rows() -> None:
    passed, failed = expect_non_negative(
        [{"quantity": 1.0}, {"quantity": -1.0}, {"quantity": None}],
        "quantity",
    )

    assert passed == [{"quantity": 1.0}]
    assert failed == [{"quantity": -1.0}, {"quantity": None}]


def test_expect_not_null_splits_failed_rows() -> None:
    passed, failed = expect_not_null(
        [{"country_code": "DE"}, {"country_code": None}],
        "country_code",
    )

    assert passed == [{"country_code": "DE"}]
    assert failed == [{"country_code": None}]


def test_validate_contract_columns_raises_for_missing_columns() -> None:
    contract = load_contract("conf/data_contracts/gold_renewable_stability.yaml")

    with pytest.raises(ValueError):
        validate_contract_columns([{"country_code": "DE"}], contract)


def test_validate_contract_rows_raises_for_non_nullable_field() -> None:
    contract = load_contract("conf/data_contracts/gold_renewable_stability.yaml")

    with pytest.raises(ValueError, match="must not be null"):
        validate_contract_rows(
            [
                {
                    "country_code": None,
                    "total_generation": 30.0,
                    "avg_generation": 15.0,
                    "max_generation": 20.0,
                    "min_generation": 10.0,
                    "volatility_index": 10.0,
                }
            ],
            contract,
        )


def test_validate_contract_rows_raises_for_type_mismatch() -> None:
    contract = load_contract("conf/data_contracts/gold_renewable_stability.yaml")

    with pytest.raises(ValueError, match="expected type double"):
        validate_contract_rows(
            [
                {
                    "country_code": "DE",
                    "total_generation": "30.0",
                    "avg_generation": 15.0,
                    "max_generation": 20.0,
                    "min_generation": 10.0,
                    "volatility_index": 10.0,
                }
            ],
            contract,
        )
