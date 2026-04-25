from eu_energy_intelligence.intelligence import AnomalyScorer, RollingWindowForecaster
from eu_energy_intelligence.streaming import LocalStreamBuffer


def test_local_stream_buffer_batches_by_topic() -> None:
    buffer = LocalStreamBuffer()
    buffer.publish("prices", "NL", {"price_eur_mwh": 85.0})
    buffer.publish("generation", "NL", {"quantity": 100.0})

    price_events = buffer.drain("prices")
    remaining = buffer.drain()

    assert len(price_events) == 1
    assert price_events[0].key == "NL"
    assert len(remaining) == 1
    assert remaining[0].topic == "generation"


def test_rolling_window_forecaster_emits_forecast_fields() -> None:
    forecaster = RollingWindowForecaster(window=2)

    rows = forecaster.forecast(
        [
            {"price_eur_mwh": 10.0},
            {"price_eur_mwh": 20.0},
            {"price_eur_mwh": 40.0},
        ],
        "price_eur_mwh",
    )

    assert rows[0]["forecast_value"] == 10.0
    assert rows[2]["forecast_value"] == 15.0
    assert rows[2]["forecast_error"] == 25.0


def test_anomaly_scorer_adds_flags() -> None:
    scorer = AnomalyScorer(contamination=0.25, random_state=7)

    rows = scorer.score(
        [
            {"price_eur_mwh": 50.0, "load_mw": 1000.0},
            {"price_eur_mwh": 52.0, "load_mw": 1005.0},
            {"price_eur_mwh": 350.0, "load_mw": 1900.0},
            {"price_eur_mwh": 49.0, "load_mw": 995.0},
        ],
        ["price_eur_mwh", "load_mw"],
    )

    assert len(rows) == 4
    assert all("anomaly_score" in row for row in rows)
    assert any(row["is_anomaly"] for row in rows)
