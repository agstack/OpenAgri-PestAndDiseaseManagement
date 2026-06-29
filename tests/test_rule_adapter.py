from __future__ import annotations

from utils.agstack_adapter import _pestmodel_to_rules, risk_index_weather_to_wdf

from tests.fakes import (
    FakeCondition,
    FakeOperator,
    FakePestModel,
    FakeRule,
    FakeUnit,
)


def _cond(unit: str, symbol: str, value: float) -> FakeCondition:
    return FakeCondition(FakeUnit(unit), FakeOperator(symbol), value)


def _pm(*rules) -> FakePestModel:
    return FakePestModel(name="UNCINE", rules=list(rules))


class TestPestModelToRules:
    def test_basic_mapping(self):
        pm = _pm(FakeRule("high", [_cond("atmospheric_temperature", ">", 15.0)]))
        rules = _pestmodel_to_rules(pm)
        assert rules == [
            {"conditions": [{"field": "air_temperature", "op": ">", "value": 15.0}],
             "risk": "high"}
        ]

    def test_multi_condition(self):
        pm = _pm(FakeRule("moderate", [
            _cond("atmospheric_temperature", ">=", 10),
            _cond("atmospheric_relative_humidity", ">", 80),
        ]))
        conds = _pestmodel_to_rules(pm)[0]["conditions"]
        assert [c["field"] for c in conds] == ["air_temperature", "relative_humidity"]
        assert all(isinstance(c["value"], float) for c in conds)

    def test_probability_value_lowercased_and_defaulted(self):
        pm = _pm(
            FakeRule("HIGH", [_cond("precipitation", ">", 1)]),
            FakeRule(None, [_cond("precipitation", ">", 0)]),
        )
        risks = [r["risk"] for r in _pestmodel_to_rules(pm)]
        assert risks == ["high", "low"]

    def test_unmappable_unit_condition_dropped(self):
        # soil_temperature_* has no canonical package field -> condition dropped,
        # and a rule left with no conditions is dropped entirely.
        pm = _pm(FakeRule("high", [_cond("soil_temperature_10cm", ">", 5)]))
        assert _pestmodel_to_rules(pm) == []

    def test_unknown_operator_dropped(self):
        pm = _pm(FakeRule("high", [_cond("atmospheric_temperature", "~=", 15)]))
        assert _pestmodel_to_rules(pm) == []

    def test_partial_unmappable_keeps_mappable_conditions(self):
        pm = _pm(FakeRule("high", [
            _cond("atmospheric_temperature", ">", 15),
            _cond("soil_temperature_20cm", ">", 5),
        ]))
        conds = _pestmodel_to_rules(pm)[0]["conditions"]
        assert conds == [{"field": "air_temperature", "op": ">", "value": 15.0}]

    def test_no_rules(self):
        assert _pestmodel_to_rules(_pm()) == []


class TestRiskIndexWeatherToWdf:
    def _weather(self):
        return {
            "data": [
                {"timestamp": "2026-05-01T00:00:00",
                 "values": {"temperature_2m": 20.0, "relative_humidity_2m": 85.0,
                            "precipitation": 0.5}},
                {"timestamp": "2026-05-01T01:00:00",
                 "values": {"temperature_2m": 19.0, "relative_humidity_2m": 90.0,
                            "precipitation": 0.0}},
            ]
        }

    def test_maps_to_canonical_fields(self):
        wdf = risk_index_weather_to_wdf(self._weather())
        assert "air_temperature" in wdf
        assert "relative_humidity" in wdf
        assert "precipitation" in wdf

    def test_length_matches_hours(self):
        wdf = risk_index_weather_to_wdf(self._weather())
        assert len(wdf.timestamps) == 2
        assert len(wdf["air_temperature"]) == 2

    def test_unmappable_openmeteo_field_dropped(self):
        wd = {"data": [{"timestamp": "2026-05-01T00:00:00",
                        "values": {"temperature_2m": 20.0,
                                   "soil_temperature_0_to_7cm": 12.0}}]}
        wdf = risk_index_weather_to_wdf(wd)
        assert "air_temperature" in wdf
        # no canonical field exists for soil temperature
        assert "soil_temperature_0_to_7cm" not in wdf

    def test_handles_tz_aware_timestamps(self):
        wd = {"data": [{"timestamp": "2026-05-01T00:00:00+00:00",
                        "values": {"temperature_2m": 20.0}}]}
        wdf = risk_index_weather_to_wdf(wd)
        assert len(wdf.timestamps) == 1
