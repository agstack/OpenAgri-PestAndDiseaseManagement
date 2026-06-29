"""
Unit tests for app/utils/fuzzy_risk.py and app/utils/fuzzy_config.py.
Uses other/weather_data.xlsx as sample input.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pytest

from utils.fuzzy_risk import (
    compute_features,
    calculate_fuzzy_risk,
    membership_humidity,
    membership_temp,
    membership_rainfall,
    membership_phenology,
    score_day,
    _trapezoid,
    _definition_to_rules,
)

WEATHER_XLSX = Path("other/weather_data.xlsx")


# ─── fixtures ────────────────────────────────────────────────────────────────

@dataclass
class FakeThreatModel:
    scientific_name: str
    common_name: str
    definition: dict


@pytest.fixture(scope="module")
def raw_weather() -> pd.DataFrame:
    df = pd.read_excel(WEATHER_XLSX)
    df["date"] = pd.to_datetime(df["date"])
    return df


@pytest.fixture(scope="module")
def enriched_weather(raw_weather: pd.DataFrame) -> pd.DataFrame:
    return compute_features(raw_weather)


@pytest.fixture
def fungal_threat_model() -> FakeThreatModel:
    return FakeThreatModel(
        scientific_name="Venturia inaequalis",
        common_name="Apple scab",
        definition={
            "bio_params": {
                "t_base": 5.0,
                "t_lethal_min": -5.0,
                "t_lethal_max": 40.0,
                "t_optimal_min": 15.0,
                "t_optimal_max": 25.0,
                "min_streak": 2,
                "pheno_frac_lo": 0.10,
                "pheno_frac_hi": 0.80,
                "min_wetness_hours_critical": 12.0,
                "min_wetness_hours_high": 6.0,
            },
            "fuzzy_rules": [
                {
                    "hum_lo": 75.0, "hum_hi": 100.0,
                    "temp_lo": 5.0,  "temp_hi": 25.0,
                    "rain_min": 2.0,
                    "risk_level": "high",
                    "type": "fungal",
                },
                {
                    "hum_lo": 60.0, "hum_hi": 100.0,
                    "temp_lo": 5.0,  "temp_hi": 30.0,
                    "rain_min": 0.5,
                    "risk_level": "moderate",
                    "type": "fungal",
                },
            ],
        },
    )


@pytest.fixture
def insect_threat_model() -> FakeThreatModel:
    return FakeThreatModel(
        scientific_name="Cydia pomonella",
        common_name="Codling moth",
        definition={
            "bio_params": {
                "t_base": 10.0,
                "t_lethal_min": -10.0,
                "t_lethal_max": 38.0,
                "min_streak": 1,
                "pheno_frac_lo": 0.10,
                "pheno_frac_hi": 0.80,
            },
            "fuzzy_rules": [
                {
                    "hum_lo": 0.0,  "hum_hi": 100.0,
                    "temp_lo": 15.0, "temp_hi": 30.0,
                    "rain_min": 0.0,
                    "risk_level": "high",
                    "type": "insect",
                },
            ],
        },
    )


# ─── _trapezoid ──────────────────────────────────────────────────────────────

class TestTrapezoid:
    def test_below_range(self):
        assert _trapezoid(0.0, 1.0, 2.0, 3.0, 4.0) == 0.0

    def test_above_range(self):
        assert _trapezoid(5.0, 1.0, 2.0, 3.0, 4.0) == 0.0

    def test_plateau(self):
        assert _trapezoid(2.5, 1.0, 2.0, 3.0, 4.0) == 1.0

    def test_left_ramp(self):
        v = _trapezoid(1.5, 1.0, 2.0, 3.0, 4.0)
        assert 0.0 < v < 1.0

    def test_right_ramp(self):
        v = _trapezoid(3.5, 1.0, 2.0, 3.0, 4.0)
        assert 0.0 < v < 1.0


# ─── membership functions ─────────────────────────────────────────────────────

class TestMembershipHumidity:
    def test_full_range_always_one(self):
        assert membership_humidity(50.0, 0.0, 100.0) == 1.0

    def test_inside_range(self):
        assert membership_humidity(85.0, 75.0, 100.0) == 1.0

    def test_below_range(self):
        assert membership_humidity(10.0, 75.0, 100.0) == 0.0

    def test_transition_zone(self):
        v = membership_humidity(74.0, 75.0, 100.0)
        assert 0.0 < v < 1.0


class TestMembershipTemp:
    def test_whatever_range(self):
        assert membership_temp(20.0, -999.0, 999.0) == 1.0

    def test_inside_range(self):
        assert membership_temp(15.0, 10.0, 20.0) == 1.0

    def test_below_lethal(self):
        assert membership_temp(-20.0, 5.0, 25.0) == 0.0

    def test_above_lethal(self):
        assert membership_temp(50.0, 5.0, 25.0) == 0.0

    def test_transition(self):
        v = membership_temp(9.0, 10.0, 20.0)
        assert 0.0 < v < 1.0


class TestMembershipRainfall:
    def test_zero_threshold_always_one(self):
        assert membership_rainfall(0.0, 0.0) == 1.0
        assert membership_rainfall(10.0, 0.0) == 1.0

    def test_above_threshold(self):
        assert membership_rainfall(5.0, 2.0) == 1.0

    def test_below_threshold(self):
        assert membership_rainfall(0.1, 2.0) == 0.0

    def test_transition(self):
        v = membership_rainfall(1.5, 2.0)
        assert 0.0 < v < 1.0


class TestMembershipPhenology:
    def test_inside_window(self):
        assert membership_phenology(500.0, 200.0, 800.0) == 1.0

    def test_outside_window_low(self):
        assert membership_phenology(10.0, 200.0, 800.0) == 0.0

    def test_outside_window_high(self):
        assert membership_phenology(1200.0, 200.0, 800.0) == 0.0

    def test_soft_entry(self):
        v = membership_phenology(190.0, 200.0, 800.0)
        assert 0.0 < v < 1.0

    def test_soft_exit(self):
        v = membership_phenology(820.0, 200.0, 800.0)
        assert 0.0 < v < 1.0


# ─── _definition_to_rules ────────────────────────────────────────────────────

class TestDefinitionToRules:
    def test_risk_level_mapping(self):
        rules = _definition_to_rules({
            "fuzzy_rules": [
                {"hum_lo": 70.0, "hum_hi": 100.0,
                 "temp_lo": 5.0, "temp_hi": 25.0,
                 "rain_min": 1.0, "risk_level": "high"},
                {"hum_lo": 0.0, "hum_hi": 100.0,
                 "temp_lo": -999.0, "temp_hi": 999.0,
                 "rain_min": 0.0, "risk_level": "critical"},
            ]
        })
        assert len(rules) == 2
        assert rules[0]["risk_score"] == 80
        assert rules[0]["risk"] == "High"
        assert rules[1]["risk_score"] == 100

    def test_empty_rules(self):
        assert _definition_to_rules({}) == []
        assert _definition_to_rules({"fuzzy_rules": []}) == []

    def test_null_numeric_rule_fields_use_defaults(self):
        # keys present but null → must use defaults, not crash with float(None)
        rules = _definition_to_rules({
            "fuzzy_rules": [{
                "hum_lo": None, "hum_hi": None,
                "temp_lo": None, "temp_hi": None,
                "rain_min": None, "risk_level": "low",
            }]
        })
        assert len(rules) == 1
        assert rules[0]["hum_lo"] == 0.0
        assert rules[0]["hum_hi"] == 100.0
        assert rules[0]["temp_lo"] == -999.0
        assert rules[0]["temp_hi"] == 999.0
        assert rules[0]["rain_min"] == 0.0

    def test_zero_temp_lo_not_replaced_by_default(self):
        # 0.0 is a valid temp_lo (frost threshold); must not be treated as falsy
        rules = _definition_to_rules({
            "fuzzy_rules": [{
                "hum_lo": 0.0, "hum_hi": 100.0,
                "temp_lo": 0.0, "temp_hi": 10.0,
                "rain_min": 0.0, "risk_level": "moderate",
            }]
        })
        assert rules[0]["temp_lo"] == 0.0

    def test_null_risk_level_falls_back_to_low(self):
        # null risk_level must not produce "none" label silently
        rules = _definition_to_rules({
            "fuzzy_rules": [{
                "hum_lo": 0.0, "hum_hi": 100.0,
                "temp_lo": -999.0, "temp_hi": 999.0,
                "rain_min": 0.0, "risk_level": None,
            }]
        })
        assert rules[0]["risk"] == "Low"
        assert rules[0]["risk_score"] == 10


# ─── compute_features ────────────────────────────────────────────────────────

class TestComputeFeatures:
    def test_output_has_required_columns(self, enriched_weather: pd.DataFrame):
        required = [
            "temp_avg", "temp_avg_7d", "humidity_7d",
            "rain_3d", "wetness_h", "wetness_3d",
            "gdd_cum_5b", "gdd_cum_0b", "gdd_cum_10b",
            "gdd_annual_ref",
            "streak_hum70", "streak_hum60",
            "vpd",
        ]
        for col in required:
            assert col in enriched_weather.columns, f"Missing column: {col}"

    def test_temp_avg_is_midpoint(self, enriched_weather: pd.DataFrame):
        expected = (enriched_weather["temp_max"] + enriched_weather["temp_min"]) / 2.0
        pd.testing.assert_series_equal(
            enriched_weather["temp_avg"].reset_index(drop=True),
            expected.reset_index(drop=True),
            check_names=False,
        )

    def test_gdd_cumulative_non_decreasing_within_season(self, enriched_weather: pd.DataFrame):
        for season, grp in enriched_weather.groupby("season_year"):
            diffs = grp["gdd_cum_5b"].diff().dropna()
            assert (diffs >= -1e-9).all(), f"GDD5 decreased in season {season}"

    def test_streak_hum70_non_negative(self, enriched_weather: pd.DataFrame):
        assert (enriched_weather["streak_hum70"] >= 0).all()

    def test_wetness_h_capped_at_24(self, enriched_weather: pd.DataFrame):
        assert (enriched_weather["wetness_h"] <= 24.0).all()

    def test_vpd_non_negative(self, enriched_weather: pd.DataFrame):
        assert (enriched_weather["vpd"] >= 0.0).all()

    def test_extra_t_bases_added(self, raw_weather: pd.DataFrame):
        df = compute_features(raw_weather, extra_t_bases={8.0, 15.0})
        assert "gdd_cum_8b" in df.columns
        assert "gdd_cum_15b" in df.columns

    def test_no_rows_dropped(self, raw_weather: pd.DataFrame, enriched_weather: pd.DataFrame):
        assert len(enriched_weather) == len(raw_weather)


# ─── score_day ───────────────────────────────────────────────────────────────

class TestScoreDay:
    def _make_row(self, **overrides) -> pd.Series:
        base = {
            "temp_avg": 18.0, "temp_max": 22.0, "temp_min": 14.0,
            "humidity": 85.0, "rainfall": 3.0,
            "temp_avg_7d": 17.0, "humidity_7d": 83.0, "rain_3d": 5.0,
            "wetness_h": 14.0,
            "gdd_cum_5b": 400.0, "gdd_annual_ref_5b": 1000.0,
            "gdd_cum_10b": 200.0, "gdd_annual_ref_10b": 800.0,
            "gdd_cum_pheno": 400.0, "gdd_annual_ref": 1000.0,
            "streak_hum60": 3, "streak_hum70": 3,
        }
        base.update(overrides)
        return pd.Series(base)

    def _rules(self) -> list[dict]:
        return [{
            "hum_lo": 75.0, "hum_hi": 100.0,
            "temp_lo": 5.0, "temp_hi": 25.0,
            "rain_min": 2.0,
            "risk": "High", "risk_score": 80,
        }]

    def _params(self) -> dict:
        return {
            "t_base": 5.0, "min_streak": 1,
            "pheno_frac_lo": 0.10, "pheno_frac_hi": 0.80,
        }

    def test_favorable_conditions_give_nonzero_score(self):
        row = self._make_row()
        result = score_day(row, self._rules(), "Venturia inaequalis", self._params())
        assert result["score"] > 0.0

    def test_lethal_max_suppresses_score(self):
        row = self._make_row(temp_avg=45.0)
        params = {**self._params(), "t_lethal_max": 40.0}
        result = score_day(row, self._rules(), "Venturia inaequalis", params)
        assert result["score"] == 0.0
        assert result["risk_class"] == "Low"

    def test_lethal_min_suppresses_score(self):
        row = self._make_row(temp_avg=-10.0)
        params = {**self._params(), "t_lethal_min": -5.0}
        result = score_day(row, self._rules(), "Venturia inaequalis", params)
        assert result["score"] == 0.0

    def test_no_active_rules_returns_low(self):
        # pathogens use 7d MA — must zero out humidity_7d and rain_3d too
        row = self._make_row(humidity=10.0, humidity_7d=10.0, rainfall=0.0, rain_3d=0.0)
        result = score_day(row, self._rules(), "Venturia inaequalis", self._params())
        assert result["risk_class"] == "Low"
        assert result["score"] == 0.0

    def test_score_capped_at_100(self):
        rules = [{"hum_lo": 0.0, "hum_hi": 100.0, "temp_lo": -999.0, "temp_hi": 999.0,
                  "rain_min": 0.0, "risk": "Critical", "risk_score": 100}]
        row = self._make_row()
        params = {"t_base": 5.0, "min_streak": 1}
        result = score_day(row, rules, "SomeInsect", params)
        assert result["score"] <= 100.0

    def test_risk_class_thresholds(self):
        def score_for(rules):
            row = self._make_row()
            return score_day(row, rules, "SomeInsect",
                             {"t_base": 5.0, "min_streak": 1})

        high_rules = [{"hum_lo": 0.0, "hum_hi": 100.0, "temp_lo": -999.0,
                       "temp_hi": 999.0, "rain_min": 0.0,
                       "risk": "High", "risk_score": 80}]
        result = score_for(high_rules)
        assert result["risk_class"] in ("Low", "Moderate", "High", "Critical", "Out of season")


# ─── calculate_fuzzy_risk (end-to-end) ───────────────────────────────────────

class TestCalculateFuzzyRisk:
    def test_returns_dataframe(
        self, raw_weather: pd.DataFrame, fungal_threat_model: FakeThreatModel
    ):
        result = calculate_fuzzy_risk(raw_weather, [fungal_threat_model])
        assert isinstance(result, pd.DataFrame)

    def test_output_columns(
        self, raw_weather: pd.DataFrame, fungal_threat_model: FakeThreatModel
    ):
        result = calculate_fuzzy_risk(raw_weather, [fungal_threat_model])
        for col in ["date", "scientific_name", "common_name", "risk_score", "risk_class", "detail"]:
            assert col in result.columns, f"Missing column: {col}"

    def test_one_row_per_day_per_model(
        self, raw_weather: pd.DataFrame,
        fungal_threat_model: FakeThreatModel,
        insect_threat_model: FakeThreatModel,
    ):
        result = calculate_fuzzy_risk(raw_weather, [fungal_threat_model, insect_threat_model])
        assert len(result) == len(raw_weather) * 2

    def test_scores_in_valid_range(
        self, raw_weather: pd.DataFrame, fungal_threat_model: FakeThreatModel
    ):
        result = calculate_fuzzy_risk(raw_weather, [fungal_threat_model])
        assert (result["risk_score"] >= 0.0).all()
        assert (result["risk_score"] <= 100.0).all()

    def test_risk_class_valid_values(
        self, raw_weather: pd.DataFrame, fungal_threat_model: FakeThreatModel
    ):
        valid = {"Low", "Moderate", "High", "Critical", "Out of season"}
        result = calculate_fuzzy_risk(raw_weather, [fungal_threat_model])
        assert set(result["risk_class"].unique()).issubset(valid)

    def test_scientific_name_propagated(
        self, raw_weather: pd.DataFrame, fungal_threat_model: FakeThreatModel
    ):
        result = calculate_fuzzy_risk(raw_weather, [fungal_threat_model])
        assert (result["scientific_name"] == "Venturia inaequalis").all()

    def test_empty_threat_models_returns_empty(self, raw_weather: pd.DataFrame):
        result = calculate_fuzzy_risk(raw_weather, [])
        assert len(result) == 0

    def test_model_without_rules_is_skipped(self, raw_weather: pd.DataFrame):
        no_rules = FakeThreatModel(
            scientific_name="X", common_name="Y",
            definition={"bio_params": {}, "fuzzy_rules": []},
        )
        result = calculate_fuzzy_risk(raw_weather, [no_rules])
        assert len(result) == 0

    def test_insect_model_runs(
        self, raw_weather: pd.DataFrame, insect_threat_model: FakeThreatModel
    ):
        result = calculate_fuzzy_risk(raw_weather, [insect_threat_model])
        assert len(result) == len(raw_weather)

    def test_null_t_base_does_not_crash(self, raw_weather: pd.DataFrame):
        # bio_params.t_base explicitly null — engine must fall back to 5.0, not TypeError
        tm = FakeThreatModel(
            scientific_name="Null Base", common_name="Test",
            definition={
                "bio_params": {"t_base": None},
                "fuzzy_rules": [
                    {"hum_lo": 70.0, "hum_hi": 100.0, "temp_lo": 5.0, "temp_hi": 30.0,
                     "rain_min": 1.0, "risk_level": "moderate"},
                ],
            },
        )
        result = calculate_fuzzy_risk(raw_weather, [tm])
        assert len(result) == len(raw_weather)
        assert (result["risk_score"] >= 0.0).all()

    def test_null_bio_params_does_not_crash(self, raw_weather: pd.DataFrame):
        # bio_params key exists but value is null — engine must not AttributeError
        tm = FakeThreatModel(
            scientific_name="Null Params", common_name="Test",
            definition={
                "bio_params": None,
                "fuzzy_rules": [
                    {"hum_lo": 70.0, "hum_hi": 100.0, "temp_lo": 5.0, "temp_hi": 30.0,
                     "rain_min": 1.0, "risk_level": "moderate"},
                ],
            },
        )
        result = calculate_fuzzy_risk(raw_weather, [tm])
        assert len(result) == len(raw_weather)

    def test_all_null_bio_params_does_not_crash(self, raw_weather: pd.DataFrame):
        # all bio_params fields explicitly null (as created by colleague's threat model)
        # min_streak null was causing TypeError: '<' not supported between int and NoneType
        tm = FakeThreatModel(
            scientific_name="Arborea-arboris", common_name="arbor",
            definition={
                "bio_params": {
                    "t_base": None, "pheno_hi": None, "pheno_lo": None,
                    "min_streak": None, "t_lethal_max": None, "t_lethal_min": None,
                    "pheno_frac_hi": None, "pheno_frac_lo": None,
                    "t_optimal_max": None, "t_optimal_min": None,
                    "pheno_frac_ref_gdd5": None,
                    "min_wetness_hours_high": None, "min_wetness_hours_critical": None,
                },
                "fuzzy_rules": [
                    {"type": "ww", "hum_hi": 100, "hum_lo": 0,
                     "temp_hi": 999, "temp_lo": -999, "rain_min": 0,
                     "risk_level": "low"},
                ],
            },
        )
        result = calculate_fuzzy_risk(raw_weather, [tm])
        assert len(result) == len(raw_weather)
        assert (result["risk_score"] >= 0.0).all()
        assert (result["risk_score"] <= 100.0).all()
