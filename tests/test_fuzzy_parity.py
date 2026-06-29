from __future__ import annotations

import numpy as np
import pandas as pd

from core.config import settings
from utils.fuzzy_risk import _format_results, calculate_fuzzy_risk

from tests.fakes import FakeCrop, FakeParcel, FakeThreatModel


def _daily(n: int = 30) -> pd.DataFrame:
    t = np.arange(n)
    humidity = np.clip(70 + 25 * np.sin(t / 2.0 + 1), 0, 100)
    return pd.DataFrame(
        {
            "date": pd.date_range("2026-04-01", periods=n, freq="D"),
            "temp_max": 18 + 6 * np.sin(t / 3.0),
            "temp_min": 8 + 5 * np.sin(t / 3.0),
            "humidity": humidity,
            "rainfall": np.clip(5 * np.sin(t / 1.5), 0, None),
        }
    )


def _grape_downy() -> FakeThreatModel:
    return FakeThreatModel(
        scientific_name="Plasmopara viticola",
        common_name="Downy mildew",
        crop=FakeCrop("grape"),
        definition={
            "bio_params": {
                "t_base": 10.0,
                "t_lethal_min": -3.0,
                "t_lethal_max": 38.0,
                "t_optimal_min": 18.0,
                "t_optimal_max": 25.0,
                "min_streak": 3,
                "min_wetness_hours_critical": 16.0,
                "min_wetness_hours_high": 10.0,
                "pheno_frac_lo": 0.1,
                "pheno_frac_hi": 0.75,
                "pheno_frac_ref_gdd5": 2200.0,
            },
            "fuzzy_rules": [
                {
                    "hum_lo": 90,
                    "hum_hi": 100,
                    "temp_lo": 18,
                    "temp_hi": 25,
                    "rain_min": 10.0,
                    "risk_level": "critical",
                    "type": "fungal",
                },
                {
                    "hum_lo": 80,
                    "hum_hi": 100,
                    "temp_lo": 13,
                    "temp_hi": 30,
                    "rain_min": 5.0,
                    "risk_level": "high",
                    "type": "fungal",
                },
                {
                    "hum_lo": 65,
                    "hum_hi": 90,
                    "temp_lo": 10,
                    "temp_hi": 32,
                    "rain_min": 1.0,
                    "risk_level": "moderate",
                    "type": "fungal",
                },
                {
                    "hum_lo": 0,
                    "hum_hi": 65,
                    "temp_lo": 0,
                    "temp_hi": 38,
                    "rain_min": 0.0,
                    "risk_level": "low",
                    "type": "fungal",
                },
            ],
        },
    )


def _run(df, tms, *, use_agstack: bool, monkeypatch) -> pd.DataFrame:
    monkeypatch.setattr(settings, "USE_AGSTACK_PND", use_agstack)
    out = calculate_fuzzy_risk(df, tms)
    return out.sort_values("date").reset_index(drop=True)


def test_parity_report(monkeypatch):
    df = _daily()
    tm = _grape_downy()

    inline = _run(df, [tm], use_agstack=False, monkeypatch=monkeypatch)
    pkg = _run(df, [tm], use_agstack=True, monkeypatch=monkeypatch)

    # structural parity — must hold (numeric/class parity is NOT yet asserted;
    # see docs/agstack-pnd-alignment.md §8 for the measured divergences)
    assert len(inline) == len(pkg) == len(df)
    assert list(inline.columns) == list(pkg.columns)
    assert inline["risk_score"].astype(float).between(0, 100).all()
    assert pkg["risk_score"].astype(float).between(0, 100).all()

    delta = (inline["risk_score"].astype(float) - pkg["risk_score"].astype(float)).abs()
    class_agree = (inline["risk_class"] == pkg["risk_class"]).mean()

    print(
        f"\n[parity] rows={len(inline)} "
        f"max|Δscore|={delta.max():.2f} mean|Δscore|={delta.mean():.2f} "
        f"class_agree={class_agree:.0%}"
    )
    print(
        pd.DataFrame(
            {
                "date": inline["date"].dt.date,
                "inline": inline["risk_score"].round(1),
                "pkg": pkg["risk_score"].round(1),
                "in_cls": inline["risk_class"],
                "pkg_cls": pkg["risk_class"],
            }
        ).to_string(index=False)
    )


def test_phenology_gating_matches_inline(monkeypatch):
    df = _daily(60)
    tm = _grape_downy()

    inline = _run(df, [tm], use_agstack=False, monkeypatch=monkeypatch)
    pkg = _run(df, [tm], use_agstack=True, monkeypatch=monkeypatch)

    in_oos = (inline["risk_class"] == "Out of season").to_numpy()
    pkg_oos = (pkg["risk_class"] == "Out of season").to_numpy()
    assert in_oos.any(), "fixture should exercise some out-of-season days"
    assert (in_oos == pkg_oos).all()


def test_agstack_path_serializes_to_jsonld(monkeypatch):
    """Regression: the package path must emit a datetime64 'date' column so the
    OCSM serializer (_results_to_jsonld does row['date'].date()) doesn't crash."""
    df = _daily(10)
    monkeypatch.setattr(settings, "USE_AGSTACK_PND", True)
    out = calculate_fuzzy_risk(df, [_grape_downy()])

    assert out["date"].dtype.kind == "M"  # datetime64, matching the inline path
    envelope = _format_results(
        out, FakeParcel(latitude=45.1, longitude=12.3), "json-ld"
    )
    members = envelope["@graph"][0]["hasMember"]
    assert members and members[0]["phenomenonTime"]


def _windowless_fungus() -> FakeThreatModel:
    # no phenology window -> always in season; a high-humidity rule + a low fallback
    return FakeThreatModel(
        scientific_name="Test fungus",
        common_name="Test",
        crop=FakeCrop("grape"),
        definition={
            "bio_params": {"t_base": 5.0},
            "fuzzy_rules": [
                {"hum_lo": 90, "hum_hi": 100, "temp_lo": 10, "temp_hi": 30,
                 "rain_min": 0.0, "risk_level": "high", "type": "fungal"},
                {"hum_lo": 0, "hum_hi": 70, "temp_lo": 0, "temp_hi": 35,
                 "rain_min": 0.0, "risk_level": "low", "type": "fungal"},
            ],
        },
    )


def _flat_daily(n: int = 20) -> pd.DataFrame:
    return pd.DataFrame({
        "date": pd.date_range("2026-05-01", periods=n, freq="D"),
        "temp_max": np.full(n, 22.0),
        "temp_min": np.full(n, 18.0),      # avg 20, inside [10, 30]
        "humidity": np.full(n, 70.0),      # daily-mean RH -> matches the "low" rule
        "rainfall": np.zeros(n),
    })


def _hourly_with_spike(n: int = 20) -> pd.DataFrame:
    rows = []
    for d in pd.date_range("2026-05-01", periods=n, freq="D"):
        for h in range(24):
            rows.append({
                "date": d + pd.Timedelta(hours=h),
                "atmospheric_temperature": 20.0,
                "atmospheric_relative_humidity": 98.0 if h == 12 else 70.0,  # daily MAX 98
                "precipitation": 0.0,
            })
    return pd.DataFrame(rows)


def test_hourly_path_uses_daily_max_rh(monkeypatch):
    """The hourly path lets the package take the daily MAX RH (98 -> 'high' rule),
    while the daily bridge passes the mean RH (70 -> 'low' rule) -> hourly scores
    strictly higher. Proves the accuracy mechanism without phenology in the way."""
    monkeypatch.setattr(settings, "USE_AGSTACK_PND", True)
    tm, daily, hourly = _windowless_fungus(), _flat_daily(), _hourly_with_spike()

    daily_out = calculate_fuzzy_risk(daily, [tm])
    hourly_out = calculate_fuzzy_risk(daily, [tm], hourly_df=hourly)

    assert hourly_out["risk_score"].astype(float).between(0, 100).all()
    assert hourly_out["risk_score"].max() > daily_out["risk_score"].max()


def test_hourly_none_matches_daily_path(monkeypatch):
    """hourly_df=None must be byte-identical to the daily path (no regression)."""
    monkeypatch.setattr(settings, "USE_AGSTACK_PND", True)
    tm, daily = _grape_downy(), _daily(40)
    a = calculate_fuzzy_risk(daily, [tm]).sort_values("date").reset_index(drop=True)
    b = calculate_fuzzy_risk(daily, [tm], hourly_df=None).sort_values("date").reset_index(drop=True)
    assert a["risk_score"].tolist() == b["risk_score"].tolist()
    assert a["risk_class"].tolist() == b["risk_class"].tolist()
