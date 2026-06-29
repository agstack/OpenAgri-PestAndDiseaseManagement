from __future__ import annotations

import inspect

from core.config import settings
from utils.risk_index import (
    _calculate_risk_index_probability_wd_agstack,
    calculate_risk_index_probability_wd,
)

from tests.fakes import (
    FakeCondition,
    FakeOperator,
    FakePestModel,
    FakeRule,
    FakeUnit,
)

PARCEL = {"@id": "urn:openagri:parcel:test"}


def _cond(unit: str, symbol: str, value: float) -> FakeCondition:
    return FakeCondition(FakeUnit(unit), FakeOperator(symbol), value)


def _pm() -> FakePestModel:
    # temp > 15 AND humidity > 80 -> high
    return FakePestModel(
        name="UNCINE",
        rules=[FakeRule("high", [
            _cond("atmospheric_temperature", ">", 15.0),
            _cond("atmospheric_relative_humidity", ">", 80.0),
        ])],
    )


def _constant_day(values: dict, hours: int = 6) -> dict:
    """One calendar day, constant weather each hour -> every hour yields the same
    level, so the per-hour inline path and the per-day-max agstack path agree."""
    return {
        "data": [
            {"timestamp": f"2026-05-01T{h:02d}:00:00", "values": dict(values)}
            for h in range(hours)
        ]
    }


def _run(weather, *, use_agstack: bool, monkeypatch, parameter=None):
    monkeypatch.setattr(settings, "USE_AGSTACK_PND", use_agstack)
    return calculate_risk_index_probability_wd(
        PARCEL, [_pm()], weather, lat=45.1, lon=12.3, parameter=parameter
    )


def _levels(doc):
    return [m["hasSimpleResult"] for m in doc["@graph"][0]["hasMember"]]


def test_structural_parity(monkeypatch):
    wd = _constant_day({"temperature_2m": 20.0, "relative_humidity_2m": 85.0})
    inline = _run(wd, use_agstack=False, monkeypatch=monkeypatch)
    pkg = _run(wd, use_agstack=True, monkeypatch=monkeypatch)

    assert len(inline["@graph"]) == len(pkg["@graph"]) == 1
    assert len(pkg["@graph"][0]["hasMember"]) == len(wd["data"])
    obs = pkg["@graph"][0]["hasMember"][0]
    assert set(obs) == {"@id", "@type", "phenomenonTime", "hasSimpleResult"}
    assert [m["phenomenonTime"] for m in inline["@graph"][0]["hasMember"]] == \
           [m["phenomenonTime"] for m in pkg["@graph"][0]["hasMember"]]


def test_levels_match_on_constant_high_day(monkeypatch):
    wd = _constant_day({"temperature_2m": 20.0, "relative_humidity_2m": 85.0})
    inline = _levels(_run(wd, use_agstack=False, monkeypatch=monkeypatch))
    pkg = _levels(_run(wd, use_agstack=True, monkeypatch=monkeypatch))
    assert inline == pkg
    assert set(pkg) == {"high"}


def test_levels_match_on_constant_low_day(monkeypatch):
    # below the rule thresholds -> default "low" both ways
    wd = _constant_day({"temperature_2m": 5.0, "relative_humidity_2m": 50.0})
    inline = _levels(_run(wd, use_agstack=False, monkeypatch=monkeypatch))
    pkg = _levels(_run(wd, use_agstack=True, monkeypatch=monkeypatch))
    assert inline == pkg
    assert set(pkg) == {"low"}


def test_parameter_high_filter_parity(monkeypatch):
    high = _constant_day({"temperature_2m": 20.0, "relative_humidity_2m": 85.0})
    low = _constant_day({"temperature_2m": 5.0, "relative_humidity_2m": 50.0})

    # high day, parameter="high": both keep all hours
    i_hi = _run(high, use_agstack=False, monkeypatch=monkeypatch, parameter="high")
    p_hi = _run(high, use_agstack=True, monkeypatch=monkeypatch, parameter="high")
    assert len(i_hi["@graph"][0]["hasMember"]) == len(p_hi["@graph"][0]["hasMember"]) \
           == len(high["data"])

    # low day, parameter="high": both drop all hours
    i_lo = _run(low, use_agstack=False, monkeypatch=monkeypatch, parameter="high")
    p_lo = _run(low, use_agstack=True, monkeypatch=monkeypatch, parameter="high")
    assert i_lo["@graph"][0]["hasMember"] == p_lo["@graph"][0]["hasMember"] == []


def test_eval_absent_from_agstack_path():
    src = inspect.getsource(_calculate_risk_index_probability_wd_agstack)
    assert "eval(" not in src
