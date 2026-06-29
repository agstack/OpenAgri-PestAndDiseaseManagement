from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time


@dataclass
class FakeCrop:
    name: str


@dataclass
class FakeThreatModel:
    scientific_name: str
    common_name: str
    crop: FakeCrop
    definition: dict
    threat_type: str | None = None


@dataclass
class FakeDataRow:
    date: date
    time: time
    atmospheric_temperature: float | None = None
    atmospheric_relative_humidity: float | None = None
    precipitation: float | None = None
    average_wind_speed: float | None = None
    atmospheric_pressure: float | None = None


@dataclass
class FakeParcel:
    latitude: float
    longitude: float


@dataclass
class FakeUnit:
    name: str


@dataclass
class FakeOperator:
    symbol: str


@dataclass
class FakeCondition:
    unit: FakeUnit
    operator: FakeOperator
    value: float


@dataclass
class FakeRule:
    probability_value: str
    conditions: list
    from_time: time | None = None
    to_time: time | None = None


@dataclass
class FakePestModel:
    name: str
    rules: list
