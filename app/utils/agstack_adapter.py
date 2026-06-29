from __future__ import annotations

from datetime import datetime, time as _time
from typing import Any, Iterable

import numpy as np
import pandas as pd

from agstack_pnd.foundation.types import (
    ModelResult,
    ThreatDefinition,
    ThreatType,
    WeatherDataFrame,
)

from .wdutils import openmeteo_friendly_variables

FIELD_MAP: dict[str, str] = {
    "atmospheric_temperature": "air_temperature",
    "atmospheric_relative_humidity": "relative_humidity",
    "precipitation": "precipitation",
    "average_wind_speed": "wind_speed",
    "atmospheric_pressure": "atmospheric_pressure",
}

_OPENMETEO_TO_UNIT: dict[str, str] = {
    om: unit for unit, om in openmeteo_friendly_variables.items()
}

_PACKAGE_OPERATORS = frozenset({">", ">=", "<", "<=", "==", "!="})

_BIOPARAM_RENAME: dict[str, str] = {"pheno_frac_ref_gdd5": "pheno_fraction_ref_gdd5"}

_TYPE_FROM_RULE: dict[str, ThreatType] = {
    "fungal": ThreatType.FUNGUS,
    "insect": ThreatType.INSECT,
}


def _to_float(value: Any, default: float) -> float:
    return default if value is None else float(value)


def _float_array(values: Iterable[Any]) -> np.ndarray | None:
    arr = [np.nan if v is None else float(v) for v in values]
    if all(np.isnan(x) for x in arr):
        return None
    return np.array(arr, dtype=float)


def _build_wdf(
    timestamps: Any, raw_fields: dict[str, Iterable[Any]]
) -> WeatherDataFrame:
    fields = {
        name: arr
        for name, values in raw_fields.items()
        if (arr := _float_array(values)) is not None
    }
    return WeatherDataFrame(timestamps=np.asarray(timestamps), fields=fields)


def _row_timestamp(row: Any) -> np.datetime64:
    clock = getattr(row, "time", None) or _time()
    return np.datetime64(datetime.combine(row.date, clock))


def _row_columns(rows: list[Any]) -> dict[str, list[Any]]:
    columns: dict[str, list[Any]] = {canonical: [] for canonical in FIELD_MAP.values()}
    for row in rows:
        for source_attr, canonical in FIELD_MAP.items():
            columns[canonical].append(getattr(row, source_attr, None))
    return columns


def _infer_threat_type(definition: dict) -> ThreatType:
    for rule in definition.get("fuzzy_rules") or []:
        rule_type = (rule.get("type") or "").lower()
        if rule_type in _TYPE_FROM_RULE:
            return _TYPE_FROM_RULE[rule_type]
    return ThreatType.FUNGUS


def _resolve_threat_type(tm: Any, definition: dict) -> ThreatType:
    explicit = getattr(tm, "threat_type", None)
    if explicit:
        try:
            return ThreatType(str(explicit).lower())
        except ValueError:
            pass
    return _infer_threat_type(definition)


def _crop_name(tm: Any) -> str:
    crop = getattr(tm, "crop", None)
    if crop is not None and getattr(crop, "name", None):
        return crop.name
    return getattr(tm, "crop_name", "")


_GATE_NEUTRAL: dict[str, Any] = {
    "t_lethal_min": -1.0e9,
    "t_lethal_max": 1.0e9,
    "min_streak": 1,
    "min_wetness_hours_critical": 0.0,
    "min_wetness_hours_high": 0.0,
}


def _package_bio_params(raw: dict) -> dict:
    raw = raw or {}
    cleaned: dict[str, Any] = {}
    for key, value in raw.items():
        if value is None:
            continue
        cleaned[_BIOPARAM_RENAME.get(key, key)] = value

    for key, neutral in _GATE_NEUTRAL.items():
        if raw.get(key) is None:
            cleaned[key] = neutral

    has_frac = (
        raw.get("pheno_frac_lo") is not None and raw.get("pheno_frac_hi") is not None
    )
    has_abs = raw.get("pheno_lo") is not None and raw.get("pheno_hi") is not None
    if has_frac:
        cleaned.pop("pheno_lo", None)
        cleaned.pop("pheno_hi", None)
    elif has_abs:
        cleaned.pop("pheno_frac_lo", None)
        cleaned.pop("pheno_frac_hi", None)
    else:
        cleaned["pheno_lo"] = -1.0e9
        cleaned["pheno_hi"] = 1.0e9
        cleaned.pop("pheno_frac_lo", None)
        cleaned.pop("pheno_frac_hi", None)
    return cleaned


def _clean_rule(rule: dict) -> dict:
    return {
        "hum_lo": _to_float(rule.get("hum_lo"), 0.0),
        "hum_hi": _to_float(rule.get("hum_hi"), 100.0),
        "temp_lo": _to_float(rule.get("temp_lo"), -999.0),
        "temp_hi": _to_float(rule.get("temp_hi"), 999.0),
        "rain_min": _to_float(rule.get("rain_min"), 0.0),
        "risk_level": rule.get("risk_level") or "low",
    }


def daily_df_to_wdf(daily: pd.DataFrame) -> WeatherDataFrame:
    temp_avg = (daily["temp_max"].astype(float) + daily["temp_min"].astype(float)) / 2.0
    return _build_wdf(
        pd.to_datetime(daily["date"]).to_numpy(),
        {
            "air_temperature": temp_avg,
            "relative_humidity": daily["humidity"],
            "precipitation": daily["rainfall"],
        },
    )


def hourly_rows_to_wdf(rows: Iterable[Any]) -> WeatherDataFrame:
    rows = list(rows)
    timestamps = [_row_timestamp(r) for r in rows]
    return _build_wdf(timestamps, _row_columns(rows))


def hourly_df_to_wdf(hourly: pd.DataFrame) -> WeatherDataFrame:
    """Build a WeatherDataFrame from an hourly DataFrame with a datetime ``date``
    column and DB-named weather columns (atmospheric_temperature, …). One timestamp
    per reading, so the package aggregates hourly->daily itself (mean temp, MAX RH).
    """
    ts = pd.to_datetime(hourly["date"])
    if getattr(ts.dt, "tz", None) is not None:
        ts = ts.dt.tz_convert("UTC").dt.tz_localize(None)
    raw_fields = {
        canonical: hourly[source]
        for source, canonical in FIELD_MAP.items()
        if source in hourly.columns
    }
    return _build_wdf(ts.to_numpy(), raw_fields)


def _pestmodel_to_rules(pest_model: Any) -> list[dict]:
    rules: list[dict] = []
    for rule in getattr(pest_model, "rules", None) or []:
        conditions: list[dict] = []
        for cond in getattr(rule, "conditions", None) or []:
            unit_name = getattr(getattr(cond, "unit", None), "name", None)
            field = FIELD_MAP.get(unit_name) if unit_name else None
            op = getattr(getattr(cond, "operator", None), "symbol", None)
            if field is None or op not in _PACKAGE_OPERATORS:
                continue
            conditions.append({"field": field, "op": op, "value": float(cond.value)})
        if not conditions:
            continue
        rules.append(
            {
                "conditions": conditions,
                "risk": (getattr(rule, "probability_value", None) or "low").lower(),
            }
        )
    return rules


def _parse_ts(value: Any) -> np.datetime64:
    ts = pd.Timestamp(value)
    if ts.tz is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.to_datetime64()


def risk_index_weather_to_wdf(weather_data: dict) -> WeatherDataFrame:
    hours = weather_data.get("data") or []
    timestamps = [_parse_ts(h["timestamp"]) for h in hours]

    om_to_canonical: dict[str, str] = {}
    for hour in hours:
        for om_field in hour.get("values") or {}:
            unit_name = _OPENMETEO_TO_UNIT.get(om_field)
            canonical = FIELD_MAP.get(unit_name) if unit_name else None
            if canonical is not None:
                om_to_canonical[om_field] = canonical

    columns: dict[str, list[Any]] = {
        canonical: [] for canonical in om_to_canonical.values()
    }
    for hour in hours:
        values = hour.get("values") or {}
        for om_field, canonical in om_to_canonical.items():
            columns[canonical].append(values.get(om_field))

    return _build_wdf(timestamps, columns)


def threatmodel_to_definition(tm: Any) -> ThreatDefinition:
    definition = tm.definition or {}
    return ThreatDefinition.model_validate(
        {
            "scientific_name": tm.scientific_name,
            "common_name": tm.common_name,
            "crop": _crop_name(tm),
            "threat_type": _resolve_threat_type(tm, definition),
            "bio_params": _package_bio_params(definition.get("bio_params") or {}),
            "fuzzy_rules": [
                _clean_rule(r) for r in (definition.get("fuzzy_rules") or [])
            ],
        }
    )


def result_to_rows(result: ModelResult, tm: Any) -> list[dict]:
    return [
        {
            "date": score.date,
            "scientific_name": tm.scientific_name,
            "common_name": tm.common_name,
            "risk_score": score.value,
            "risk_class": score.risk_level.value.title() if score.risk_level else "Low",
            "detail": "",
        }
        for score in result.daily_scores
    ]
