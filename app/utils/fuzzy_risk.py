"""
Fuzzy pest/disease risk engine

Public API
──────────
compute_features(df, extra_t_bases)  → enriched DataFrame
calculate_fuzzy_risk(weather_df, threat_models) → results DataFrame
"""

from __future__ import annotations

import datetime
import uuid
import warnings
from typing import Any, List, Optional

import numpy as np
import openmeteo_requests
import pandas as pd
import requests_cache
from fastapi import HTTPException
from retry_requests import retry
from sqlalchemy.orm import Session

import crud
from schemas.threat_model import ThreatModelDB
from utils.custom_schemas import context as OPENAGRI_CONTEXT
from utils.fuzzy_config import (
    FUZZY_TRANSITION_FRACTION,
    FUZZY_MIN_MU,
    RISK_THRESHOLD_CRITICAL,
    RISK_THRESHOLD_HIGH,
    RISK_THRESHOLD_MODERATE,
    STREAK_MIN_FACTOR,
    PHENOLOGY_T_BASE,
    PHENO_FUZZY_MARGIN_FRAC,
    WETNESS_HOURS_BY_RH,
    WETNESS_HOURS_BY_RAIN,
)
from core.config import settings

warnings.filterwarnings("ignore")

_RISK_SCORE_MAP = {
    "critical": 100,
    "high": 80,
    "moderate": 40,
    "medium": 40,
    "low": 10,
}

# Genera whose behaviour is better modelled with 7-day moving averages
_PATHOGEN_KEYWORDS = {
    "Venturia",
    "Plasmopora",
    "Plasmopara",
    "Uncinula",
    "Erysiphe",
    "Botrytis",
    "Monilia",
    "Monilinia",
    "Taphrina",
    "Erwinia",
    "Guignardia",
    "Phomopsis",
    "Colletotrichum",
    "Pseudomonas",
    "Pseudocercospora",
    "Alternaria",
    "Hemileia",
    "Phytophthora",
    "Moniliophthora",
}


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE ENGINEERING
# ─────────────────────────────────────────────────────────────────────────────


def _wetness_hours(rh_series: pd.Series, rain_series: pd.Series) -> pd.Series:
    rh = rh_series.values
    rain = rain_series.values
    h_rh = np.zeros(len(rh))
    for thr, hrs in sorted(WETNESS_HOURS_BY_RH, key=lambda x: x[0]):
        h_rh = np.where(rh >= thr, hrs, h_rh)
    h_rain = np.zeros(len(rain))
    for thr, hrs in sorted(WETNESS_HOURS_BY_RAIN, key=lambda x: x[0]):
        h_rain = np.where(rain >= thr, hrs, h_rain)
    return pd.Series(np.minimum(24.0, h_rh + h_rain), index=rh_series.index)


def _gdd_season_key(date_series: pd.Series, gdd_reset_month: int) -> pd.Series:
    years = date_series.dt.year
    months = date_series.dt.month
    if gdd_reset_month == 1:
        return years
    return np.where(months >= gdd_reset_month, years, years - 1)


def compute_features(
    df: pd.DataFrame,
    extra_t_bases: set[float] | None = None,
) -> pd.DataFrame:
    """Enrich a raw daily weather DataFrame with derived features.

    Required input columns: date, temp_max, temp_min, humidity, rainfall
    extra_t_bases: additional GDD base temperatures beyond {0, 5, 10}
    """
    gdd_reset_month = settings.GDD_RESET_MONTH

    df = df.copy().sort_values("date").reset_index(drop=True)
    df["temp_avg"] = (df["temp_max"] + df["temp_min"]) / 2.0

    for w in [3, 7, 14]:
        df[f"temp_avg_{w}d"] = df["temp_avg"].rolling(w, min_periods=1).mean()
        df[f"humidity_{w}d"] = df["humidity"].rolling(w, min_periods=1).mean()
        df[f"rainfall_{w}d"] = df["rainfall"].rolling(w, min_periods=1).sum()

    df["rain_3d"] = df["rainfall_3d"]
    df["rain_10d"] = df["rainfall"].rolling(10, min_periods=1).sum()

    df["wetness_h"] = _wetness_hours(df["humidity"], df["rainfall"])
    df["wetness_3d"] = df["wetness_h"].rolling(3, min_periods=1).sum()

    df["season_year"] = _gdd_season_key(pd.to_datetime(df["date"]), gdd_reset_month)

    _all_bases = sorted({0.0, 5.0, 10.0} | (extra_t_bases or set()))
    for t_base in _all_bases:
        label = f"{int(t_base)}b"
        if f"gdd_cum_{label}" not in df.columns:
            daily = np.maximum(0.0, df["temp_avg"] - t_base)
            df[f"gdd_daily_{label}"] = daily
            df[f"gdd_cum_{label}"] = df.groupby("season_year")[
                f"gdd_daily_{label}"
            ].transform("cumsum")

    df["gdd_daily_pheno"] = np.maximum(0.0, df["temp_avg"] - PHENOLOGY_T_BASE)
    df["gdd_cum_pheno"] = df.groupby("season_year")["gdd_daily_pheno"].transform(
        "cumsum"
    )

    for t_base in _all_bases:
        label = f"{int(t_base)}b"
        col = f"gdd_daily_{label}"
        seasons_b = df.groupby("season_year")[col].agg(count="count", total="sum")
        complete_b = seasons_b[seasons_b["count"] >= 350]
        if len(complete_b) > 0:
            ref_b = float(complete_b["total"].mean())
        else:
            last_b = seasons_b.iloc[-1]
            ref_b = float(last_b["total"]) * (365.0 / max(int(last_b["count"]), 1))
        df[f"gdd_annual_ref_{label}"] = ref_b

    df["gdd_annual_ref"] = df["gdd_annual_ref_5b"]

    _e_sat = 0.6108 * np.exp(17.27 * df["temp_avg"] / (df["temp_avg"] + 237.3))
    df["vpd"] = ((1.0 - df["humidity"] / 100.0) * _e_sat).clip(lower=0.0)
    df["vpd_3d"] = df["vpd"].rolling(3, min_periods=1).mean()
    df["vpd_7d"] = df["vpd"].rolling(7, min_periods=1).mean()

    for thr in [60, 70, 80, 85, 90, 95]:
        count, streak = 0, []
        for h in df["humidity"]:
            count = count + 1 if h >= thr else 0
            streak.append(count)
        df[f"streak_hum{thr}"] = streak

    return df


# ─────────────────────────────────────────────────────────────────────────────
# FUZZY MEMBERSHIP FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────


def _trapezoid(x: float, a: float, b: float, c: float, d: float) -> float:
    if x <= a or x >= d:
        return 0.0
    if b <= x <= c:
        return 1.0
    if a < x < b:
        return (x - a) / (b - a)
    return (d - x) / (d - c)


def membership_humidity(h: float, lo: float, hi: float) -> float:
    if lo == 0.0 and hi == 100.0:
        return 1.0
    width = max(hi - lo, 5.0)
    transition = width * FUZZY_TRANSITION_FRACTION
    return _trapezoid(h, lo - transition, lo, hi, hi + transition)


def membership_temp(t: float, lo: float, hi: float) -> float:
    if lo <= -900 and hi >= 900:
        return 1.0
    width = max(hi - lo, 2.0)
    transition = max(width * FUZZY_TRANSITION_FRACTION, 1.5)
    return _trapezoid(t, lo - transition, lo, hi, hi + transition)


def membership_rainfall(r_cum_3d: float, rain_min: float) -> float:
    if rain_min == 0.0:
        return 1.0
    return _trapezoid(r_cum_3d, rain_min * 0.4, rain_min, rain_min * 3, rain_min * 6)


def membership_phenology(gdd_now: float, lo: float, hi: float) -> float:
    width = hi - lo
    margin = width * PHENO_FUZZY_MARGIN_FRAC
    if lo <= gdd_now <= hi:
        return 1.0
    if (lo - margin) < gdd_now < lo:
        return (gdd_now - (lo - margin)) / margin
    if hi < gdd_now < (hi + margin):
        return ((hi + margin) - gdd_now) / margin
    return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# PER-DAY SCORING
# ─────────────────────────────────────────────────────────────────────────────


def _phenology_mu(
    weather_row: pd.Series, pest_params: dict
) -> tuple[float, float, float | None, float | None]:
    pheno_lo = pest_params.get("pheno_lo")
    pheno_hi = pest_params.get("pheno_hi")
    frac_lo = pest_params.get("pheno_frac_lo")
    frac_hi = pest_params.get("pheno_frac_hi")

    _tb = pest_params.get("t_base")
    t_base_pest = _tb if _tb is not None else PHENOLOGY_T_BASE
    gdd_col_pest = f"gdd_cum_{int(t_base_pest)}b"
    ref_col_pest = f"gdd_annual_ref_{int(t_base_pest)}b"
    gdd_now = float(weather_row.get(gdd_col_pest, weather_row.get("gdd_cum_pheno", 0)))
    gdd_ref = float(weather_row.get(ref_col_pest, weather_row.get("gdd_annual_ref", 0)))

    if frac_lo is not None and frac_hi is not None and gdd_ref > 0:
        lo_pheno = frac_lo * gdd_ref
        hi_pheno = frac_hi * gdd_ref
    elif pheno_lo is not None and pheno_hi is not None:
        lo_pheno = pheno_lo
        hi_pheno = pheno_hi
    else:
        return 1.0, gdd_now, None, None

    if hi_pheno >= 9000:
        return 1.0, gdd_now, lo_pheno, hi_pheno
    return (
        membership_phenology(gdd_now, lo_pheno, hi_pheno),
        gdd_now,
        lo_pheno,
        hi_pheno,
    )


def _classify(score: float) -> str:
    if score >= RISK_THRESHOLD_CRITICAL:
        return "Critical"
    if score >= RISK_THRESHOLD_HIGH:
        return "High"
    if score >= RISK_THRESHOLD_MODERATE:
        return "Moderate"
    return "Low"


def score_day(
    weather_row: pd.Series,
    rules_pest: list[dict],
    pest_key: str,
    pest_params: dict,
) -> dict:
    t_raw = float(weather_row["temp_avg"])
    t_lethal_max_p = pest_params.get("t_lethal_max")
    t_lethal_min_p = pest_params.get("t_lethal_min")

    if t_lethal_max_p is not None and t_raw > float(t_lethal_max_p):
        return {
            "score": 0.0,
            "risk_class": "Low",
            "detail": f"T={t_raw:.1f}°C > t_lethal_max={t_lethal_max_p}°C",
        }
    if t_lethal_min_p is not None and t_raw < float(t_lethal_min_p):
        return {
            "score": 0.0,
            "risk_class": "Low",
            "detail": f"T={t_raw:.1f}°C < t_lethal_min={t_lethal_min_p}°C",
        }

    mu_pheno, gdd_now, lo_pheno, hi_pheno = _phenology_mu(weather_row, pest_params)
    if lo_pheno is not None and mu_pheno == 0.0:
        return {
            "score": 0.0,
            "risk_class": "Out of season",
            "detail": f"GDD={gdd_now:.0f} outside [{lo_pheno:.0f},{hi_pheno:.0f}]",
        }

    # Weather input selection: fungi/bacteria use 7d MA; insects use daily
    is_pathogen = any(kw in pest_key for kw in _PATHOGEN_KEYWORDS)
    if is_pathogen:
        t_eff = weather_row.get("temp_avg_7d", weather_row["temp_avg"])
        h_eff = weather_row.get("humidity_7d", weather_row["humidity"])
        r_eff = weather_row.get("rain_3d", weather_row["rainfall"])
        streak_col = "streak_hum70"
    else:
        t_eff = weather_row["temp_avg"]
        h_eff = weather_row["humidity"]
        r_eff = weather_row.get("rain_3d", weather_row["rainfall"])
        streak_col = "streak_hum60"

    streak = int(weather_row.get(streak_col, 0))

    # Fuzzification + Mamdani AND aggregation
    activated = []
    for rule in rules_pest:
        mu_t = membership_temp(t_eff, rule["temp_lo"], rule["temp_hi"])
        mu_h = membership_humidity(h_eff, rule["hum_lo"], rule["hum_hi"])
        mu_r = membership_rainfall(r_eff, rule["rain_min"])
        mu = min(mu_t, mu_h, mu_r)
        if mu > FUZZY_MIN_MU:
            activated.append((mu, rule["risk_score"], rule["risk"]))

    if not activated:
        return {"score": 0.0, "risk_class": "Low", "detail": "no active rules"}

    # Weighted defuzzification
    total_mu = sum(m for m, _, _ in activated)
    score = sum(m * s for m, s, _ in activated) / total_mu

    # Streak penalty
    min_streak = pest_params.get("min_streak") or 1
    if streak < min_streak:
        if min_streak >= 2:
            streak_factor = streak / max(min_streak, 1)
        else:
            streak_factor = max(STREAK_MIN_FACTOR, streak / max(min_streak, 1))
        score *= streak_factor

    # Mills-type leaf-wetness gate
    wh = float(weather_row.get("wetness_h", 0))
    min_wh_crit = pest_params.get("min_wetness_hours_critical")
    min_wh_high = pest_params.get("min_wetness_hours_high")

    if (
        min_wh_crit is not None
        and isinstance(min_wh_crit, (int, float))
        and score >= RISK_THRESHOLD_CRITICAL
        and wh < float(min_wh_crit)
    ):
        score = min(score, float(RISK_THRESHOLD_CRITICAL) - 0.1)

    if (
        min_wh_high is not None
        and isinstance(min_wh_high, (int, float))
        and score >= RISK_THRESHOLD_HIGH
        and wh < float(min_wh_high)
    ):
        score = min(score, float(RISK_THRESHOLD_HIGH) - 0.1)

    # Phenological scaling + classification
    score = min(100.0, round(score * mu_pheno, 1))
    risk_class = _classify(score)

    best = max(activated, key=lambda x: x[0])
    return {
        "score": score,
        "risk_class": risk_class,
        "detail": (
            f"best_rule={best[2]} mu={best[0]:.2f} "
            f"mu_pheno={mu_pheno:.2f} streak={streak}d wh={wh:.0f}h"
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC ENGINE ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────


def _nf(val: Any, default: float) -> float:
    """Return float(val) or default when val is None (key absent OR explicit null)."""
    return default if val is None else float(val)


def _definition_to_rules(definition: dict) -> list[dict]:
    """Convert ThreatModel.definition fuzzy_rules to score_day format."""
    rules = []
    for r in definition.get("fuzzy_rules", []):
        raw_level = r.get("risk_level")
        risk_label = str(raw_level).lower() if raw_level is not None else "low"
        rules.append(
            {
                "hum_lo": _nf(r.get("hum_lo"), 0.0),
                "hum_hi": _nf(r.get("hum_hi"), 100.0),
                "temp_lo": _nf(r.get("temp_lo"), -999.0),
                "temp_hi": _nf(r.get("temp_hi"), 999.0),
                "rain_min": _nf(r.get("rain_min"), 0.0),
                "risk": risk_label.capitalize(),
                "risk_score": _RISK_SCORE_MAP.get(risk_label, 10),
            }
        )
    return rules


def calculate_fuzzy_risk(
    weather_df: pd.DataFrame,
    threat_models: list[Any],
    hourly_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Run the fuzzy risk model for all threat models over the weather period.

    threat_models: objects with .scientific_name, .common_name, .definition
                   (.definition is a dict with 'bio_params' and 'fuzzy_rules')
    hourly_df:     optional pre-aggregation hourly weather (DB-named columns). Used
                   only by the agstack path, which feeds it to the package so it
                   aggregates hourly->daily itself (mean temp, MAX RH) — an accuracy
                   upgrade over the daily bridge. The inline path ignores it.

    Returns DataFrame with columns:
      date, scientific_name, common_name, risk_score, risk_class, detail
    """
    if settings.USE_AGSTACK_PND:
        return _calculate_fuzzy_risk_agstack(weather_df, threat_models, hourly_df)

    extra_t_bases = {
        float(
            v
            if (v := (tm.definition.get("bio_params") or {}).get("t_base")) is not None
            else 5.0
        )
        for tm in threat_models
    }
    enriched = compute_features(weather_df, extra_t_bases=extra_t_bases)

    results = []
    for tm in threat_models:
        definition = tm.definition if isinstance(tm.definition, dict) else {}
        bio_params = definition.get("bio_params") or {}
        rules = _definition_to_rules(definition)

        if not rules:
            continue

        for _, row in enriched.iterrows():
            res = score_day(row, rules, tm.scientific_name, bio_params)
            results.append(
                {
                    "date": row["date"],
                    "scientific_name": tm.scientific_name,
                    "common_name": tm.common_name,
                    "risk_score": res["score"],
                    "risk_class": res["risk_class"],
                    "detail": res["detail"],
                }
            )

    return pd.DataFrame(results)


def _calculate_fuzzy_risk_agstack(
    weather_df: pd.DataFrame,
    threat_models: list[Any],
    hourly_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    from agstack_pnd.models.agronomic.phenology import PhenologicalGating
    from agstack_pnd.models.disease.fuzzy_mamdani import FuzzyMamdaniRisk
    from utils.agstack_adapter import (
        daily_df_to_wdf,
        hourly_df_to_wdf,
        result_to_rows,
        threatmodel_to_definition,
    )

    scored = [
        tm
        for tm in threat_models
        if (tm.definition if isinstance(tm.definition, dict) else {}).get("fuzzy_rules")
    ]
    if not scored or weather_df.empty:
        return pd.DataFrame()

    extra_t_bases = {
        float(
            v
            if (v := (tm.definition.get("bio_params") or {}).get("t_base")) is not None
            else 5.0
        )
        for tm in scored
    }
    # The daily frame always supplies the phenology reference + season_year.
    enriched = compute_features(weather_df, extra_t_bases=extra_t_bases)
    season_by_date = dict(
        zip(pd.to_datetime(enriched["date"]).dt.date, enriched["season_year"])
    )

    # Scoring frame: hourly when supplied (the package aggregates hourly->daily
    # itself — mean temp, MAX relative humidity — an intended accuracy upgrade over
    # the daily bridge), else the daily frame (unchanged behaviour).
    if hourly_df is not None and not hourly_df.empty:
        score_frame, to_wdf = hourly_df.copy(), hourly_df_to_wdf
    else:
        score_frame, to_wdf = weather_df.copy(), daily_df_to_wdf
    score_frame["date"] = pd.to_datetime(score_frame["date"])
    score_frame["__season"] = score_frame["date"].dt.date.map(season_by_date)
    # Defensive: a scoring-frame date absent from the daily reference maps to NaN.
    # Drop it explicitly (groupby would silently exclude it anyway). In normal wiring
    # the hourly/daily frames share a source so no row is dropped.
    score_frame = score_frame.dropna(subset=["__season"]).sort_values("date")

    fuzzy = FuzzyMamdaniRisk()
    pheno = PhenologicalGating()
    rows: list[dict] = []
    for tm in scored:
        definition = threatmodel_to_definition(tm)
        t_base = float(definition.bio_params.t_base)
        ref_col = f"gdd_annual_ref_{int(t_base)}b"
        ref_value = float(
            enriched[ref_col].iloc[0]
            if ref_col in enriched.columns
            else enriched["gdd_annual_ref_5b"].iloc[0]
        )
        if (
            definition.bio_params.pheno_frac_lo is not None
            and definition.bio_params.pheno_frac_hi is not None
        ):
            definition.bio_params.pheno_fraction_ref_gdd5 = ref_value

        for _season, slice_df in score_frame.groupby("__season", sort=True):
            wdf = to_wdf(slice_df.drop(columns="__season"))
            result = fuzzy.calculate(weather_data=wdf, threat=definition)
            mu_by_date = {
                s.date: s.value
                for s in pheno.calculate(
                    wdf, {"base_temp": t_base}, threat=definition
                ).daily_scores
            }
            for row in result_to_rows(result, tm):
                if mu_by_date.get(row["date"], 1.0) == 0.0:
                    row["risk_score"] = 0.0
                    row["risk_class"] = "Out of season"
                else:
                    row["risk_class"] = _classify(float(row["risk_score"]))
                rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


# ─── endpoint helpers ────────────────────────────────────────────────────────


def _weather_rows_to_daily_df(rows) -> pd.DataFrame:
    """Aggregate DB hourly rows to one row per calendar day."""
    if not rows:
        return pd.DataFrame(
            columns=["date", "temp_max", "temp_min", "humidity", "rainfall"]
        )
    records = [
        {
            "date": r.date,
            "temp": r.atmospheric_temperature,
            "humidity": r.atmospheric_relative_humidity,
            "rainfall": r.precipitation or 0.0,
        }
        for r in rows
        if r.atmospheric_temperature is not None
    ]
    if not records:
        return pd.DataFrame(
            columns=["date", "temp_max", "temp_min", "humidity", "rainfall"]
        )
    df = pd.DataFrame(records)
    daily = (
        df.groupby("date")
        .agg(
            temp_max=("temp", "max"),
            temp_min=("temp", "min"),
            humidity=("humidity", "mean"),
            rainfall=("rainfall", "sum"),
        )
        .reset_index()
    )
    daily["date"] = pd.to_datetime(daily["date"])
    return daily.sort_values("date").reset_index(drop=True)


def _weather_rows_to_hourly_df(rows) -> pd.DataFrame:
    """DB hourly rows -> one-row-per-reading DataFrame for the agstack package
    (which aggregates hourly->daily itself: mean temp, MAX RH, sum rain). Columns:
    date (hourly datetime), atmospheric_temperature, atmospheric_relative_humidity,
    precipitation. DB-named columns so the adapter's FIELD_MAP applies."""
    cols = [
        "date", "atmospheric_temperature",
        "atmospheric_relative_humidity", "precipitation",
    ]
    records = [
        {
            "date": datetime.datetime.combine(
                r.date, getattr(r, "time", None) or datetime.time()
            ),
            "atmospheric_temperature": r.atmospheric_temperature,
            "atmospheric_relative_humidity": r.atmospheric_relative_humidity,
            "precipitation": r.precipitation or 0.0,
        }
        for r in (rows or [])
        if r.atmospheric_temperature is not None
    ]
    if not records:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def _openmeteo_forecast_hourly_df(
    latitude: float, longitude: float, days_ahead: int
) -> pd.DataFrame:
    """Fetch hourly forecast from OpenMeteo; return one row per hour with a tz-aware
    (UTC) datetime ``date`` column and DB-named weather columns. Consumed by
    ``_hourly_df_to_daily`` (daily bridge) and the agstack hourly path
    (``hourly_df_to_wdf``)."""
    if days_ahead < settings.OPEN_METEO_MIN_FORECAST_DAYS:
        days_ahead = settings.OPEN_METEO_MIN_FORECAST_DAYS
    if days_ahead > settings.OPEN_METEO_MAX_FORECAST_DAYS:
        days_ahead = settings.OPEN_METEO_MAX_FORECAST_DAYS

    cache_sess = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_sess = retry(cache_sess, retries=3, backoff_factor=0.2)
    client = openmeteo_requests.Client(session=retry_sess)

    try:
        responses = client.weather_api(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": latitude,
                "longitude": longitude,
                "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation"],
                "forecast_days": days_ahead,
            },
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OpenMeteo error: {e}")
    finally:
        client.session.close()

    hourly = responses[0].Hourly()
    dates = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )
    return pd.DataFrame(
        {
            "date": dates,
            "atmospheric_temperature": hourly.Variables(0).ValuesAsNumpy(),
            "atmospheric_relative_humidity": hourly.Variables(1).ValuesAsNumpy(),
            "precipitation": hourly.Variables(2).ValuesAsNumpy(),
        }
    )


def _openmeteo_to_daily_df(
    latitude: float, longitude: float, days_ahead: int
) -> pd.DataFrame:
    """Fetch hourly forecast from OpenMeteo and aggregate to daily."""
    return _hourly_df_to_daily(
        _openmeteo_forecast_hourly_df(latitude, longitude, days_ahead)
    )


def _hourly_df_to_daily(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate an OpenMeteo hourly DataFrame (DB column names) to daily records."""
    if df.empty:
        return pd.DataFrame(
            columns=["date", "temp_max", "temp_min", "humidity", "rainfall"]
        )
    work = pd.DataFrame(
        {
            "date": pd.to_datetime(df["date"]).dt.tz_localize(None).dt.date,
            "temp": df["atmospheric_temperature"],
            "humidity": df["atmospheric_relative_humidity"],
            "rainfall": df["precipitation"].fillna(0.0),
        }
    )
    daily = (
        work.groupby("date")
        .agg(
            temp_max=("temp", "max"),
            temp_min=("temp", "min"),
            humidity=("humidity", "mean"),
            rainfall=("rainfall", "sum"),
        )
        .reset_index()
    )
    daily["date"] = pd.to_datetime(daily["date"])
    return daily.sort_values("date").reset_index(drop=True)


def _results_to_jsonld(
    results_df: pd.DataFrame, parcel, include_meta: bool = True
) -> dict:
    """Convert calculate_fuzzy_risk output to OpenAGRI JSON-LD envelope."""
    graph = []
    for sci_name, grp in results_df.groupby("scientific_name"):
        common = grp["common_name"].iloc[0]
        observations = []
        for _, row in grp.iterrows():
            obs = {
                "@id": f"urn:openagri:fuzzyRisk:obs:{uuid.uuid4()}",
                "@type": ["Observation", "PestInfestationRisk"],
                "phenomenonTime": str(row["date"].date()),
                "hasSimpleResult": f"{row['risk_score']:.1f}",
                "riskClass": row["risk_class"],
            }
            if include_meta and row.get("detail"):
                obs["meta"] = row["detail"]
            observations.append(obs)

        graph.append(
            {
                "@id": f"urn:openagri:fuzzyRisk:col:{uuid.uuid4()}",
                "@type": ["ObservationCollection"],
                "description": f"Fuzzy risk for {sci_name} ({common})",
                "observedProperty": {
                    "@id": f"urn:openagri:fuzzyRisk:op:{uuid.uuid4()}",
                    "@type": ["ObservableProperty", "PestInfection"],
                    "name": sci_name,
                    "commonName": common,
                },
                "madeBySensor": {
                    "@id": f"urn:openagri:fuzzyRisk:model:{uuid.uuid4()}",
                    "@type": ["Sensor", "FuzzyRiskModel"],
                    "name": "Fuzzy Pest & Disease Risk Model v2.0",
                },
                "hasFeatureOfInterest": {
                    "@id": f"urn:openagri:fuzzyRisk:foi:{uuid.uuid4()}",
                    "@type": ["FeatureOfInterest", "Point"],
                    "long": str(parcel.longitude),
                    "lat": str(parcel.latitude),
                },
                "resultTime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "hasMember": observations,
            }
        )

    return {"@context": OPENAGRI_CONTEXT, "@graph": graph}


def _format_results(results_df: pd.DataFrame, parcel, fmt: str) -> dict | list:
    """Return raw JSON records or OpenAGRI JSON-LD envelope."""
    if fmt == "json":
        out = results_df.copy()
        out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
        return out.to_dict(orient="records")
    return _results_to_jsonld(results_df, parcel)


def _resolve_threat_models(
    db: Session, threat_model_ids: Optional[List[uuid.UUID]]
) -> List[ThreatModelDB]:
    if threat_model_ids:
        models = [crud.threat_model.get(db=db, id=tid) for tid in threat_model_ids]
        missing = [str(tid) for tm, tid in zip(models, threat_model_ids) if tm is None]
        if missing:
            raise HTTPException(
                status_code=404, detail=f"ThreatModels not found: {missing}"
            )
    else:
        models = crud.threat_model.get_multi(db=db, limit=10000)
    return [ThreatModelDB.model_validate(tm) for tm in models]
