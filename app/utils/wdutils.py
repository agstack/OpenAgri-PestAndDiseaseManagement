import datetime
import uuid
from typing import Optional

import pandas
from fastapi import HTTPException

import utils

from core import settings
from enum import Enum
from utils.gatekeeper_client import GatekeeperClient
from utils.weather_service_client import WeatherServiceClient

import pandas as pd

# WEATHER_DATA_API_CALL_URL = str(settings.GATEKEEPER_BASE_URL).strip("/") + "/api/proxy/weather_data"

class TimeUnit(Enum):
    HOURLY = "hourly"
    DAILY = "daily"

openmeteo_friendly_variables = {
    "atmospheric_temperature": "temperature_2m",
    "atmospheric_relative_humidity" : "relative_humidity_2m",
    "precipitation": "precipitation",
    "atmospheric_pressure": "surface_pressure",
    "average_wind_speed": "wind_speed_10m",
    "soil_temperature_10cm": "soil_temperature_0_to_7cm",
    "soil_temperature_20cm": "soil_temperature_7_to_28cm",
    "soil_temperature_30cm": "soil_temperature_28_to_100cm",
    "soil_temperature_40cm": "soil_temperature_100_to_255cm"
}

openweathermap_friendly_variables = {
    "timestamp": "timestamp",
    "ambient_humidity": "atmospheric_relative_humidity",
    "ambient_temperature": "atmospheric_temperature",
    "wind_speed": "average_wind_speed",
    "precipitation": "precipitation"
}

def fetch_weather_data(
        latitude: float,
        longitude: float,
        start_date: datetime.date,
        end_date: datetime.date,
        variables: list,
        access_token: str,
        radius_km: int = 10,
        how_often: TimeUnit = TimeUnit.DAILY
) -> dict:
    client = GatekeeperClient(str(settings.GATEKEEPER_BASE_URL))
    return client.get_weather_history(
        access_token=access_token,
        lat=latitude,
        lon=longitude,
        start_date=start_date,
        end_date=end_date,
        variables=variables,
        radius_km=radius_km,
        how_often=how_often.value,
    )


def fetch_weather_service_forecast_weather_data(
    latitude: float,
    longitude: float,
    access_token: Optional[str] = None,
):
    client = GatekeeperClient(str(settings.GATEKEEPER_BASE_URL))
    raw = client.get_weather_forecast(access_token=access_token, lat=latitude, lon=longitude)
    return convert_weather_service_forecast_weather_data_to_dataframe(raw)


def fetch_weather_service_history_weather_data_offline(
    latitude: float,
    longitude: float,
    start_date,
    end_date,
    variables: list,
):
    if settings.WEATHER_SERVICE_BASE_URL is None:
        raise HTTPException(
            status_code=500,
            detail="WEATHER_SERVICE_BASE_URL is not configured for offline deployment."
        )
    client = WeatherServiceClient(str(settings.WEATHER_SERVICE_BASE_URL))
    raw = client.get_hourly_history(
        lat=latitude,
        lon=longitude,
        start_date=start_date,
        end_date=end_date,
        variables=variables,
    )
    return convert_weather_service_history_weather_data_to_dataframe(raw)


def convert_weather_service_history_weather_data_to_dataframe(response_json: dict):
    openmeteo_to_openagri = {v: k for k, v in openmeteo_friendly_variables.items()}
    rows = []
    for obs in response_json.get("data", []):
        row = {"timestamp": obs["timestamp"]}
        for var, val in obs.get("values", {}).items():
            openagri_name = openmeteo_to_openagri.get(var)
            if openagri_name:
                row[openagri_name] = val
        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty or "timestamp" not in df.columns:
        raise HTTPException(
            status_code=400,
            detail="Weather service returned no history data"
        )
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def convert_weather_service_forecast_weather_data_to_dataframe(json_data: list):
    df = pd.json_normalize(json_data)
    if df.empty or "timestamp" not in df.columns:
        raise HTTPException(
            status_code=400,
            detail="Weather service returned no forecast data"
        )
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df_pivoted = df.pivot(index="timestamp", columns="measurement_type", values="value")
    df_pivoted.columns.name = None
    df_pivoted.reset_index(inplace=True)
    return df_pivoted


def calculate_risk_index_forecast_wd(
    df: pandas.DataFrame,
    parcel,
    pest_models: list,
    formatting: str = "JSON-LD",
):
    reverse_dict = {v: k for k, v in openweathermap_friendly_variables.items()}
    df.rename(columns=reverse_dict, inplace=True)

    owm_valid = df.columns.intersection(openweathermap_friendly_variables.keys())
    df_owm = df[owm_valid].rename(columns=openweathermap_friendly_variables)

    owm_covered_openagri = set(openweathermap_friendly_variables.values())
    extra_openagri = set(openmeteo_friendly_variables.keys()) - owm_covered_openagri
    extra_cols = df.columns.intersection(extra_openagri)
    df = pd.concat([df_owm, df[extra_cols]], axis=1)

    for pm in pest_models:
        risks_for_current_pm = ["Low"] * df.shape[0]

        for rule in pm.rules:
            final_str = "(x['{}'] {} {})".format(
                rule.conditions[0].unit.name,
                rule.conditions[0].operator.symbol,
                rule.conditions[0].value,
            )
            for cond in rule.conditions[1:]:
                final_str = (final_str + " & " + "(x['{}'] {} {})".format(
                    cond.unit.name, cond.operator.symbol, cond.value
                )
                             )

            df_with_risk = df.assign(
                risk=eval("lambda x: {}".format(final_str))
            )

            risks_for_current_pm = [
                rule.probability_value if x else y
                for x, y in zip(df_with_risk["risk"], risks_for_current_pm)
            ]

        df["{}".format(pm.name)] = risks_for_current_pm

    if formatting == "JSON":
        models = []
        for pm in pest_models:
            observations = [
                {
                    "timestamp": "{}".format(str(date).replace(" ", "T")),
                    "risk": "{}".format(risk),
                }
                for date, risk in zip(df["timestamp"], df["{}".format(pm.name)])
            ]
            models.append(
                {
                    "name": pm.name,
                    "location": {
                        "lat": parcel["location"]["lat"],
                        "lon": parcel["location"]["long"],
                    },
                    "observations": observations,
                }
            )
        return {
            "result_time": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "models": models,
        }

    context = utils.context

    graph = []

    for pm in pest_models:

        calculated_risks = []
        for date, risk in zip(df["timestamp"], df["{}".format(pm.name)]):
            calculated_risks.append(
                {
                    "@id": "urn:openagri:pestInfectationRisk:obs2:{}".format(
                        uuid.uuid4()
                    ),
                    "@type": ["Observation", "PestInfestationRisk"],
                    "phenomenonTime": "{}".format(str(date).replace(" ", "T")),
                    "hasSimpleResult": "{}".format(risk),
                }
            )

        graph_element = {
            "@id": "urn:openagri:pestInfectationRisk:{}".format(uuid.uuid4()),
            "@type": ["ObservationCollection"],
            "description": "{} pest infectation risk forecast in x ".format(pm.name),
            "observedProperty": {
                "@id": "urn:openagri:pestInfectationRisk:op:{}".format(uuid.uuid4()),
                "@type": ["ObservableProperty", "PestInfection"],
                "name": "UNCINE pest infection",
                "hasAgriPest": {
                    "@id": "urn:openagri:pest:UNCINE",
                    "@type": "AgriPest",
                    "name": "UNCINE",
                    "description": "Uncinula necator (syn. Erysiphe necator) is a fungus that causes powdery mildew of grape. It is a common pathogen of Vitis species, including the wine grape, Vitis vinifera",
                    "eppoConcept": "https://gd.eppo.int/taxon/UNCINE",
                },
            },
            "madeBySensor": {
                "@id": "urn:openagri:pestInfectationRisk:model:{}".format(uuid.uuid4()),
                "@type": ["Sensor", "AIPestDetectionModel"],
                "name": "AI pest detaction model xyz",
            },
            "hasFeatureOfInterest": {
                "@id": "urn:openagri:pestInfectationRisk:foi:{}".format(uuid.uuid4()),
                "@type": ["FeatureOfInterest", "Point"],
                "long": "{}".format(parcel["location"]["long"]),
                "lat": "{}".format(parcel["location"]["lat"]),
            },
            "basedOnWeatherDataset": {
                "@id": "urn:openagri:weatherDataset:{}".format(parcel["@id"]),
                "@type": "WeatherDataset",
                "name": "parcel_name_tba",
            },
            "resultTime": "{}".format(
                datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            ),
            "hasMember": calculated_risks,
        }

        graph.append(graph_element)

    doc = {"@context": context, "@graph": graph}

    return doc
