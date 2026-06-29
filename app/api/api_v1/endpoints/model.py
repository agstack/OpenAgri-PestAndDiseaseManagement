import uuid
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

import crud
from api import deps
import datetime

from core import settings
from models import Parcel
from schemas import DatasetIds, list_path_param
from utils import (
    fetch_parcel_by_id,
    fetch_parcel_lat_lon,
    fetch_weather_data,
    calculate_gdd_wd,
    openmeteo_friendly_variables,
    TimeUnit,
    calculate_risk_index_probability_wd,
    calculate_base,
    fetch_forecast_data_for_parcel,
    calculate_forecast_risk_index,
    fetch_weather_service_forecast_weather_data,
    fetch_weather_service_history_weather_data_offline,
    calculate_risk_index_forecast_wd
)

from http import HTTPStatus

router = APIRouter()

@router.get("/{model_ids}/gdd/", dependencies=[Depends(deps.is_using_gatekeeper)])
def calculate_gdd_fc(
        parcel_id: str,
        from_date: datetime.date,
        to_date: datetime.date,
        access_token: str = Depends(deps.get_jwt),
        model_ids: DatasetIds = Depends(list_path_param),
        db: Session = Depends(deps.get_db),
        formatting: Literal["JSON", "JSON-LD"] = "JSON-LD"
):
    """
    Calculates and returns GDD values (uses resources located on fc)
    """

    if from_date > to_date:
        raise HTTPException(
            status_code=400,
            detail="from_date must be later than to_date, from_date: {} | to_date: {}".format(from_date, to_date)
        )

    if to_date >= datetime.date.today():
        raise HTTPException(
            status_code=400,
            detail="to_date must be earlier than today's date, to_date:{}".format(to_date)
        )

    parcel_fc = fetch_parcel_by_id(access_token=access_token, parcel_id=parcel_id)

    if not parcel_fc:
        raise HTTPException(
            status_code=400,
            detail="Parcel with ID:{} doesn't exist".format(parcel_id)
        )

    lat, lon = fetch_parcel_lat_lon(parcel_fc)

    # Now that we have a (lat,lon) pair, we can query the weather service for weather data
    weather_data = fetch_weather_data(
        latitude=lat,
        longitude=lon,
        access_token=access_token,
        start_date=from_date,
        end_date=to_date,
        variables=["temperature_2m_max"]
    )

    # Fetch the disease models from the DB
    disease_models_db = []
    for disease_id in model_ids.ids:
        disease_model_db = crud.disease.get(db=db, id=disease_id)
        if not disease_model_db:
            raise HTTPException(
                status_code=400,
                detail="Error, model with ID {} does not exist".format(disease_id)
            )

        disease_models_db.append(disease_model_db)

    if formatting.lower() == "json-ld":
        response = calculate_gdd_wd(
            disease_models=disease_models_db,
            weather_data=weather_data
        )
    else:
        response = calculate_base(
            disease_models=disease_models_db,
            weather_data=weather_data
        )

    return response

@router.get("/{model_ids}/risk-index/", dependencies=[Depends(deps.is_using_gatekeeper)])
def calculate_risk_index_fc(
        parcel_id: str,
        from_date: datetime.date,
        to_date: datetime.date,
        access_token: str = Depends(deps.get_jwt),
        model_ids: DatasetIds = Depends(list_path_param),
        db: Session = Depends(deps.get_db),
        formatting: Literal["JSON", "JSON-LD"] = "JSON-LD"
):
    """
    Calculates and returns risk index values (uses resources located on fc)
    """

    if formatting == "JSON":
        raise HTTPException(
            status_code=HTTPStatus.NOT_IMPLEMENTED,
            detail="Error, the JSON format has yet to be implemented"
        )

    if from_date > to_date:
        raise HTTPException(
            status_code=400,
            detail="from_date must be later than to_date, from_date: {} | to_date: {}".format(from_date, to_date)
        )

    parcel_fc = fetch_parcel_by_id(access_token=access_token, parcel_id=parcel_id)

    if not parcel_fc:
        raise HTTPException(
            status_code=400,
            detail="Parcel with ID:{} doesn't exist".format(parcel_id)
        )

    # Fetch the pest models from the database AND fetch a unique list of the units they require
    variables_for_weather_data_call = []
    pest_models_db = []
    for pest_id in model_ids.ids:
        pest_model_db = crud.pest_model.get(db=db, id=pest_id)
        if not pest_model_db:
            raise HTTPException(
                status_code=400,
                detail="Error, model with ID {} does not exist".format(pest_id)
            )

        pest_models_db.append(pest_model_db)

        # Appends the list of units
        list_of_values = list(set([condition.unit.name for rule in pest_model_db.rules for condition in rule.conditions]))

        variables_for_weather_data_call += list_of_values

    lat, lon = fetch_parcel_lat_lon(parcel_fc)

    variables_for_weather_data_call_openmeteo = [openmeteo_friendly_variables[var_name] for var_name in variables_for_weather_data_call]

    # Now that we have a (lat,lon) pair, we can query the weather service for weather data
    weather_data = fetch_weather_data(
        latitude=lat, longitude=lon, access_token=access_token, start_date=from_date, end_date=to_date,
        variables=variables_for_weather_data_call_openmeteo, how_often=TimeUnit.HOURLY
    )

    if "data" not in weather_data:
        raise HTTPException(
            status_code=400,
            detail="Error, weather data API call failed, no data field in response"
        )

    calculation_results = calculate_risk_index_probability_wd(
        parcel=parcel_fc, pest_models=pest_models_db, weather_data=weather_data, lat=lat, lon=lon,
    )

    return calculation_results


@router.get("/{model_ids}/risk-index/forecast/", dependencies=[Depends(deps.get_jwt)])
def calculate_risk_index_including_forecast(
    parcel_id: int,
    past_days: int = 0,
    forecast_days: int = 7,
    model_ids: DatasetIds = Depends(list_path_param),
    db: Session = Depends(deps.get_db),
    formatting: Literal["JSON", "JSON-LD"] = "JSON-LD",
):
    """
    Calculates risk index forecast using weather data obtained from open-meteo
    """

    if formatting == "JSON":
        raise HTTPException(
            status_code=HTTPStatus.NOT_IMPLEMENTED,
            detail="Error, the JSON format has yet to be implemented",
        )

    if past_days < settings.OPEN_METEO_MIN_PAST_DAYS or past_days > settings.OPEN_METEO_MAX_PAST_DAYS:
        raise HTTPException(
            status_code=400,
            detail="Error, past_days outside of bounds, bounds are from {} to {}".format(
                settings.OPEN_METEO_MIN_PAST_DAYS, settings.OPEN_METEO_MAX_PAST_DAYS
            ),
        )

    if forecast_days < settings.OPEN_METEO_MIN_FORECAST_DAYS or forecast_days > settings.OPEN_METEO_MAX_FORECAST_DAYS:
        raise HTTPException(
            status_code=400,
            detail="Error, forecast_days outside of bounds, bounds are from {} to {}".format(
                settings.OPEN_METEO_MIN_FORECAST_DAYS,
                settings.OPEN_METEO_MAX_FORECAST_DAYS,
            ),
        )

    parcel_db: Parcel = crud.parcel.get(db=db, id=parcel_id)
    if not parcel_db:
        raise HTTPException(
            status_code=400,
            detail="Error, parcel with id {} doesn't exist".format(parcel_id),
        )

    # Fetch the pest models from the database AND fetch a unique list of the units they require
    variables_for_weather_data_call = []
    pest_models_db = []
    for pest_id in model_ids.ids:
        pest_model_db = crud.pest_model.get(db=db, id=pest_id)
        if not pest_model_db:
            raise HTTPException(
                status_code=400,
                detail="Error, model with ID {} does not exist".format(pest_id),
            )

        pest_models_db.append(pest_model_db)

        # Appends the list of units
        list_of_values = list(
            set(
                [
                    condition.unit.name
                    for rule in pest_model_db.rules
                    for condition in rule.conditions
                ]
            )
        )

        variables_for_weather_data_call += list_of_values

    hourly_fields = [
        openmeteo_friendly_variables[var_name]
        for var_name in variables_for_weather_data_call
    ]

    if len(hourly_fields) == 0:
        raise HTTPException(
            status_code=400,
            detail="Pest models don't have any conditions that risk index can be calculated against"
        )

    weather_data = fetch_forecast_data_for_parcel(
        latitude=parcel_db.latitude,
        longitude=parcel_db.longitude,
        hourly_fields=hourly_fields,
        past_days=past_days,
        forecast_days=forecast_days,
    )

    if weather_data.empty:
        raise HTTPException(
            status_code=400,
            detail="Error, weather data API call failed, no data field in response",
        )

    calculation_results = calculate_forecast_risk_index(
        parcel=parcel_db, pest_models=pest_models_db, weather_data=weather_data
    )

    return calculation_results



@router.get("/{model_ids}/risk-index/forecast/weather-service/", dependencies=[Depends(deps.is_using_gatekeeper)])
def risk_index_forecast_wd(
    parcel_id: str,
    access_token: str = Depends(deps.get_jwt),
    model_ids: DatasetIds = Depends(list_path_param),
    db: Session = Depends(deps.get_db),
    formatting: Literal["JSON", "JSON-LD"] = "JSON-LD",
):
    """
    Calculates pest risk index forecast for a parcel using weather service data via gatekeeper.
    Gatekeeper only — returns 403 if USING_GATEKEEPER is disabled.

    Path:    model_ids         — comma-separated pest model UUIDs
    Query:   parcel_id         — UUID of the parcel to fetch location from
             formatting        — "JSON-LD" (default): ObservationCollection graph with full linked-data context
                                 "JSON": plain summary object { result_time, models: [{ name, location, observations: [{ timestamp, risk }] }] }
    Returns: 200 with risk classifications per model per forecast timestamp
             403 if USING_GATEKEEPER is disabled
             400 if parcel does not exist, any model UUID does not exist, or weather service call fails
    """

    parcel_fc = fetch_parcel_by_id(access_token=access_token, parcel_id=parcel_id)

    if not parcel_fc:
        raise HTTPException(
            status_code=400,
            detail="Parcel with ID:{} doesn't exist".format(parcel_id)
        )

    lat, lon = fetch_parcel_lat_lon(parcel_fc)

    # Now that we have a (lat,lon) pair, we can query the weather service for weather data
    weather_data = fetch_weather_service_forecast_weather_data(
        latitude=lat,
        longitude=lon,
        access_token=access_token
    )

    # Fetch the pest models from the database
    pest_models_db = []
    for pest_id in model_ids.ids:
        pest_model_db = crud.pest_model.get(db=db, id=pest_id)
        if not pest_model_db:
            raise HTTPException(
                status_code=400,
                detail="Error, model with ID {} does not exist".format(pest_id),
            )

        pest_models_db.append(pest_model_db)

    calculation_results = calculate_risk_index_forecast_wd(
        parcel=parcel_fc, pest_models=pest_models_db, df=weather_data, formatting=formatting
    )

    return calculation_results


@router.get("/{model_ids}/risk-index/forecast/weather-service/offline/", dependencies=[Depends(deps.is_offline_deployment)])
def risk_index_forecast_wd_offline(
    latitude: float,
    longitude: float,
    past_days: int = 30,
    model_ids: DatasetIds = Depends(list_path_param),
    db: Session = Depends(deps.get_db),
    formatting: Literal["JSON", "JSON-LD"] = "JSON-LD",
):
    """
    Calculates pest risk index for the given location using historical weather data
    from the weather service, without requiring a gatekeeper instance or access token.
    Offline-deployment only — returns 403 if OFFLINE_DEPLOYMENT is not enabled.

    Path:    model_ids         — comma-separated pest model UUIDs
    Query:   latitude          — WGS-84 latitude
             longitude         — WGS-84 longitude
             past_days         — days of history to fetch (1–92, default 30)
             formatting        — "JSON-LD" (default): ObservationCollection graph with full linked-data context
                                 "JSON": plain summary object { result_time, models: [{ name, location, observations: [{ timestamp, risk }] }] }
    Returns: 200 with risk classifications per model per hour in the requested window
             403 if OFFLINE_DEPLOYMENT is disabled
             400 if past_days out of range, any model UUID does not exist, or weather service call fails
             500 if WEATHER_SERVICE_BASE_URL is not configured
    """

    if past_days < 1 or past_days > settings.OPEN_METEO_MAX_PAST_DAYS:
        raise HTTPException(
            status_code=400,
            detail="Error, past_days must be between 1 and {}".format(settings.OPEN_METEO_MAX_PAST_DAYS),
        )

    pest_models_db = []
    variables_for_weather_data_call = []
    for pest_id in model_ids.ids:
        pest_model_db = crud.pest_model.get(db=db, id=pest_id)
        if not pest_model_db:
            raise HTTPException(
                status_code=400,
                detail="Error, model with ID {} does not exist".format(pest_id),
            )
        pest_models_db.append(pest_model_db)

        unit_names = list(set(
            condition.unit.name
            for rule in pest_model_db.rules
            for condition in rule.conditions
        ))
        variables_for_weather_data_call += unit_names

    hourly_fields = list(set(
        openmeteo_friendly_variables[name]
        for name in variables_for_weather_data_call
        if name in openmeteo_friendly_variables
    ))

    if not hourly_fields:
        raise HTTPException(
            status_code=400,
            detail="Pest models have no conditions mappable to weather variables",
        )

    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=past_days)

    weather_data = fetch_weather_service_history_weather_data_offline(
        latitude=latitude,
        longitude=longitude,
        start_date=start_date,
        end_date=end_date,
        variables=hourly_fields,
    )

    synthetic_parcel = {
        "@id": "urn:openagri:offline:{}".format(uuid.uuid4()),
        "location": {"lat": latitude, "long": longitude},
    }

    calculation_results = calculate_risk_index_forecast_wd(
        parcel=synthetic_parcel, pest_models=pest_models_db, df=weather_data, formatting=formatting
    )

    return calculation_results
