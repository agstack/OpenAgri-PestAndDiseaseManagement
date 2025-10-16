from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

import crud
from api import deps
import datetime


from schemas import DatasetIds, list_path_param
from utils import fetch_parcel_by_id, fetch_parcel_lat_lon, fetch_weather_data, calculate_gdd_wd, \
    openmeteo_friendly_variables, TimeUnit, calculate_risk_index_probability_wd, calculate_base

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
        parcel=parcel_fc, pest_models=pest_models_db, weather_data=weather_data, lat=lat, lon=lon
    )

    return calculation_results