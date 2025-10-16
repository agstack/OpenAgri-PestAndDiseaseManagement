import datetime
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

import crud
from api import deps
from schemas import Message, CreateData, ListData

from utils import get_logger

logger = get_logger(api_path_name=__name__)
router = APIRouter()

@router.get("/", response_model=ListData, dependencies=[Depends(deps.get_jwt)])
def get_all_data(
        db: Session = Depends(deps.get_db)
) -> ListData:
    """
    This API returns all weather data present in the db.

    WARNING: WILL BE SLOWER AS MORE DATA IS ACCRUED OVER TIME
    """

    response_object = crud.data.get_all(db=db)

    return ListData(list_of_data=response_object)


@router.get("/parcel/{parcel_id}/from/{start}/to/{end}/", response_model=ListData, dependencies=[Depends(deps.get_jwt)])
def get_data_for_parcel(
        parcel_id: int,
        start: datetime.date,
        end: datetime.date,
        db: Session = Depends(deps.get_db)
) -> ListData:
    """
    This API returns weather data for a date interval, for some parcel
    """

    response_object = crud.data.get_data_by_parcel_id_and_date_interval(
        db=db, parcel_id=parcel_id, start=start, end=end
    )

    return ListData(list_of_data=response_object)


@router.post("/{parcel_id}/", response_model=Message, dependencies=[Depends(deps.get_jwt)])
def upload_weather_data_for_parcel(
        parcel_id: int,
        data: List[CreateData],
        db: Session = Depends(deps.get_db)
) -> Message:
    """
    Manually upload one or more weather data points.

    Elements contain:
    [
        "date", "time", "parcel_location", "atmospheric_temperature", "atmospheric_temperature_daily_min",
        "atmospheric_temperature_daily_max", "atmospheric_temperature_daily_average", "atmospheric_relative_humidity",
        "atmospheric_pressure", "precipitation", "average_wind_speed", "wind_direction", "wind_gust",
        "leaf_relative_humidity", "leaf_temperature", "leaf_wetness",
        "soil_temperature_10cm", "soil_temperature_20cm", "soil_temperature_30cm", "soil_temperature_40cm",
        "soil_temperature_50cm", "soil_temperature_60cm", "solar_irradiance_copernicus"
    ]

    date and time are mandatory fields.

    date format: %Y-%m-%d
    time format: %H:%M:%S
    """

    parcel_db = crud.parcel.get(db=db, id=parcel_id)

    if not parcel_db:
        raise HTTPException(
            status_code=400,
            detail="Error, parcel with ID:{} does not exist".format(parcel_id)
        )

    crud.data.batch_insert(db=db, list_of_data=data, parcel_id=parcel_id)

    response_object = Message(
        message="Successfully uploaded data."
    )

    return response_object


@router.delete("/{data_id}/", response_model=Message, dependencies=[Depends(deps.get_jwt)])
def remove_data_point(
        data_id: int,
        db: Session = Depends(deps.get_db)
) -> Message:
    """
    Remove a single weather datapoint
    """

    data_db = crud.data.get(db=db, id=data_id)

    if not data_db:
        raise HTTPException(
            status_code=400,
            detail="Error, data point with ID:{} does not exist.".format(data_id)
        )

    crud.data.remove(db=db, id=data_id)

    response_object = Message(
        message="Successfully removed datapoint with ID:{}".format(data_id)
    )

    return response_object