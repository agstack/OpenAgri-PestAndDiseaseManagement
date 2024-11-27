import datetime

from fastapi import APIRouter, Depends, File, UploadFile, HTTPException
from sqlalchemy.orm import Session

import crud
import utils
from api import deps
from models import User
from schemas import Message, BaseDataset

from csv import reader
from codecs import iterdecode

from utils.data import read_rows_csv

router = APIRouter()


@router.post("/upload/", response_model=Message)
async def upload(
        csv_file: UploadFile = File(...),
        db: Session = Depends(deps.get_db),
        user: User = Depends(deps.get_current_user)
) -> Message:
    """
    Upload a .csv file that contains data about weather.

    List of columns that this api will parse:

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

    any decimal numbers present in the dataset should be formatted using "." (dot) and not "," (comma),
    but nevertheless, the api will attempt to swap "," for ".".

    """

    dataset_db = crud.dataset.get_by_name(db=db, name=csv_file.filename)

    if dataset_db:
        raise HTTPException(
            status_code=400,
            detail="Error, dataset with same filename already uploaded, please rename your dataset"
        )

    csv_reader = reader(iterdecode(csv_file.file, "utf-8-sig"), delimiter=";")

    # Parse the .csv headers
    headers = next(csv_reader)

    possible_column_names = utils.possible_column_names

    # If there are more columns than what is expected, decline the dataset
    if len(headers) > len(possible_column_names):
        raise HTTPException(
            status_code=400,
            detail="Error, dataset has more than {} columns, not supported, please conform to required dataset format".format(len(possible_column_names))
        )

    # if there is no useful information then decline the dataset (see comment #A)
    if len(headers) < 3:
        raise HTTPException(
            status_code=400,
            detail="Error, won't accept dataset with less than 3 columns, please provide more information in the dataset"
        )

    if "date" not in headers or "time" not in headers:
        raise HTTPException(
            status_code=400,
            detail="Error, can't upload dataset with no date or time information"
        )

    # Find usable columns and their place in the file (column sequence)
    usable_column_names = {}

    col_pos = 0
    for col in headers:
        if col in possible_column_names:
            usable_column_names[col] = col_pos
        col_pos = col_pos + 1

    #A
    # Refuse to accept dataset if there is only date+time information.
    # If but one of these is missing, then we have date+something or time+something which is again, useless.
    if len(usable_column_names.items()) < 3:
        raise HTTPException(
            status_code=400,
            detail="Error, .csv contains two or less usable columns, please upload a file with more columns"
        )

    new_dataset = crud.dataset.create(db=db, obj_in=BaseDataset(name=csv_file.filename))

    try:
        rows = await read_rows_csv(csv_reader, new_dataset, usable_column_names)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="leaf_wetness row value is out of bounds, bounds: [0, 1]"
        )

    batch_data = crud.data.batch_insert(db=db, rows=rows)

    if not batch_data:
        crud.dataset.remove(db=db, id=new_dataset.id)

        raise HTTPException(
            status_code=400,
            detail="Unable to create dataset, error with database, please contact repository maintainer"
        )

    return Message(message="Successfully uploaded file.")
