import datetime

from fastapi import APIRouter, Depends, File, UploadFile, HTTPException
from sqlalchemy.orm import Session

import crud
from api import deps
from models import User
from schemas import Message, CreateData, CreateDataset

from csv import reader
from codecs import iterdecode

router = APIRouter()


@router.post("/upload/", response_model=Message)
async def upload(
        csv_file: UploadFile = File(...),
        db: Session = Depends(deps.get_db),
        user: User = Depends(deps.get_current_user)
) -> Message:
    """
    Upload a .csv file (Using the ploutos .csv provided as how a baseline file should be formatted)
    """

    dataset_db = crud.dataset.get_by_name(db=db, name=csv_file.filename)

    if dataset_db:
        raise HTTPException(
            status_code=400,
            detail="Error, dataset with same name already uploaded, please rename your dataset"
        )

    new_dataset = crud.dataset.create(db=db, obj_in=CreateDataset(name=csv_file.filename))

    csv_reader = reader(iterdecode(csv_file.file, "utf-8-sig"), delimiter=";")
    # Skips the first row (column definitions) of the file
    next(csv_reader)

    rows = []
    for row in csv_reader:

        if len(row) < 14:
            raise HTTPException(
                status_code=400,
                detail="Error during upload, file does not conform to standard (missing columns), please format accordingly."
            )

        # Might raise exception due to wrong data in wrong columns (swapped date and float format, causes exception in strptime)
        try:
            obj_in = CreateData(
                date=datetime.datetime.strptime(row[0], "%Y-%m-%d"),
                time=datetime.datetime.strptime(row[1], "%H:%M:%S").time(),
                nuts3=row[2],
                nuts2=row[3],
                temperature_air=float(row[4].replace(",", ".")) if row[4] != "" else None,
                relative_humidity=float(row[5].replace(",", ".")) if row[5] != "" else None,
                precipitation=float(row[6].replace(",", ".")) if row[6] != "" else None,
                wind_speed=float(row[7].replace(",", ".")) if row[7] != "" else None,
                wind_direction=float(row[8].replace(",", ".")) if row[8] != "" else None,
                wind_gust=float(row[9].replace(",", ".")) if row[9] != "" else None,
                atmospheric_pressure=float(row[10].replace(",", ".")) if row[10] != "" else None,
                relative_humidity_canopy=float(row[11].replace(",", ".")) if row[11] != "" else None,
                temperature_canopy=float(row[12].replace(",", ".")) if row[12] != "" else None,
                solar_irradiance_copernicus=float(row[13].replace(",", ".")) if row[13] != "" else None,
                dataset_id=new_dataset.id
            )
        except Exception:
            crud.dataset.remove(db=db, id=new_dataset.id)

            raise HTTPException(
                status_code=400,
                detail="Error when parsing row, sequence of data is wrong, row in question ({})".format(row)
            )

        rows.append(obj_in)

    batch_data = crud.data.batch_insert(db=db, rows=rows)

    if not batch_data:
        crud.dataset.remove(db=db, id=new_dataset.id)

        raise HTTPException(
            status_code=400,
            detail="Unable to create dataset, error with database, please contact repository maintainer"
        )

    return Message(message="Successfully uploaded file.")
