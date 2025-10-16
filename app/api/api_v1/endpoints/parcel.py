from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

import crud
from api import deps
from schemas import Message, CreateParcel, ParcelWKT, Parcels

from shapely import wkt, errors

from utils import fetch_historical_data_for_parcel

router = APIRouter()

@router.get("/", response_model=Parcels, dependencies=[Depends(deps.get_jwt)])
def get_all_parcels(
        db: Session = Depends(deps.get_db)
) -> Parcels:
    """
    Returns a list of all parcels present in the system
    """

    response_obj = Parcels(elements=crud.parcel.get_all(db=db))

    return response_obj

@router.post("/", response_model=Message, dependencies=[Depends(deps.get_jwt)])
def upload_parcel_lat_lon(
        parcel_information: CreateParcel,
        db: Session = Depends(deps.get_db)
) -> Message:
    """
    Upload a set of latitude and longitude values, that represent a parcel location
    """

    parcel_db = crud.parcel.create(db=db, obj_in=parcel_information)

    fetch_historical_data_for_parcel(db=db, parcel=parcel_db)

    response_object = Message(
        message="Successfully uploaded parcel information!"
    )

    return response_object

@router.post("/wkt-format/", response_model=Message, dependencies=[Depends(deps.get_jwt)])
def upload_parcel_wkt(
        parcel_information: ParcelWKT,
        db: Session = Depends(deps.get_db)
) -> Message:
    """
    Upload a WKT polygon, that represents a parcel location (latitude, longitude)

    Example: POLYGON ((25.2 16.2, 16.2 16.15, 17.2 15.2, 20.1 20.1, 25.2 16.2))
    """

    try:
        base_geometry = wkt.loads(parcel_information.wkt_polygon)
    except errors.ShapelyError as se:
        raise HTTPException(
            status_code=400,
            detail="Error during WKT parsing, please check format, floats,"
                   " that it encompasses a closed structure. Specific exception information: [{}]".format(se)
        )

    c_latitude = base_geometry.centroid.x
    c_longitude = base_geometry.centroid.y

    parcel_db = crud.parcel.create(db=db, obj_in=CreateParcel(latitude=c_latitude, longitude=c_longitude, name=parcel_information.name))

    fetch_historical_data_for_parcel(db=db, parcel=parcel_db)

    response_object = Message(
        message="Successfully uploaded parcel information!"
    )

    return response_object

@router.delete("/{parcel_id}/", response_model=Message, dependencies=[Depends(deps.get_jwt)])
def delete_parcel_by_id(
        parcel_id: int,
        db: Session = Depends(deps.get_db)
) -> Message:
    """
    Removes a parcel from the system via ID
    """

    parcel_db = crud.parcel.get(db=db, id=parcel_id)

    if not parcel_db:
        raise HTTPException(
            status_code=400,
            detail="Error, no parcel with ID:{} found.".format(parcel_id)
        )

    crud.parcel.remove(db=db, id=parcel_id)

    response_object = Message(
        message="Successfully removed parcel with ID:{}".format(parcel_id)
    )

    return response_object