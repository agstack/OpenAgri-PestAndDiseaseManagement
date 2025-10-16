import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

import utils
from api import deps
from schemas import list_path_param, DatasetIds

import crud

router = APIRouter()


@router.get("/calculate-risk-index/weather/{parcel_id}/model/{model_ids}/verbose/{from_date}/from/{to_date}/to/", dependencies=[Depends(deps.get_jwt)])
def calculate_risk_index_verbose(
    from_date: datetime.date,
    to_date: datetime.date,
    parcel_id: int,
    model_ids: DatasetIds = Depends(list_path_param),
    db: Session = Depends(deps.get_db)
):
    """
    Calculates the risk index for some parcel x, a period of time (i.e. 2025-01-01 -> 2025-01-31) and a pest model.
    """

    if from_date > to_date:
        raise HTTPException(
            status_code=400,
            detail="Error, from_date is later than to_date, please swap them."
        )

    # Check ids
    parcel_db = crud.parcel.get(db=db, id=parcel_id)

    if not parcel_db:
        raise HTTPException(
            status_code=400,
            detail="Error, parcel with ID:{} does not exist".format(parcel_id)
        )

    pest_models = []

    for mid in model_ids.ids:
        pest_model_db = crud.pest_model.get(db=db, id=mid)

        if not pest_model_db:
            raise HTTPException(
                status_code=400,
                detail="Error, pest model with ID:{} does not exist".format(mid)
            )

        pest_models.append(pest_model_db)

    calculations = utils.calculate_risk_index_probability(db=db, parcel=parcel_db, pest_models=pest_models,
                                                          from_date=from_date, to_date=to_date)

    return calculations

@router.get("/calculate-risk-index/weather/{parcel_id}/model/{model_ids}/high/{from_date}/from/{to_date}/to/", dependencies=[Depends(deps.get_jwt)])
def calculate_risk_index_high(
    from_date: datetime.date,
    to_date: datetime.date,
    parcel_id: int,
    model_ids: DatasetIds = Depends(list_path_param),
    db: Session = Depends(deps.get_db)
):
    """
    Calculates the risk index for some parcel x, a period of time (i.e. 2025-01-01 -> 2025-01-31) and a pest model.

    This API only returns "High" risk datapoints
    """

    if from_date > to_date:
        raise HTTPException(
            status_code=400,
            detail="Error, from_date is later than to_date, please swap them."
        )

    # Check ids
    parcel_db = crud.parcel.get(db=db, id=parcel_id)

    if not parcel_db:
        raise HTTPException(
            status_code=400,
            detail="Error, parcel with ID:{} does not exist".format(parcel_id)
        )

    pest_models = []

    for mid in model_ids.ids:
        pest_model_db = crud.pest_model.get(db=db, id=mid)

        if not pest_model_db:
            raise HTTPException(
                status_code=400,
                detail="Error, pest model with ID:{} does not exist".format(mid)
            )

        pest_models.append(pest_model_db)

    calculations = utils.calculate_risk_index_probability(db=db, parcel=parcel_db, pest_models=pest_models,
                                                          from_date=from_date, to_date=to_date, parameter="high")

    return calculations


@router.get("/calculate-gdd/parcel/{parcel_id}/model/{model_ids}/verbose/{from_date}/from/{to_date}/to/", dependencies=[Depends(deps.get_jwt)])
def calculate_gdd(
    from_date: datetime.date,
    to_date: datetime.date,
    parcel_id: int,
    model_ids: DatasetIds = Depends(list_path_param),
    db: Session = Depends(deps.get_db)
):
    """
    Calculates and returns GDD for pest in parcel and date interval
    """

    if from_date > to_date:
        raise HTTPException(
            status_code=400,
            detail="from_date must be later than to_date, from_date: {} | to_date: {}".format(from_date, to_date)
        )

    parcel_db = crud.parcel.get(db=db, id=parcel_id)

    if not parcel_db:
        raise HTTPException(
            status_code=400,
            detail="Error, parcel with ID {} does not exist".format(parcel_id)
        )

    disease_models_db = []
    for disease_id in model_ids.ids:
        disease_model_db = crud.disease.get(db=db, id=disease_id)
        if not disease_model_db:
            raise HTTPException(
                status_code=400,
                detail="Error, model with ID {} does not exist".format(disease_id)
            )

        disease_models_db.append(disease_model_db)

    response = utils.calculate_gdd(db=db, parcel=parcel_db,
                                          disease_models=disease_models_db, start=from_date, end=to_date)

    if not response:
        raise HTTPException(
            status_code=400,
            detail=f"Error, no existing data for provided date interval: from:{from_date.isoformat()} to:{to_date.isoformat()}"
        )

    return response

