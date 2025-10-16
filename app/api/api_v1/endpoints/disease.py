from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

import crud
from api import deps
from schemas import ListDisease, InputDisease, DiseaseDB, Message, CreateDisease, UpdateDiseaseModel
from pydantic import UUID4

from http import HTTPStatus

router = APIRouter()

@router.get("/", response_model=ListDisease, dependencies=[Depends(deps.get_jwt)])
def get_all_diseases(
        db: Session = Depends(deps.get_db)
) -> ListDisease:
    """
    Returns a list of all diseases
    """

    response = crud.disease.get_all(db=db)

    return ListDisease(diseases=response)

@router.post("/", response_model=DiseaseDB, dependencies=[Depends(deps.get_jwt)])
def create_disease(
        input_obj: InputDisease,
        db: Session = Depends(deps.get_db)
) -> DiseaseDB:
    """
    Create a new disease
    """

    # Check whether start and end are properly assigned
    for gdd_point in input_obj.gdd_points:
        if gdd_point.end < gdd_point.start:
            raise HTTPException(
                status_code=400,
                detail="Error, gdd intervals for growth period must start with lower value "
                       "and end with higher one: {}".format(gdd_point)
            )

        if gdd_point.start < 0 or gdd_point.end < 0:
            raise HTTPException(
                status_code=400,
                detail="Error, negative values for GDD are not allowed"
            )

    disease_db = crud.disease.create_with_gdd_points(
        db=db,
        disease_schema=CreateDisease(
            name=input_obj.name,
            eppo_code=input_obj.eppo_code,
            base_gdd=input_obj.base_gdd,
            description=input_obj.description
        ),
        gdd_points=input_obj.gdd_points
    )

    return disease_db


@router.get("/{disease_name}/", response_model=DiseaseDB, dependencies=[Depends(deps.get_jwt)])
def get_by_name(
        disease_name: str,
        db: Session = Depends(deps.get_db)
) -> DiseaseDB:
    """
    Returns a disease by name, if it exists
    """

    disease_db = crud.disease.get_by_name(db=db, name=disease_name)

    if not disease_db:
        raise HTTPException(
            status_code=400,
            detail="Error, disease with name {} does not exist".format(disease_name)
        )

    return disease_db

@router.delete("/{disease_id}/", response_model=Message, dependencies=[Depends(deps.get_jwt)])
def remove_disease(
        disease_id: UUID4,
        db: Session = Depends(deps.get_db)
) -> Message:
    """
    Remove a disease by ID
    """

    disease_db = crud.disease.get(db=db, id=disease_id)

    if not disease_db:
        raise HTTPException(
            status_code=400,
            detail="Error, disease with ID {} does not exist".format(disease_id)
        )

    crud.disease.remove(db=db, id=disease_id)

    response_obj = Message(message="Successfully removed disease with ID {}".format(disease_id))

    return response_obj

@router.patch("/{disease_id}/", status_code=HTTPStatus.NO_CONTENT, dependencies=[Depends(deps.get_jwt)])
def patch_disease(
        disease_id: UUID4,
        disease_model: UpdateDiseaseModel,
        db: Session = Depends(deps.get_db)
):
    """
    Patch an existing disease by passing values that should be updated
    """

    disease_db = crud.disease.get(db=db, id=disease_id)

    if not disease_db:
        raise HTTPException(
            status_code=400,
            detail="Error, cannot update disease model that doesn't exist."
        )

    disease_unpacked = disease_model.model_dump(exclude_unset=True)

    _ = crud.disease.update_with_gdd_points_overwrite(db=db, db_obj=disease_db, obj_in=disease_unpacked)

    return
