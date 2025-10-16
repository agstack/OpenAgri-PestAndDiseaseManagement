from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

import crud
from api import deps
from schemas import Units, UnitCreate, Message, UnitDelete
from crud import unit

router = APIRouter()


@router.post("/", response_model=Message, dependencies=[Depends(deps.get_jwt)])
def create_unit(
        unit_in: UnitCreate,
        db: Session = Depends(deps.get_db)
):
    """
    Creates a user defined unit
    """

    unit_db = crud.unit.get_by_name(db=db, name=unit_in.name)

    if unit_db:
        raise HTTPException(
            status_code=400,
            detail="That unit already exists."
        )

    unit.create(db=db, obj_in=unit_in)

    return Message(message="Successfully created!")


@router.get("/", response_model=Units, dependencies=[Depends(deps.get_jwt)])
def get_units(
        db: Session = Depends(deps.get_db)
):
    """
    Returns a list of symbols that are currently available in the system
    """

    units_db = unit.get_all(db=db)

    return Units(units=units_db)


@router.delete("/", dependencies=[Depends(deps.get_jwt)])
def delete_unit(
        unit_id: UnitDelete,
        db: Session = Depends(deps.get_db)
):
    """
    Delete a unit
    """

    unit_db = crud.unit.get(db=db, id=unit_id.id)

    if not unit_db:
        raise HTTPException(
            status_code=400,
            detail="Can't delete a unit that doesn't exist."
        )

    removed_unit = unit.remove(db=db, id=unit_id.id)

    return Message(message="Successfully removed unit!")
