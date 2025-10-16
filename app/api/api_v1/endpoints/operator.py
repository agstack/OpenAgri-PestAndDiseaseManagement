from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

import crud
from api import deps
from schemas import Operators

router = APIRouter()

@router.get("/", response_model=Operators, dependencies=[Depends(deps.get_jwt)])
def get_all_operators(
        db: Session = Depends(deps.get_db)
) -> Operators:
    """
    Returns all operators that can be used.
    """

    operators_db = crud.operator.get_all(db=db)

    return Operators(operators=operators_db)
