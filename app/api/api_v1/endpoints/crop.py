from typing import List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

import crud
from api import deps
from schemas.crop import CropCreate, CropDB

router = APIRouter()


@router.get("/", response_model=List[CropDB], dependencies=[Depends(deps.get_jwt)])
def list_crops(db: Session = Depends(deps.get_db)):
    return crud.crop.get_multi(db=db)


@router.post("/", response_model=CropDB, dependencies=[Depends(deps.get_jwt)])
def create_crop(crop_in: CropCreate, db: Session = Depends(deps.get_db)):
    return crud.crop.create(db=db, obj_in=crop_in)


@router.delete("/{crop_id}/", response_model=CropDB, dependencies=[Depends(deps.get_jwt)])
def delete_crop(crop_id: UUID, db: Session = Depends(deps.get_db)):
    obj = crud.crop.get(db=db, id=crop_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Crop not found")
    return crud.crop.remove(db=db, id=crop_id)
