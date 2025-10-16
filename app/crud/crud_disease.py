import traceback
from typing import List, Dict, Any, Optional

from fastapi.encoders import jsonable_encoder
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from crud.base import CRUDBase
from models import Disease, GDDInterval
from schemas import CreateDisease, GDDIntervalInput, UpdateDiseaseModel


class CrudDisease(CRUDBase[Disease, CreateDisease, dict]):

    def get_all(self, db: Session):
        response = db.query(Disease).all()

        return response

    def get_by_name(self, db: Session, name: str) -> Disease:
        response = db.query(Disease).filter(Disease.name == name).first()

        return response

    def create_with_gdd_points(self, db: Session, disease_schema: CreateDisease, gdd_points: List[GDDIntervalInput]):
        # Create disease
        obj_in_data = jsonable_encoder(disease_schema)
        disease_db_obj = Disease(**obj_in_data)
        db.add(disease_db_obj)
        try:
            db.commit()
        except SQLAlchemyError:
            traceback.print_exc()
            return None
        db.refresh(disease_db_obj)

        # Create gdd points for disease
        obj_in_data = [jsonable_encoder(x) for x in gdd_points]
        gdd_points_db_obj = [GDDInterval(**x) for x in obj_in_data]
        for gdd_obj in gdd_points_db_obj:
            gdd_obj.disease_id = disease_db_obj.id
        db.add_all(gdd_points_db_obj)
        try:
            db.commit()
        except SQLAlchemyError:
            traceback.print_exc()
            return None

        for gdd_obj in gdd_points_db_obj:
            db.refresh(gdd_obj)

        db.refresh(disease_db_obj)

        return disease_db_obj

    def update_with_gdd_points_overwrite(
            self,
            db: Session,
            db_obj: Disease,
            obj_in: UpdateDiseaseModel | Dict[str, Any]
    ) -> Optional[Any]:
        obj_data = jsonable_encoder(db_obj)
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.model_dump(exclude_unset=True)

        gdd_points = None
        if "gdd_points" in update_data:
            interval_data: list = update_data.pop("gdd_points", [])

            gdd_points = [GDDInterval(**x) for x in interval_data]
            for gdd_point in gdd_points:
                gdd_point.disease_id = db_obj.id

            db.add_all(gdd_points)

        for field in obj_data:
            if field in update_data:
                setattr(db_obj, field, update_data[field])

        if gdd_points:
            setattr(db_obj, "gdd_points", gdd_points)

        db.add(db_obj)
        try:
            db.commit()
        except SQLAlchemyError:
            traceback.print_exc()
            return None

        db.refresh(db_obj)

        return db_obj


disease = CrudDisease(Disease)
