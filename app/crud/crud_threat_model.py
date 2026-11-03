from typing import List, Optional
from uuid import UUID

from fastapi.encoders import jsonable_encoder
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from crud.base import CRUDBase
from models.threat_model import ThreatModel
from schemas.threat_model import ThreatModelCreate, ThreatModelUpdate


class CRUDThreatModel(CRUDBase[ThreatModel, ThreatModelCreate, ThreatModelUpdate]):

    def get_by_crop(self, db: Session, crop_id: UUID) -> List[ThreatModel]:
        return (
            db.query(self.model)
            .filter(self.model.crop_id == crop_id)
            .all()
        )

    def create(self, db: Session, obj_in: ThreatModelCreate, **kwargs) -> Optional[ThreatModel]:
        data = jsonable_encoder(obj_in)
        # definition is a nested Pydantic model — encode to plain dict for JSONB
        data["definition"] = obj_in.definition.model_dump()
        db_obj = self.model(**data)
        db.add(db_obj)
        try:
            db.commit()
        except SQLAlchemyError:
            db.rollback()
            return None
        db.refresh(db_obj)
        return db_obj

    def update(self, db: Session, db_obj: ThreatModel, obj_in, **kwargs) -> Optional[ThreatModel]:
        update_data = (
            obj_in if isinstance(obj_in, dict)
            else obj_in.model_dump(exclude_unset=True)
        )
        if "definition" in update_data and update_data["definition"] is not None:
            # convert nested model to plain dict if it came through as a Pydantic object
            d = update_data["definition"]
            update_data["definition"] = d if isinstance(d, dict) else d.model_dump()
        for field, value in update_data.items():
            setattr(db_obj, field, value)
        db.add(db_obj)
        try:
            db.commit()
        except SQLAlchemyError:
            db.rollback()
            return None
        db.refresh(db_obj)
        return db_obj


threat_model = CRUDThreatModel(ThreatModel)
