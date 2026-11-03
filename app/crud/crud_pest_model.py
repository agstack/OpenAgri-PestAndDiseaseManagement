from typing import Any, Optional

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, joinedload
from crud.base import CRUDBase

from models import PestModel, Cultivation, Rule, Condition
from schemas import CreatePestModel, UpdatePestModel


class CrudPestModel(CRUDBase[PestModel, CreatePestModel, UpdatePestModel]):

    def get_with_rules(self, db: Session, id: Any) -> Optional[PestModel]:
        return db.query(PestModel).options(
            joinedload(PestModel.rules).joinedload(Rule.conditions).joinedload(Condition.unit),
            joinedload(PestModel.rules).joinedload(Rule.conditions).joinedload(Condition.operator),
        ).filter(PestModel.id == id).first()

    def create(self, db: Session, obj_in: CreatePestModel, **kwargs) -> Optional[PestModel]:

        pest_model_db = PestModel(name=obj_in.name, description=obj_in.description, geo_areas_of_application=obj_in.geo_areas_of_application)

        # Attempt to create a pest model entity
        db.add(pest_model_db)
        try:
            db.commit()
        except SQLAlchemyError:
            db.rollback()
            return None
        db.refresh(pest_model_db)

        # Assemble the cultivations from the list
        cultivations = [Cultivation(name=x, pest_model_id=pest_model_db.id) for x in obj_in.cultivations]

        # Attempt to create the cultivations and bind them to the previously created pest model entity
        db.add_all(cultivations)
        try:
            db.commit()
        except SQLAlchemyError:
            db.rollback()

            db.delete(pest_model_db)
            try:
                db.commit()
            except SQLAlchemyError:
                return None

            return None

        for c in cultivations:
            db.refresh(c)

        db.refresh(pest_model_db)

        return pest_model_db


    def get_all(self, db: Session):
        return db.query(PestModel).all()


pest_model = CrudPestModel(PestModel)
