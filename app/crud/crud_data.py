import datetime
from typing import List, Optional
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from crud.base import CRUDBase
from models import Data
from schemas import CreateData, Temp


class CrudData(CRUDBase[Data, CreateData, dict]):

    def get_all(self, db: Session):
        return db.query(Data).all()

    def batch_insert(self, db: Session, list_of_data: List[Temp], parcel_id: int) -> Optional[List[Data]]:
        data_model = [Data(**x.model_dump(), parcel_id=parcel_id) for x in list_of_data]

        db.add_all(data_model)
        try:
            db.commit()
        except SQLAlchemyError:
            db.rollback()
            return None

        return data_model

    def get_data_query_by_parcel_id(self, db: Session, parcel_id: int):
        return db.query(Data).filter(Data.parcel_id == parcel_id).order_by(Data.date.asc(), Data.time.asc())

    def get_data_query_by_parcel_id_and_date_interval(self, db: Session, parcel_id: int, date_from: datetime.date,
                                                      date_to: datetime.date):
        return db.query(Data).filter(
            Data.parcel_id == parcel_id,
            Data.date.between(date_from, date_to)
        ).order_by(Data.date.asc(), Data.time.asc())

    def get_data_by_parcel_id_and_date(self, db: Session, parcel_id: int, date: datetime.date, time: datetime.time):
        return db.query(Data).filter(Data.parcel_id == parcel_id, Data.date == date, Data.time == time).first()

    def get_data_by_parcel_id_and_date_interval(self, db: Session, parcel_id: int,
                                                start: datetime.date, end: datetime.date,
                                                skip: int = 0, limit: int = 500):
        return db.query(Data).filter(
            Data.parcel_id == parcel_id, Data.date >= start, Data.date <= end
        ).order_by(Data.date.asc(), Data.time.asc()).offset(skip).limit(limit).all()


data = CrudData(Data)