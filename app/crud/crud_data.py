import datetime

from sqlalchemy import and_
from sqlalchemy.orm import Session, Query
from crud.base import CRUDBase
from models import Data
from schemas import CreateData, UpdateData


class CrudData(CRUDBase[Data, CreateData, UpdateData]):

    def get_all(self, db: Session):
        return db.query(Data).all()

    def get_data_interval_query(self, db: Session, from_date: datetime.date, to_date: datetime.date, rule_from_time: datetime.time, rule_to_time: datetime.time) -> Query:
        query = db.query(Data).filter(and_(Data.date <= to_date, Data.date >= from_date, Data.time >= rule_from_time, Data.time <= rule_to_time)).order_by(Data.date.asc(), Data.time.asc()).group_by(Data.id, Data.date)
        return query


data = CrudData(Data)
