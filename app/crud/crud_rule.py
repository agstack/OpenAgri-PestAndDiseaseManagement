from sqlalchemy.orm import Session

from crud.base import CRUDBase
from models import Rule
from schemas import CreateRule, UpdateRule


class CrudRule(CRUDBase[Rule, CreateRule, UpdateRule]):

    def get_all(self, db: Session):
        return db.query(Rule).all()


rule = CrudRule(Rule)
