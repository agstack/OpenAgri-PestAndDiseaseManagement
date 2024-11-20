from typing import Optional

from sqlalchemy.orm import Session

from crud.base import CRUDBase
from models import Dataset
from schemas import BaseDataset


class CrudDataset(CRUDBase[Dataset, BaseDataset, BaseDataset]):

    def get_by_name(self, db: Session, name: str) -> Optional[Dataset]:
        dataset_db = db.query(Dataset).filter(Dataset.name == name).first()

        return dataset_db



dataset = CrudDataset(Dataset)
