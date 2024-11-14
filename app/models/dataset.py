from typing import List

from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import UUID

from uuid import uuid4

from sqlalchemy.orm import Mapped, relationship

from db.base_class import Base


class Dataset(Base):
    __tablename__ = "dataset"

    id = Column(UUID(as_uuid=True), primary_key=True, unique=True, nullable=False, default=uuid4)
    name = Column(String, nullable=False)

    data: Mapped[List["Data"]] = relationship(back_populates="dataset")
