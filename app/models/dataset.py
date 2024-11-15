from typing import List

from sqlalchemy import Column, String, Integer

from sqlalchemy.orm import Mapped, relationship

from db.base_class import Base


class Dataset(Base):
    __tablename__ = "dataset"

    id = Column(Integer, primary_key=True, unique=True, nullable=False)
    name = Column(String, nullable=False)

    data: Mapped[List["Data"]] = relationship(back_populates="dataset", cascade="all, delete-orphan")
