from typing import List

from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import UUID
from uuid import uuid4
from sqlalchemy.orm import Mapped, relationship

from db.base_class import Base

class PestModel(Base):
    __tablename__ = "pest"

    id = Column(UUID(as_uuid=True), primary_key=True, unique=True, nullable=False, default=uuid4)

    name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    geo_areas_of_application = Column(String, nullable=True)

    cultivations : Mapped[List["Cultivation"]] = relationship(back_populates="pest_model")

    rules: Mapped[List["Rule"]] = relationship(back_populates="pest_model")
