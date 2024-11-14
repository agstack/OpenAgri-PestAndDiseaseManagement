from uuid import uuid4

from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base_class import Base


class Cultivation(Base):
    __tablename__ = "cultivation"

    id = Column(UUID(as_uuid=True), primary_key=True, unique=True, nullable=False, default=uuid4)

    name = Column(String, nullable=False)

    pest_model_id: Mapped[int] = mapped_column(ForeignKey("pest.id"))
    pest_model: Mapped["PestModel"] = relationship(back_populates="cultivations")
