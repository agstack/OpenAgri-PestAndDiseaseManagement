import uuid
from typing import List

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base_class import Base


class Crop(Base):
    __tablename__ = "crop"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100))
    description: Mapped[str] = mapped_column(String(500), nullable=True)

    threat_models: Mapped[List["ThreatModel"]] = relationship(
        back_populates="crop", cascade="all, delete-orphan"
    )
