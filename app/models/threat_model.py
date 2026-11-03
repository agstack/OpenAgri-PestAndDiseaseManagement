import uuid

from sqlalchemy import String, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base_class import Base


class ThreatModel(Base):
    __tablename__ = "threat_model"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)

    scientific_name: Mapped[str] = mapped_column(String(50))
    common_name: Mapped[str] = mapped_column(String(50))
    label: Mapped[str] = mapped_column(String(50), nullable=True)
    note: Mapped[str] = mapped_column(String(300), nullable=True)
    definition: Mapped[dict] = mapped_column(JSONB())

    crop_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("crop.id"))
    crop: Mapped["Crop"] = relationship(back_populates="threat_models")
