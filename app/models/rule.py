from typing import List, Optional

from sqlalchemy import Column, Integer, String, TIME, ForeignKey
from sqlalchemy.orm import Mapped, relationship, mapped_column

from db.base_class import Base


class Rule(Base):
    __tablename__ = "rule"
    id = Column(Integer, primary_key=True, unique=True, nullable=False)

    name = Column(String)
    description = Column(String, nullable=True)
    from_time = Column(TIME, nullable=True)
    to_time = Column(TIME, nullable=True)
    probability_value = Column(String, nullable=True, default="Low")

    conditions: Mapped[List["Condition"]] = relationship(back_populates="rule", cascade="all,delete")

    # Optional because the rule may not be a part of any pest model
    pest_model_id: Mapped[Optional[int]] = mapped_column(ForeignKey("pest.id"))
    pest_model: Mapped[Optional["PestModel"]] = relationship(back_populates="rules")
