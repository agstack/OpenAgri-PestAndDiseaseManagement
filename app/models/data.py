from sqlalchemy import Column, Integer, Date, Time, String, Float, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base_class import Base


class Data(Base):
    __tablename__ = 'data'
    id = Column(Integer, primary_key=True, unique=True, nullable=False)
    date = Column(Date, nullable=True)
    time = Column(Time, nullable=True)
    nuts3 = Column(String, nullable=True)
    nuts2 = Column(String, nullable=True)
    temperature_air = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})
    relative_humidity = Column(Float, nullable=True, info={"unit_of_measure": "percentage"})
    precipitation = Column(Float, nullable=True, info={"unit_of_measure": "mm"})
    wind_speed = Column(Float, nullable=True, info={"unit_of_measure": "km/h"})
    wind_direction = Column(Float, nullable=True)
    wind_gust = Column(Float, nullable=True, info={"unit_of_measure": "km/h"})
    atmospheric_pressure = Column(Float, nullable=True, info={"unit_of_measure": "mbar"})
    relative_humidity_canopy = Column(Float, nullable=True, info={"unit_of_measure": "percentage"})
    temperature_canopy = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})
    solar_irradiance_copernicus = Column(Float, nullable=True, info={"unit_of_measure": "W/m2"})

    dataset_id : Mapped[int] = mapped_column(ForeignKey("dataset.id"))
    dataset: Mapped["Dataset"] = relationship(back_populates="data")
