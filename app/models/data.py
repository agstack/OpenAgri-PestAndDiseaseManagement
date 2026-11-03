from sqlalchemy import Column, Integer, Date, Time, String, Float, ForeignKey
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.testing.schema import mapped_column

from db.base_class import Base


class Data(Base):
    __tablename__ = 'data'
    id = Column(Integer, primary_key=True, unique=True, nullable=False)

    date = Column(Date, nullable=True)
    time = Column(Time, nullable=True)

    atmospheric_temperature = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})
    atmospheric_temperature_daily_min = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})
    atmospheric_temperature_daily_max = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})
    atmospheric_temperature_daily_average = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})
    atmospheric_relative_humidity = Column(Float, nullable=True, info={"unit_of_measure": "percentage"})
    atmospheric_pressure = Column(Float, nullable=True, info={"unit_of_measure": "mbar"})

    precipitation = Column(Float, nullable=True, info={"unit_of_measure": "mm"})

    average_wind_speed = Column(Float, nullable=True, info={"unit_of_measure": "km/h"})
    wind_direction = Column(String, nullable=True)
    wind_gust = Column(Float, nullable=True, info={"unit_of_measure": "km/h"})

    # Open meteo does not have these
    leaf_relative_humidity = Column(Float, nullable=True, info={"unit_of_measure": "percentage"})
    leaf_temperature = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})
    leaf_wetness = Column(Float, nullable=True, info={"unit_of_measure": "time-frame"})

    # it has 0-7, 7-28, 28-100, 100-255
    soil_temperature_10cm = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})
    soil_temperature_20cm = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})
    soil_temperature_30cm = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})
    soil_temperature_40cm = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})
    soil_temperature_50cm = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})
    soil_temperature_60cm = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})

    # also these
    solar_irradiance_copernicus = Column(Float, nullable=True, info={"unit_of_measure": "W/m2"})

    parcel_id: Mapped[int] = mapped_column(ForeignKey("parcel.id"), index=True)
    parcel: Mapped["Parcel"] = relationship(back_populates="data")
