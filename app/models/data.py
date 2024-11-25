from sqlalchemy import Column, Integer, Date, Time, String, Float, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base_class import Base


class Data(Base):
    __tablename__ = 'data'
    id = Column(Integer, primary_key=True, unique=True, nullable=False)

    date = Column(Date, nullable=True)
    time = Column(Time, nullable=True)
    parcel_location = Column(String, nullable=True)

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

    leaf_relative_humidity = Column(Float, nullable=True, info={"unit_of_measure": "percentage"})
    leaf_temperature = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})
    leaf_wetness = Column(Float, nullable=True, info={"unit_of_measure": "time-frame"})

    soil_temperature_10cm = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})
    soil_temperature_20cm = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})
    soil_temperature_30cm = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})
    soil_temperature_40cm = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})
    soil_temperature_50cm = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})
    soil_temperature_60cm = Column(Float, nullable=True, info={"unit_of_measure": "celsius"})

    solar_irradiance_copernicus = Column(Float, nullable=True, info={"unit_of_measure": "W/m2"})


    dataset_id : Mapped[int] = mapped_column(ForeignKey("dataset.id"))
    dataset: Mapped["Dataset"] = relationship(back_populates="data")
