from typing import Optional

from pydantic import BaseModel, ConfigDict
import datetime


class CreateData(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    date: datetime.date
    time: datetime.time

    parcel_location: Optional[str]

    atmospheric_temperature: Optional[float]
    atmospheric_temperature_daily_min: Optional[float]
    atmospheric_temperature_daily_max: Optional[float]
    atmospheric_temperature_daily_average: Optional[float]
    atmospheric_relative_humidity: Optional[float]
    atmospheric_pressure: Optional[float]

    precipitation: Optional[float]

    average_wind_speed: Optional[float]
    wind_direction: Optional[str]
    wind_gust: Optional[float]

    leaf_relative_humidity: Optional[float]
    leaf_temperature: Optional[float]
    leaf_wetness: Optional[float]

    soil_temperature_10cm: Optional[float]
    soil_temperature_20cm: Optional[float]
    soil_temperature_30cm: Optional[float]
    soil_temperature_40cm: Optional[float]
    soil_temperature_50cm: Optional[float]
    soil_temperature_60cm: Optional[float]

    solar_irradiance_copernicus: Optional[float]

    dataset_id: int
