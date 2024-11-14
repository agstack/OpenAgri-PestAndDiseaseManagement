from typing import Optional

from pydantic import BaseModel, ConfigDict, UUID4
import datetime


class CreateData(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    date: datetime.date
    time: datetime.time
    nuts3: str
    nuts2: str
    temperature_air: Optional[float]
    relative_humidity: Optional[float]
    precipitation: Optional[float]
    wind_speed: Optional[float]
    wind_direction: Optional[float]
    wind_gust: Optional[float]
    atmospheric_pressure: Optional[float]
    relative_humidity_canopy: Optional[float]
    temperature_canopy: Optional[float]
    solar_irradiance_copernicus: Optional[float]

    dataset_id: UUID4

class UpdateData(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    nuts3: Optional[str]
    nuts2: Optional[str]
    temperature_air: Optional[float]
    relative_humidity: Optional[float]
    precipitation: Optional[float]
    wind_speed: Optional[float]
    wind_direction: Optional[float]
    wind_gust: Optional[float]
    atmospheric_pressure: Optional[float]
    relative_humidity_canopy: Optional[float]
    temperature_canopy: Optional[float]
    solar_irradiance_copernicus: Optional[float]


class DataDB(BaseModel):
    date: datetime.date
    time: datetime.time
    nuts3: str
    nuts2: str
    temperature_air: float
    relative_humidity: float
    precipitation: float
    wind_speed: float
    wind_direction: float
    wind_gust: float
    atmospheric_pressure: float
    relative_humidity_canopy: float
    temperature_canopy: float
    solar_irradiance_copernicus: float
