from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Operator, Unit

from core.config import settings

engine = create_engine(settings.SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

db = SessionLocal()

def init_units():
    db.add(Unit(name="atmospheric_temperature", symbol="celsius"))
    db.add(Unit(name="atmospheric_temperature_daily_min", symbol="celsius"))
    db.add(Unit(name="atmospheric_temperature_daily_max", symbol="celsius"))
    db.add(Unit(name="atmospheric_temperature_daily_average", symbol="celsius"))
    db.add(Unit(name="atmospheric_relative_humidity", symbol="%"))
    db.add(Unit(name="atmospheric_pressure", symbol="mbar"))

    db.add(Unit(name="precipitation", symbol="mm"))

    db.add(Unit(name="average_wind_speed", symbol="km/h"))
    db.add(Unit(name="wind_direction", symbol="N/S/E/W"))
    db.add(Unit(name="wind_gust", symbol="km/h"))

    db.add(Unit(name="leaf_relative_humidity", symbol="%"))
    db.add(Unit(name="leaf_temperature", symbol="celsius"))
    db.add(Unit(name="leaf_wetness", symbol="h"))

    db.add(Unit(name="soil_temperature_10cm", symbol="celsius"))
    db.add(Unit(name="soil_temperature_20cm", symbol="celsius"))
    db.add(Unit(name="soil_temperature_30cm", symbol="celsius"))
    db.add(Unit(name="soil_temperature_40cm", symbol="celsius"))
    db.add(Unit(name="soil_temperature_50cm", symbol="celsius"))
    db.add(Unit(name="soil_temperature_60cm", symbol="celsius"))

    db.add(Unit(name="solar_irradiance_copernicus", symbol="W/m2"))


def init_operators():
    db.add(Operator(symbol=">"))
    db.add(Operator(symbol="<"))
    db.add(Operator(symbol=">="))
    db.add(Operator(symbol="<="))
    db.add(Operator(symbol="=="))
    db.add(Operator(symbol="!="))


def init_db():
    init_units()
    init_operators()

    db.commit()
    db.close()
