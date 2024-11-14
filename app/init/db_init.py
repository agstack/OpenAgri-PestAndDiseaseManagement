from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Operator, Unit

from core.config import settings

engine = create_engine(settings.SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

db = SessionLocal()

def init_units():
    db.add(Unit(name="temperature_air", symbol="Celsius"))
    db.add(Unit(name="relative_humidity", symbol="%"))
    db.add(Unit(name="precipitation", symbol="mm"))
    db.add(Unit(name="wind_speed", symbol="km/h"))
    db.add(Unit(name="wind_gust", symbol="km/h"))
    db.add(Unit(name="atmospheric_pressure", symbol="mbar"))
    db.add(Unit(name="relative_humidity_canopy", symbol="%"))
    db.add(Unit(name="temperature_canopy", symbol="°C"))
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
