from typing import Optional, Any, List

from password_validator import PasswordValidator
from pydantic import AnyHttpUrl, field_validator
from pydantic_settings import BaseSettings
from os import environ


class Settings(BaseSettings):
    CORS_ORIGINS: List[AnyHttpUrl] | List[str] = None

    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int

    SQLALCHEMY_DATABASE_URI: Optional[str] = None

    @field_validator("SQLALCHEMY_DATABASE_URI", mode="before")
    def assemble_db_connection(cls, v: Optional[str], values) -> Any:
        if isinstance(v, str):
            return v

        url = "postgresql://{}:{}@{}:{}/{}".format(
            environ.get("POSTGRES_USER"),
            environ.get("POSTGRES_PASSWORD"),
            environ.get("POSTGRES_HOST"),
            environ.get("POSTGRES_PORT"),
            environ.get("POSTGRES_DB")
        )

        return url

    PASSWORD_SCHEMA_OBJ: PasswordValidator = PasswordValidator()
    PASSWORD_SCHEMA_OBJ \
        .min(8) \
        .max(100) \
        .has().uppercase() \
        .has().lowercase() \
        .has().digits() \
        .has().no().spaces() \

    ACCESS_TOKEN_EXPIRATION_TIME: int
    REFRESH_TOKEN_EXPIRATION_TIME: int
    JWT_KEY: str
    SERVICE_PORT: int
    JWT_ALGORITHM: str

    # Gatekeeper info
    USING_GATEKEEPER: bool
    GATEKEEPER_BASE_URL: Optional[AnyHttpUrl] = None
    GATEKEEPER_USERNAME: Optional[str] = None
    GATEKEEPER_PASSWORD: Optional[str] = None
    SERVICE_NAME: Optional[str] = None
    LOGGING: str

    OFFLINE_DEPLOYMENT: bool = False
    DISABLE_AUTH: bool = False
    WEATHER_SERVICE_BASE_URL: Optional[AnyHttpUrl] = None
    ENABLE_WEATHER_ENRICHMENT_ON_PARCEL_CREATE: bool = True

    OPEN_METEO_MAX_PAST_DAYS: int = 92
    OPEN_METEO_MIN_PAST_DAYS: int = 1
    OPEN_METEO_MIN_FORECAST_DAYS: int = 1
    OPEN_METEO_MAX_FORECAST_DAYS: int = 16

    # Month when cumulative GDD resets (1 = Jan for NH, 7 = Jul for SH)
    GDD_RESET_MONTH: int = 1


settings = Settings()
