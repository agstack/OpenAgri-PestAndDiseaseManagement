import db.session

from models import Parcel

import openmeteo_requests
import crud

import requests_cache
import pandas as pd
from retry_requests import retry

from datetime import date, timedelta

from schemas import NewCreateData


def get_open_meteo_data():
    # DB Session
    session = db.session.SessionLocal()

    parcels = session.query(Parcel).all()

    if len(parcels) == 0:
        session.close()
        return

    print("got parcels")

    # Set up the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    print("setup openmeteo api")

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": [x.latitude for x in parcels],
        "longitude": [x.longitude for x in parcels],
        "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "rain", "surface_pressure",
                   "wind_speed_10m", "soil_temperature_0cm", "soil_temperature_6cm", "soil_temperature_18cm",
                   "soil_temperature_54cm"],
        "past_days": 1
    }
    responses = openmeteo.weather_api(url, params=params)

    print("got response")

    for parcel, parcel_db in zip(responses, parcels):
        # Process hourly data. The order of variables needs to be the same as requested.
        hourly = parcel.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
        hourly_precipitation = hourly.Variables(2).ValuesAsNumpy()
        hourly_rain = hourly.Variables(3).ValuesAsNumpy()
        hourly_surface_pressure = hourly.Variables(4).ValuesAsNumpy()
        hourly_wind_speed_10m = hourly.Variables(5).ValuesAsNumpy()
        hourly_soil_temperature_0cm = hourly.Variables(6).ValuesAsNumpy()
        hourly_soil_temperature_6cm = hourly.Variables(7).ValuesAsNumpy()
        hourly_soil_temperature_18cm = hourly.Variables(8).ValuesAsNumpy()
        hourly_soil_temperature_54cm = hourly.Variables(9).ValuesAsNumpy()

        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=False),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=False),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            )
        }

        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
        hourly_data["precipitation"] = hourly_precipitation
        hourly_data["rain"] = hourly_rain
        hourly_data["surface_pressure"] = hourly_surface_pressure
        hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
        hourly_data["soil_temperature_0cm"] = hourly_soil_temperature_0cm
        hourly_data["soil_temperature_6cm"] = hourly_soil_temperature_6cm
        hourly_data["soil_temperature_18cm"] = hourly_soil_temperature_18cm
        hourly_data["soil_temperature_54cm"] = hourly_soil_temperature_54cm

        hourly_dataframe = pd.DataFrame(data=hourly_data)

        hourly_dataframe["date"]=hourly_dataframe["date"].astype(str)

        hourly_dataframe = hourly_dataframe[(hourly_dataframe["date"] < date.today().strftime("%Y-%m-%d")) & (hourly_dataframe["date"] >= (date.today() - timedelta(days=1)).strftime("%Y-%m-%d"))]

        print("formatted df, getting ready for batch insert")

        print(hourly_dataframe.head(160).to_string())

        for x in hourly_dataframe.itertuples():
            print(x)

        # check whether data already present (to dodge duplicates)
        if crud.data.get_data_by_parcel_id_and_date(db=session, parcel_id=parcel_db.id, date=hourly_dataframe.iloc[0]["date"].split(" ")[0], time=hourly_dataframe.iloc[0]["date"].split(" ")[1]):
            print("data already present, dumping call")
            continue

        crud.data.batch_insert(
            db=session,
            list_of_data=[
                NewCreateData(
                    date=x[1].split(" ")[0],
                    time=x[1].split(" ")[1],
                    atmospheric_temperature=x[2],
                    atmospheric_relative_humidity=x[3],
                    atmospheric_pressure=x[6],
                    precipitation=x[4],
                    average_wind_speed=x[7],
                    soil_temperature_10cm=x[8],
                    soil_temperature_20cm=x[9],
                    soil_temperature_30cm=x[10],
                    soil_temperature_40cm=x[11]
                ) for x in hourly_dataframe.itertuples()
            ],
            parcel_id=parcel_db.id
        )

        print("done with this batch: {}".format(date.today()))

    openmeteo.session.close()
    session.close()

if __name__ == "__main__":
    get_open_meteo_data()
