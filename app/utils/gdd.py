import datetime
from typing import List

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

import crud
from models import Parcel, Disease
from schemas import GDDResponseChunk, GDDResponse, DiseaseModel

from uuid import uuid4

from utils import context
from .custom_logger import get_logger

logger = get_logger(api_path_name=__name__)

def calculate_gdd(db: Session, parcel: Parcel, disease_models: List[Disease],
                  start: datetime.date, end: datetime.date):
    # SQL query for the data
    data_db = crud.data.get_data_query_by_parcel_id_and_date_interval(db=db, parcel_id=parcel.id,
                                                                      date_from=start, date_to=end)

    df = pd.read_sql(sql=data_db.statement, con=db.bind, parse_dates={"date": "%Y-%m-%d"})

    df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str))

    # swap cols
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]

    df = df.drop(["date", "time", "id"], axis=1)
    df = df.drop(["atmospheric_temperature_daily_min", "atmospheric_temperature_daily_max",
                  "atmospheric_temperature_daily_average",
                  "atmospheric_relative_humidity", "atmospheric_pressure", "precipitation", "average_wind_speed",
                  "wind_direction",
                  "wind_gust", "leaf_relative_humidity", "leaf_temperature", "leaf_wetness", "soil_temperature_10cm",
                  "soil_temperature_20cm",
                  "soil_temperature_30cm", "soil_temperature_40cm", "soil_temperature_50cm", "soil_temperature_60cm",
                  "solar_irradiance_copernicus",
                  "parcel_id"], axis=1)

    try:
        df = df.resample("1D", on="datetime").mean()
    except AttributeError as ae:
        logger.warning(ae)
        return None

    df["atmospheric_temperature"] = df["atmospheric_temperature"].apply(np.ceil)

    graph = []

    for disease_model in disease_models:

        gdd_values = []
        acc_value = 0

        for row in df.itertuples():
            date = row[0].date()
            avg_temp = row[1]

            gdd_to_add = 0
            if avg_temp > disease_model.base_gdd:
                gdd_to_add = avg_temp - disease_model.base_gdd

            acc_value += gdd_to_add

            # Find which descriptor should this gdd chunk take
            descriptor = "No defined descriptor for this amount of gdd"
            for interval in disease_model.gdd_points:
                if acc_value not in range(interval.start, interval.end):
                    continue

                descriptor = interval.descriptor

            gdd_values.append(
                GDDResponseChunk(
                    date=date,
                    gdd_value=gdd_to_add,
                    accumulated_gdd=acc_value,
                    descriptor=descriptor
                )
            )

        has_member_list = []

        for gdv in gdd_values:
            some_uuid = uuid4()
            has_member_list.append(
                {
                    "@id": "urn:openagri:accumulatedGDD1:obs5:{}".format(some_uuid),
                    "@type": "Observation",
                    "phenomenonTime": "{}".format(str(gdv.date)),
                    "hasResult": {
                        "@id": "urn:openagri:accumulatedGGD1:obs4:result:{}".format(some_uuid),
                        "@type": "QuantityValue",
                        "hasValue": "{}".format(gdv.accumulated_gdd),
                        "unit": "http://qudt.org/vocab/unit/DEG_C"
                    },
                    "descriptor": "{}".format(gdv.descriptor)
                }
            )

        some_uuid = uuid4()

        graph.append(
            [
                {
                    "@id": "urn:openagri:growingDegreeDaysForPestCalculation:{}".format(some_uuid),
                    "@type": "ObservationCollection",
                    "description": "The growing degree days calculation for a specific pest during a year, which is "
                                   "the cumulative daily average temperature above zero.",
                    "observedProperty": {
                        "@id": "urn:openagri:growingDegreeDays:op:{}".format(some_uuid),
                        "@type": ["ObservableProperty", "Temperature"],
                    },
                    "hasFeatureOfInterest": {
                        "@id": "urn:openagri:agriPest:foi:{}".format(some_uuid),
                        "@type": ["FeatureOfInterest", "AgriPest"],
                        "name": "{}".format(disease_model.name),
                        "description": "{}".format(disease_model.description),
                        "eppoConcept": "{}".format(disease_model.eppo_code),
                        "hasBaseGrowingDegree": "{}".format(disease_model.base_gdd)
                    },
                    "hasMember": has_member_list
                }
            ]
        )

    final_response = {
        "@context": context,
        "@graph": graph
    }

    return final_response


def calculate_gdd_wd(
        disease_models: List[Disease],
        weather_data: dict
):
    graph = []

    for disease_model in disease_models:
        gdd_values = []
        accumulated_gdd = 0

        for day in weather_data["data"]:

            date = day["date"]
            if not day["values"]["temperature_2m_max"]:
                break

            avg_daily_temp = int(day["values"]["temperature_2m_max"])

            gdd_to_add = 0

            if avg_daily_temp > disease_model.base_gdd:
                gdd_to_add = avg_daily_temp - disease_model.base_gdd

            accumulated_gdd += gdd_to_add

            descriptor = "No defined descriptor for this amount of gdd"
            for interval in disease_model.gdd_points:
                if accumulated_gdd not in range(interval.start, interval.end):
                    continue

                descriptor = interval.descriptor

            gdd_values.append(
                GDDResponseChunk(
                    date=date,
                    gdd_value=int(gdd_to_add),
                    accumulated_gdd=int(accumulated_gdd),
                    descriptor=descriptor
                )
            )

        has_member_list = []

        for gdv in gdd_values:
            some_uuid = uuid4()
            has_member_list.append(
                {
                    "@id": "urn:openagri:accumulatedGDD1:obs5:{}".format(some_uuid),
                    "@type": "Observation",
                    "phenomenonTime": "{}".format(str(gdv.date)),
                    "hasResult": {
                        "@id": "urn:openagri:accumulatedGGD1:obs4:result:{}".format(some_uuid),
                        "@type": "QuantityValue",
                        "hasValue": "{}".format(gdv.accumulated_gdd),
                        "unit": "http://qudt.org/vocab/unit/DEG_C"
                    },
                    "descriptor": "{}".format(gdv.descriptor)
                }
            )

        some_uuid = uuid4()

        graph.append(
            [
                {
                    "@id": "urn:openagri:growingDegreeDaysForPestCalculation:{}".format(some_uuid),
                    "@type": "ObservationCollection",
                    "description": "The growing degree days calculation for a specific pest during a year, which is "
                                   "the cumulative daily average temperature above zero.",
                    "observedProperty": {
                        "@id": "urn:openagri:growingDegreeDays:op:{}".format(some_uuid),
                        "@type": ["ObservableProperty", "Temperature"],
                    },
                    "hasFeatureOfInterest": {
                        "@id": "urn:openagri:agriPest:foi:{}".format(some_uuid),
                        "@type": ["FeatureOfInterest", "AgriPest"],
                        "name": "{}".format(disease_model.name),
                        "description": "{}".format(disease_model.description),
                        "eppoConcept": "{}".format(disease_model.eppo_code),
                        "hasBaseGrowingDegree": "{}".format(disease_model.base_gdd)
                    },
                    "hasMember": has_member_list
                }
            ]
        )

    final_response = {
        "@context": context,
        "@graph": graph,
        "@models": disease_models
    }

    return final_response

def calculate_base(
        disease_models: List[Disease],
        weather_data: dict
):
    response_value = []

    for disease_model in disease_models:
        gdd_values = []
        accumulated_gdd = 0

        for day in weather_data["data"]:

            date = day["date"]
            if not day["values"]["temperature_2m_max"]:
                break

            avg_daily_temp = int(day["values"]["temperature_2m_max"])

            gdd_to_add = 0

            if avg_daily_temp > disease_model.base_gdd:
                gdd_to_add = avg_daily_temp - disease_model.base_gdd

            accumulated_gdd += gdd_to_add

            descriptor = "No defined descriptor for this amount of gdd"
            for interval in disease_model.gdd_points:
                if accumulated_gdd not in range(interval.start, interval.end):
                    continue

                descriptor = interval.descriptor

            gdd_values.append(
                GDDResponseChunk(
                    date=date,
                    gdd_value=int(gdd_to_add),
                    accumulated_gdd=int(accumulated_gdd),
                    descriptor=descriptor
                )
            )

        response_value.append(
            DiseaseModel(
                name=disease_model.name,
                eppo_code=disease_model.eppo_code,
                base_gdd=disease_model.base_gdd,
                description=disease_model.description,
                gdd_points=disease_model.gdd_points,
                gdd_values=gdd_values
            )
        )

    return GDDResponse(models=response_value)
