import datetime

from fastapi import HTTPException

from schemas import CreateData

possible_column_names = [
        "date", "time", "parcel_location", "atmospheric_temperature", "atmospheric_temperature_daily_min",
        "atmospheric_temperature_daily_max", "atmospheric_temperature_daily_average", "atmospheric_relative_humidity",
        "atmospheric_pressure", "precipitation", "average_wind_speed", "wind_direction", "wind_gust",
        "leaf_relative_humidity", "leaf_temperature", "leaf_wetness", "soil_temperature_10cm", "soil_temperature_20cm",
        "soil_temperature_30cm", "soil_temperature_40cm", "soil_temperature_50cm", "soil_temperature_60cm",
        "solar_irradiance_copernicus"
    ]

async def read_rows_csv(csv_reader, new_dataset, usable_column_names):
    rows = []
    for row in csv_reader:
        try:
            leaf_wetness = float(row[usable_column_names["leaf_wetness"]].replace(",",
                                                                                  ".")) if "leaf_wetness" in usable_column_names else None

            if leaf_wetness and (leaf_wetness < 0.0 or leaf_wetness > 1.0):
                raise ValueError

            obj_in = CreateData(
                date=datetime.datetime.strptime(row[usable_column_names["date"]], "%Y-%m-%d"),
                time=datetime.datetime.strptime(row[usable_column_names["time"]], "%H:%M:%S").time(),

                parcel_location=row[
                    usable_column_names["parcel_location"]] if "parcel_location" in usable_column_names else None,

                atmospheric_temperature=float(row[usable_column_names["atmospheric_temperature"]].replace(",",
                                                                                                          ".")) if "atmospheric_temperature" in usable_column_names else None,
                atmospheric_temperature_daily_min=float(
                    row[usable_column_names["atmospheric_temperature_daily_min"]].replace(",",
                                                                                          ".")) if "atmospheric_temperature_daily_min" in usable_column_names else None,
                atmospheric_temperature_daily_max=float(
                    row[usable_column_names["atmospheric_temperature_daily_max"]].replace(",",
                                                                                          ".")) if "atmospheric_temperature_daily_max" in usable_column_names else None,
                atmospheric_temperature_daily_average=float(
                    row[usable_column_names["atmospheric_temperature_daily_average"]].replace(",",
                                                                                              ".")) if "atmospheric_temperature_daily_average" in usable_column_names else None,
                atmospheric_relative_humidity=float(
                    row[usable_column_names["atmospheric_relative_humidity"]].replace(",",
                                                                                      ".")) if "atmospheric_relative_humidity" in usable_column_names else None,
                atmospheric_pressure=float(row[usable_column_names["atmospheric_pressure"]].replace(",",
                                                                                                    ".")) if "atmospheric_pressure" in usable_column_names else None,

                precipitation=float(row[usable_column_names["precipitation"]].replace(",",
                                                                                      ".")) if "precipitation" in usable_column_names else None,

                average_wind_speed=float(row[usable_column_names["average_wind_speed"]].replace(",",
                                                                                                ".")) if "average_wind_speed" in usable_column_names else None,
                wind_direction=row[
                    usable_column_names["wind_direction"]] if "wind_direction" in usable_column_names else None,
                wind_gust=float(row[usable_column_names["wind_gust"]].replace(",",
                                                                              ".")) if "wind_gust" in usable_column_names else None,

                leaf_relative_humidity=float(row[usable_column_names["leaf_relative_humidity"]].replace(",",
                                                                                                        ".")) if "leaf_relative_humidity" in usable_column_names else None,
                leaf_temperature=float(row[usable_column_names["leaf_temperature"]].replace(",",
                                                                                            ".")) if "leaf_temperature" in usable_column_names else None,
                leaf_wetness=leaf_wetness,

                soil_temperature_10cm=float(row[usable_column_names["soil_temperature_10cm"]].replace(",",
                                                                                                      ".")) if "soil_temperature_10cm" in usable_column_names else None,
                soil_temperature_20cm=float(row[usable_column_names["soil_temperature_20cm"]].replace(",",
                                                                                                      ".")) if "soil_temperature_20cm" in usable_column_names else None,
                soil_temperature_30cm=float(row[usable_column_names["soil_temperature_30cm"]].replace(",",
                                                                                                      ".")) if "soil_temperature_30cm" in usable_column_names else None,
                soil_temperature_40cm=float(row[usable_column_names["soil_temperature_40cm"]].replace(",",
                                                                                                      ".")) if "soil_temperature_40cm" in usable_column_names else None,
                soil_temperature_50cm=float(row[usable_column_names["soil_temperature_50cm"]].replace(",",
                                                                                                      ".")) if "soil_temperature_50cm" in usable_column_names else None,
                soil_temperature_60cm=float(row[usable_column_names["soil_temperature_60cm"]].replace(",",
                                                                                                      ".")) if "soil_temperature_60cm" in usable_column_names else None,

                solar_irradiance_copernicus=float(row[usable_column_names[
                    "solar_irradiance_copernicus"]]) if "solar_irradiance_copernicus" in usable_column_names else None,

                dataset_id=new_dataset.id
            )
        except HTTPException:
            raise HTTPException(
                status_code=400,
                detail="Error when parsing row, present leaf_wetness data is out of bounds, bounds: [0, 1], row in question: {}".format(
                    row)
            )
        except Exception:
            raise HTTPException(
                status_code=400,
                detail="Error when parsing row, data format unexpected (might be wrong data in wrong column, number in descriptor), row in question (might be missing date or time as well) ({})".format(
                    row)
            )

        rows.append(obj_in)
    return rows
