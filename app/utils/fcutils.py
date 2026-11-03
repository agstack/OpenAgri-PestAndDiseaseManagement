from fastapi import HTTPException

from core import settings
from utils.gatekeeper_client import GatekeeperClient

from shapely import wkt, errors


def fetch_parcel_by_id(access_token: str, parcel_id: str):
    client = GatekeeperClient(str(settings.GATEKEEPER_BASE_URL))
    return client.get_parcel(access_token, parcel_id)


def fetch_parcel_lat_lon(
        parcel: dict
):

    # First check if the pair (lat,long) exists, we can use these if they do
    try:
        if parcel["location"]["lat"] and parcel["location"]["long"]:
            return parcel["location"]["lat"], parcel["location"]["long"]
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Error, unexpected response from FC, original error: {e}"
        )

    # if (lat,long) is set to (None,None) then calculate the centroid from the WKT object and use that as
    try:
        wkt_polygon = parcel["hasGeometry"]["asWKT"]
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Error, missing location information for parcel, original error: {e}"
        )

    # Parse the POLYGON(...) object
    try:
        base_geometry = wkt.loads(wkt_polygon)
    except errors.ShapelyError as se:
        raise HTTPException(
            status_code=400,
            detail="Error during WKT parsing, provided WKT format has issues: {}".format(se)
        )

    c_latitude = base_geometry.centroid.x
    c_longitude = base_geometry.centroid.y

    return c_latitude, c_longitude
