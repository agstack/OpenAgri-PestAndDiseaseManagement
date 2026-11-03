import requests
from fastapi import HTTPException
from requests import RequestException


class WeatherServiceClient:
    """HTTP client for direct calls to the weather service (no gatekeeper)."""

    def __init__(self, base_url: str):
        self._base = base_url.rstrip("/")

    def _check_response(self, response) -> None:
        if response.status_code == 400:
            raise HTTPException(
                status_code=400,
                detail="Weather service error: {}".format(response.reason),
            )
        if response.status_code == 404:
            raise HTTPException(
                status_code=400,
                detail="Weather service endpoint not found (404)",
            )
        if not response.ok:
            raise HTTPException(
                status_code=400,
                detail="Weather service error: HTTP {}".format(response.status_code),
            )

    def _post(self, path: str, body: dict) -> dict:
        try:
            response = requests.post(
                self._base + path,
                headers={"Content-Type": "application/json"},
                json=body,
            )
        except RequestException:
            raise HTTPException(
                status_code=400,
                detail="Weather service connection error",
            )
        self._check_response(response)
        return response.json()

    def _get(self, path: str, params: dict) -> dict:
        try:
            response = requests.get(
                self._base + path,
                headers={"Content-Type": "application/json"},
                params=params,
            )
        except RequestException:
            raise HTTPException(
                status_code=400,
                detail="Weather service connection error",
            )
        self._check_response(response)
        return response.json()

    def get_hourly_history(
        self,
        lat: float,
        lon: float,
        start_date,
        end_date,
        variables: list,
        radius_km: int = 10,
    ) -> dict:
        return self._post(
            "/api/v1/history/hourly/",
            {
                "lat": lat,
                "lon": lon,
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "variables": variables,
                "radius_km": radius_km,
            },
        )

    def get_daily_history(
        self,
        lat: float,
        lon: float,
        start_date,
        end_date,
        variables: list,
        radius_km: int = 10,
    ) -> dict:
        return self._post(
            "/api/v1/history/daily/",
            {
                "lat": lat,
                "lon": lon,
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "variables": variables,
                "radius_km": radius_km,
            },
        )

    def get_hourly_forecast(
        self,
        lat: float,
        lon: float,
        days: int = 5,
    ) -> dict:
        return self._get(
            "/api/v1/forecast/hourly/",
            {"lat": lat, "lon": lon, "days": days},
        )
