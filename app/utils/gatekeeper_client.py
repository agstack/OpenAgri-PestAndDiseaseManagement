from typing import Optional

import requests
from fastapi import HTTPException
from requests import RequestException


class GatekeeperClient:
    """Central HTTP client for all communication with the gatekeeper service."""

    def __init__(self, base_url: str):
        self._base = base_url.rstrip("/")

    def _post(self, path: str, body: dict, access_token: Optional[str] = None):
        headers = {"Content-Type": "application/json"}
        if access_token:
            headers["Authorization"] = "Bearer {}".format(access_token)
        try:
            return requests.post(self._base + path, headers=headers, json=body)
        except RequestException as exc:
            raise HTTPException(
                status_code=400,
                detail="Error, can't connect to gatekeeper instance [{}]".format(exc),
            )

    def _get(self, path: str, access_token: Optional[str] = None, params: Optional[dict] = None):
        headers = {"Content-Type": "application/json"}
        if access_token:
            headers["Authorization"] = "Bearer {}".format(access_token)
        try:
            return requests.get(self._base + path, headers=headers, params=params)
        except RequestException:
            raise HTTPException(
                status_code=400,
                detail="Error during proxy call via gk",
            )

    # ── auth ─────────────────────────────────────────────────────────────────

    def login(self, username: str, password: str) -> dict:
        response = self._post("/api/login/", {"username": username, "password": password})
        if response.status_code == 401:
            raise HTTPException(
                status_code=400,
                detail="Error, no active account found with these credentials",
            )
        if response.status_code == 400:
            raise HTTPException(
                status_code=400,
                detail="Error, missing username/password values, please enter your username and/or password",
            )
        return response.json()

    def logout(self, refresh_token: str) -> None:
        response = self._post("/api/logout/", {"refresh": refresh_token})
        if response.status_code == 400:
            raise HTTPException(status_code=400, detail="Error, missing refresh token")
        if response.status_code == 500:
            raise HTTPException(status_code=400, detail="Error, gatekeeper returned a 500!")

    def validate_token(self, token: str, token_type: str) -> bool:
        response = self._post("/api/validate_token/", {"token": token, "token_type": token_type})
        if response.status_code == 400:
            response_json = response.json()
            if "error" in response_json and response_json["error"] == "Token is required":
                raise HTTPException(status_code=400, detail="Error, missing token")
            return False
        if response.status_code == 500:
            raise HTTPException(status_code=400, detail="Error, gatekeeper returned a 500")
        return True

    def register_user(self, username: str, email: str, password: str) -> None:
        response = self._post("/api/register/", {"username": username, "email": email, "password": password})
        if response.status_code // 100 != 2:
            raise HTTPException(status_code=400, detail="Error, gatekeeper raise issue with request.")

    def register_service(self, access_token: str, payload: dict) -> None:
        self._post("/api/register_service/", payload, access_token=access_token)

    # ── proxied resources ─────────────────────────────────────────────────────

    def get_parcel(self, access_token: str, parcel_id: str) -> Optional[dict]:
        response = self._get(
            "/api/proxy/farmcalendar/api/v1/FarmParcels/{}/?format=json".format(parcel_id),
            access_token=access_token,
        )
        if response.status_code == 404:
            return None
        return response.json()

    def get_weather_history(
        self,
        access_token: str,
        lat: float,
        lon: float,
        start_date,
        end_date,
        variables: list,
        radius_km: int = 10,
        how_often: str = "daily",
    ) -> dict:
        response = self._post(
            "/api/proxy/weather_data/api/v1/history/{}/".format(how_often),
            {
                "lat": lat,
                "lon": lon,
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "variables": variables,
                "radius_km": radius_km,
            },
            access_token=access_token,
        )
        if response.status_code == 400:
            raise HTTPException(
                status_code=400,
                detail="Error during weather data api call, original error: {}".format(response.reason),
            )
        if response.status_code == 404:
            raise HTTPException(
                status_code=400,
                detail="Error, GK returning 404, Weather Data API missing.",
            )
        if not response.ok:
            raise HTTPException(
                status_code=400,
                detail="Weather service error: {}".format(response.status_code),
            )
        return response.json()

    def get_weather_forecast(
        self,
        access_token: Optional[str],
        lat: float,
        lon: float,
    ) -> dict:
        response = self._get(
            "/api/proxy/weather_data/api/data/forecast5/",
            access_token=access_token,
            params={"lat": lat, "lon": lon},
        )
        if response.status_code == 400:
            raise HTTPException(
                status_code=400,
                detail="Error during weather data api call, original error: {}".format(response.reason),
            )
        if response.status_code == 404:
            raise HTTPException(
                status_code=400,
                detail="Error, GK returning 404, Weather Data API missing.",
            )
        if not response.ok:
            raise HTTPException(
                status_code=400,
                detail="Weather service error: {}".format(response.status_code),
            )
        return response.json()
