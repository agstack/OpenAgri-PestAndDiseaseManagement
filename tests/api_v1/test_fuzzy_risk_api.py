"""
API-level tests for /fuzzy-risk/ endpoints.

Strategy: mount the router in an isolated FastAPI app, override get_db and
get_jwt, and patch heavy dependencies (crud, OpenMeteo, calculate_fuzzy_risk)
so tests run without a real DB or network.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.api.api_v1.endpoints.fuzzy_risk import router as fuzzy_risk_router
from api import deps

# ─── shared test data ────────────────────────────────────────────────────────

ENDPOINT_MODULE = "app.api.api_v1.endpoints.fuzzy_risk"

SAMPLE_RESULTS_DF = pd.DataFrame([{
    "date":            pd.Timestamp("2024-06-01"),
    "scientific_name": "Venturia inaequalis",
    "common_name":     "Apple scab",
    "risk_score":      62.3,
    "risk_class":      "High",
    "detail":          "best_rule=High mu=0.78 streak=3d",
}])

SAMPLE_DAILY_DF = pd.DataFrame([{
    "date":     pd.Timestamp("2024-06-01"),
    "temp_max": 22.0,
    "temp_min": 14.0,
    "humidity": 80.0,
    "rainfall": 3.0,
}])

SAMPLE_HOURLY_DF = pd.DataFrame([{
    "date":                         pd.Timestamp("2024-06-01 12:00:00"),
    "atmospheric_temperature":      22.0,
    "atmospheric_relative_humidity": 80.0,
    "precipitation":                3.0,
    "atmospheric_pressure":         1013.0,
    "average_wind_speed":           5.0,
    "soil_temperature_10cm":        18.0,
    "soil_temperature_20cm":        16.0,
    "soil_temperature_30cm":        15.0,
    "soil_temperature_40cm":        14.0,
}])

CALCULATE_BODY = {
    "parcel_id": 1,
    "from_date": "2024-06-01",
    "to_date":   "2024-06-30",
}

FETCH_BODY = {
    "parcel_id": 1,
    "from_date": "2024-06-01",
    "to_date":   "2024-06-30",
}

FORECAST_BODY = {
    "parcel_id":  1,
    "days_ahead": 7,
}

FC_FORECAST_BODY = {
    "parcel_id":  "550e8400-e29b-41d4-a716-446655440000",
    "days_ahead": 7,
}


# ─── fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_db_session() -> MagicMock:
    return MagicMock(spec=Session)


def _make_parcel() -> MagicMock:
    p = MagicMock()
    p.id        = 1
    p.latitude  = 45.0
    p.longitude = 14.0
    return p


@pytest.fixture
def client(mock_db_session: MagicMock) -> TestClient:
    app = FastAPI()
    app.include_router(fuzzy_risk_router)
    app.dependency_overrides[deps.get_db]  = lambda: mock_db_session
    app.dependency_overrides[deps.get_jwt] = lambda: "token"
    with TestClient(app) as c:
        yield c


@pytest.fixture
def client_fc(mock_db_session: MagicMock) -> TestClient:
    app = FastAPI()
    app.include_router(fuzzy_risk_router)
    app.dependency_overrides[deps.get_db]               = lambda: mock_db_session
    app.dependency_overrides[deps.get_jwt]              = lambda: "token"
    app.dependency_overrides[deps.is_using_gatekeeper]  = lambda: None
    with TestClient(app) as c:
        yield c


# ─── helpers ─────────────────────────────────────────────────────────────────

def _patch_calculate_happy_path(mocker):
    """Patch all heavy deps for /calculate/ with a successful scenario."""
    mock_crud = mocker.patch(f"{ENDPOINT_MODULE}.crud")
    mock_crud.parcel.get.return_value = _make_parcel()
    mock_crud.data.get_data_query_by_parcel_id_and_date_interval.return_value.all.return_value = [MagicMock()]

    mocker.patch(f"{ENDPOINT_MODULE}._weather_rows_to_daily_df", return_value=SAMPLE_DAILY_DF)
    mocker.patch(f"{ENDPOINT_MODULE}._resolve_threat_models",    return_value=[MagicMock()])
    mocker.patch(f"{ENDPOINT_MODULE}.calculate_fuzzy_risk",      return_value=SAMPLE_RESULTS_DF)
    return mock_crud


def _patch_forecast_happy_path(mocker):
    """Patch all heavy deps for /forecast/ with a successful scenario."""
    mock_crud = mocker.patch(f"{ENDPOINT_MODULE}.crud")
    mock_crud.parcel.get.return_value = _make_parcel()

    mocker.patch(f"{ENDPOINT_MODULE}._openmeteo_to_daily_df", return_value=SAMPLE_DAILY_DF)
    mocker.patch(f"{ENDPOINT_MODULE}._resolve_threat_models", return_value=[MagicMock()])
    mocker.patch(f"{ENDPOINT_MODULE}.calculate_fuzzy_risk",   return_value=SAMPLE_RESULTS_DF)
    return mock_crud


def _patch_fetch_happy_path(mocker, fetch_fn: str):
    """Patch all heavy deps for /historical/ or /forecast-fetch/."""
    mock_crud = mocker.patch(f"{ENDPOINT_MODULE}.crud")
    mock_crud.parcel.get.return_value = _make_parcel()

    mocker.patch(f"{ENDPOINT_MODULE}.{fetch_fn}",              return_value=SAMPLE_HOURLY_DF)
    mocker.patch(f"{ENDPOINT_MODULE}._dedupe_and_store_hourly", return_value=1)
    mocker.patch(f"{ENDPOINT_MODULE}._hourly_df_to_daily",      return_value=SAMPLE_DAILY_DF)
    mocker.patch(f"{ENDPOINT_MODULE}._resolve_threat_models",   return_value=[MagicMock()])
    mocker.patch(f"{ENDPOINT_MODULE}.calculate_fuzzy_risk",     return_value=SAMPLE_RESULTS_DF)
    return mock_crud


# ─── /calculate/ ─────────────────────────────────────────────────────────────

class TestCalculateRisk:

    def test_default_format_is_jsonld(self, client, mocker):
        _patch_calculate_happy_path(mocker)
        r = client.post("/calculate/", json=CALCULATE_BODY)
        assert r.status_code == 200
        body = r.json()
        assert "@context" in body
        assert "@graph"   in body

    def test_format_jsonld_explicit(self, client, mocker):
        _patch_calculate_happy_path(mocker)
        r = client.post("/calculate/?format=json-ld", json=CALCULATE_BODY)
        assert r.status_code == 200
        body = r.json()
        assert "@context" in body
        assert "@graph"   in body

    def test_format_json(self, client, mocker):
        _patch_calculate_happy_path(mocker)
        r = client.post("/calculate/?format=json", json=CALCULATE_BODY)
        assert r.status_code == 200
        body = r.json()
        assert isinstance(body, list)
        assert len(body) == 1
        row = body[0]
        assert "date"            in row
        assert "risk_score"      in row
        assert "risk_class"      in row
        assert "scientific_name" in row
        assert "common_name"     in row

    def test_parcel_not_found(self, client, mocker):
        mock_crud = mocker.patch(f"{ENDPOINT_MODULE}.crud")
        mock_crud.parcel.get.return_value = None
        r = client.post("/calculate/", json=CALCULATE_BODY)
        assert r.status_code == 404
        assert "Parcel" in r.json()["detail"]

    def test_no_weather_data(self, client, mocker):
        mock_crud = mocker.patch(f"{ENDPOINT_MODULE}.crud")
        mock_crud.parcel.get.return_value = _make_parcel()
        mock_crud.data.get_data_query_by_parcel_id_and_date_interval.return_value.all.return_value = []
        mocker.patch(f"{ENDPOINT_MODULE}._weather_rows_to_daily_df",
                     return_value=pd.DataFrame())
        mocker.patch(f"{ENDPOINT_MODULE}._resolve_threat_models", return_value=[MagicMock()])
        r = client.post("/calculate/", json=CALCULATE_BODY)
        assert r.status_code == 404
        assert "weather" in r.json()["detail"].lower()

    def test_no_threat_models(self, client, mocker):
        mock_crud = mocker.patch(f"{ENDPOINT_MODULE}.crud")
        mock_crud.parcel.get.return_value = _make_parcel()
        mock_crud.data.get_data_query_by_parcel_id_and_date_interval.return_value.all.return_value = [MagicMock()]
        mocker.patch(f"{ENDPOINT_MODULE}._weather_rows_to_daily_df", return_value=SAMPLE_DAILY_DF)
        mocker.patch(f"{ENDPOINT_MODULE}._resolve_threat_models",    return_value=[])
        r = client.post("/calculate/", json=CALCULATE_BODY)
        assert r.status_code == 404
        assert "threat" in r.json()["detail"].lower()

    def test_invalid_format_value(self, client, mocker):
        _patch_calculate_happy_path(mocker)
        r = client.post("/calculate/?format=xml", json=CALCULATE_BODY)
        assert r.status_code == 422


# ─── /forecast/ ──────────────────────────────────────────────────────────────

class TestForecastRisk:

    def test_default_format_is_jsonld(self, client, mocker):
        _patch_forecast_happy_path(mocker)
        r = client.post("/forecast/", json=FORECAST_BODY)
        assert r.status_code == 200
        body = r.json()
        assert "@context" in body
        assert "@graph"   in body

    def test_format_json(self, client, mocker):
        _patch_forecast_happy_path(mocker)
        r = client.post("/forecast/?format=json", json=FORECAST_BODY)
        assert r.status_code == 200
        body = r.json()
        assert isinstance(body, list)
        assert len(body) == 1

    def test_parcel_not_found(self, client, mocker):
        mock_crud = mocker.patch(f"{ENDPOINT_MODULE}.crud")
        mock_crud.parcel.get.return_value = None
        r = client.post("/forecast/", json=FORECAST_BODY)
        assert r.status_code == 404

    def test_no_threat_models(self, client, mocker):
        mock_crud = mocker.patch(f"{ENDPOINT_MODULE}.crud")
        mock_crud.parcel.get.return_value = _make_parcel()
        mocker.patch(f"{ENDPOINT_MODULE}._openmeteo_to_daily_df", return_value=SAMPLE_DAILY_DF)
        mocker.patch(f"{ENDPOINT_MODULE}._resolve_threat_models", return_value=[])
        r = client.post("/forecast/", json=FORECAST_BODY)
        assert r.status_code == 404

    def test_openmeteo_empty_response(self, client, mocker):
        mock_crud = mocker.patch(f"{ENDPOINT_MODULE}.crud")
        mock_crud.parcel.get.return_value = _make_parcel()
        mocker.patch(f"{ENDPOINT_MODULE}._openmeteo_to_daily_df", return_value=pd.DataFrame())
        r = client.post("/forecast/", json=FORECAST_BODY)
        assert r.status_code == 502


# ─── /historical/ ────────────────────────────────────────────────────────────

class TestHistoricalFetchAndCalculate:

    def test_default_format_is_jsonld(self, client, mocker):
        _patch_fetch_happy_path(mocker, "fetch_archive_hourly_for_range")
        r = client.post("/historical/", json=FETCH_BODY)
        assert r.status_code == 200
        body = r.json()
        assert "@context" in body
        assert "@graph"   in body

    def test_format_jsonld_explicit(self, client, mocker):
        _patch_fetch_happy_path(mocker, "fetch_archive_hourly_for_range")
        r = client.post("/historical/?format=json-ld", json=FETCH_BODY)
        assert r.status_code == 200
        assert "@graph" in r.json()

    def test_format_json(self, client, mocker):
        _patch_fetch_happy_path(mocker, "fetch_archive_hourly_for_range")
        r = client.post("/historical/?format=json", json=FETCH_BODY)
        assert r.status_code == 200
        body = r.json()
        assert isinstance(body, list)
        assert len(body) == 1

    def test_date_validation_from_gt_to(self, client, mocker):
        mocker.patch(f"{ENDPOINT_MODULE}.crud")
        r = client.post("/historical/", json={
            "parcel_id": 1,
            "from_date": "2024-06-30",
            "to_date":   "2024-06-01",
        })
        assert r.status_code == 422

    def test_parcel_not_found(self, client, mocker):
        mock_crud = mocker.patch(f"{ENDPOINT_MODULE}.crud")
        mock_crud.parcel.get.return_value = None
        mocker.patch(f"{ENDPOINT_MODULE}._resolve_threat_models", return_value=[MagicMock()])
        r = client.post("/historical/", json=FETCH_BODY)
        assert r.status_code == 404

    def test_openmeteo_error_raises_502(self, client, mocker):
        mock_crud = mocker.patch(f"{ENDPOINT_MODULE}.crud")
        mock_crud.parcel.get.return_value = _make_parcel()
        mocker.patch(f"{ENDPOINT_MODULE}._resolve_threat_models", return_value=[MagicMock()])
        mocker.patch(f"{ENDPOINT_MODULE}.fetch_archive_hourly_for_range",
                     side_effect=RuntimeError("network error"))
        r = client.post("/historical/", json=FETCH_BODY)
        assert r.status_code == 502
        assert "archive" in r.json()["detail"].lower()

    def test_dedupe_called(self, client, mocker):
        _patch_fetch_happy_path(mocker, "fetch_archive_hourly_for_range")
        spy = mocker.patch(f"{ENDPOINT_MODULE}._dedupe_and_store_hourly", return_value=1)
        client.post("/historical/", json=FETCH_BODY)
        spy.assert_called_once()


# ─── /forecast-fetch/ ────────────────────────────────────────────────────────

class TestForecastFetchAndCalculate:

    def test_default_format_is_jsonld(self, client, mocker):
        _patch_fetch_happy_path(mocker, "fetch_forecast_hourly_for_range")
        r = client.post("/forecast-fetch/", json=FETCH_BODY)
        assert r.status_code == 200
        body = r.json()
        assert "@context" in body
        assert "@graph"   in body

    def test_format_json(self, client, mocker):
        _patch_fetch_happy_path(mocker, "fetch_forecast_hourly_for_range")
        r = client.post("/forecast-fetch/?format=json", json=FETCH_BODY)
        assert r.status_code == 200
        body = r.json()
        assert isinstance(body, list)
        assert len(body) == 1

    def test_date_validation_from_gt_to(self, client, mocker):
        mocker.patch(f"{ENDPOINT_MODULE}.crud")
        r = client.post("/forecast-fetch/", json={
            "parcel_id": 1,
            "from_date": "2024-06-30",
            "to_date":   "2024-06-01",
        })
        assert r.status_code == 422

    def test_parcel_not_found(self, client, mocker):
        mock_crud = mocker.patch(f"{ENDPOINT_MODULE}.crud")
        mock_crud.parcel.get.return_value = None
        mocker.patch(f"{ENDPOINT_MODULE}._resolve_threat_models", return_value=[MagicMock()])
        r = client.post("/forecast-fetch/", json=FETCH_BODY)
        assert r.status_code == 404

    def test_openmeteo_error_raises_502(self, client, mocker):
        mock_crud = mocker.patch(f"{ENDPOINT_MODULE}.crud")
        mock_crud.parcel.get.return_value = _make_parcel()
        mocker.patch(f"{ENDPOINT_MODULE}._resolve_threat_models", return_value=[MagicMock()])
        mocker.patch(f"{ENDPOINT_MODULE}.fetch_forecast_hourly_for_range",
                     side_effect=RuntimeError("timeout"))
        r = client.post("/forecast-fetch/", json=FETCH_BODY)
        assert r.status_code == 502
        assert "forecast" in r.json()["detail"].lower()

    def test_dedupe_called(self, client, mocker):
        _patch_fetch_happy_path(mocker, "fetch_forecast_hourly_for_range")
        spy = mocker.patch(f"{ENDPOINT_MODULE}._dedupe_and_store_hourly", return_value=1)
        client.post("/forecast-fetch/", json=FETCH_BODY)
        spy.assert_called_once()


# ─── /fc/forecast/ ───────────────────────────────────────────────────────────

def _patch_fc_forecast_happy_path(mocker):
    mocker.patch(f"{ENDPOINT_MODULE}.fetch_parcel_by_id",
                 return_value={"location": {"lat": 45.0, "long": 14.0}})
    mocker.patch(f"{ENDPOINT_MODULE}.fetch_parcel_lat_lon", return_value=(45.0, 14.0))
    mocker.patch(f"{ENDPOINT_MODULE}._openmeteo_to_daily_df", return_value=SAMPLE_DAILY_DF)
    mocker.patch(f"{ENDPOINT_MODULE}._resolve_threat_models", return_value=[MagicMock()])
    mocker.patch(f"{ENDPOINT_MODULE}.calculate_fuzzy_risk",   return_value=SAMPLE_RESULTS_DF)


class TestForecastRiskFc:

    def test_default_format_is_jsonld(self, client_fc, mocker):
        _patch_fc_forecast_happy_path(mocker)
        r = client_fc.post("/fc/forecast/", json=FC_FORECAST_BODY)
        assert r.status_code == 200
        body = r.json()
        assert "@context" in body
        assert "@graph"   in body

    def test_format_json(self, client_fc, mocker):
        _patch_fc_forecast_happy_path(mocker)
        r = client_fc.post("/fc/forecast/?format=json", json=FC_FORECAST_BODY)
        assert r.status_code == 200
        body = r.json()
        assert isinstance(body, list)
        assert len(body) == 1
        row = body[0]
        assert "date"            in row
        assert "risk_score"      in row
        assert "risk_class"      in row
        assert "scientific_name" in row
        assert "common_name"     in row

    def test_fc_parcel_not_found(self, client_fc, mocker):
        mocker.patch(f"{ENDPOINT_MODULE}.fetch_parcel_by_id", return_value=None)
        r = client_fc.post("/fc/forecast/", json=FC_FORECAST_BODY)
        assert r.status_code == 404
        assert "Parcel" in r.json()["detail"]

    def test_openmeteo_empty_response(self, client_fc, mocker):
        mocker.patch(f"{ENDPOINT_MODULE}.fetch_parcel_by_id",
                     return_value={"location": {"lat": 45.0, "long": 14.0}})
        mocker.patch(f"{ENDPOINT_MODULE}.fetch_parcel_lat_lon", return_value=(45.0, 14.0))
        mocker.patch(f"{ENDPOINT_MODULE}._openmeteo_to_daily_df", return_value=pd.DataFrame())
        r = client_fc.post("/fc/forecast/", json=FC_FORECAST_BODY)
        assert r.status_code == 502

    def test_no_threat_models(self, client_fc, mocker):
        mocker.patch(f"{ENDPOINT_MODULE}.fetch_parcel_by_id",
                     return_value={"location": {"lat": 45.0, "long": 14.0}})
        mocker.patch(f"{ENDPOINT_MODULE}.fetch_parcel_lat_lon", return_value=(45.0, 14.0))
        mocker.patch(f"{ENDPOINT_MODULE}._openmeteo_to_daily_df", return_value=SAMPLE_DAILY_DF)
        mocker.patch(f"{ENDPOINT_MODULE}._resolve_threat_models", return_value=[])
        r = client_fc.post("/fc/forecast/", json=FC_FORECAST_BODY)
        assert r.status_code == 404
        assert "threat" in r.json()["detail"].lower()

    def test_gatekeeper_required(self, client, mocker):
        # client does NOT override is_using_gatekeeper — should get 400
        _patch_fc_forecast_happy_path(mocker)
        r = client.post("/fc/forecast/", json=FC_FORECAST_BODY)
        assert r.status_code == 400
