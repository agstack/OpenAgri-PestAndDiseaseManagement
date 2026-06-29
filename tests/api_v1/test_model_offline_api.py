"""
Tests for /{model_ids}/risk-index/forecast/weather-service/offline/

Strategy: mount the model router in an isolated FastAPI app, override get_db
and is_offline_deployment, and patch the weather service call and risk calculation
so tests run without a real DB or network.
"""

from __future__ import annotations

import datetime
import uuid
from unittest.mock import MagicMock

import pandas as pd
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.api.api_v1.endpoints.model import router as model_router
from api import deps
from core import settings

ENDPOINT_MODULE = "app.api.api_v1.endpoints.model"

PEST_MODEL_ID = str(uuid.uuid4())
OFFLINE_URL = f"/{PEST_MODEL_ID}/risk-index/forecast/weather-service/offline/"

SAMPLE_WEATHER_DF = pd.DataFrame([{
    "timestamp": pd.Timestamp("2024-06-01T12:00:00"),
    "atmospheric_temperature": 22.0,
    "atmospheric_relative_humidity": 75.0,
    "precipitation": 1.5,
}])

SAMPLE_RESULT_JSONLD = {
    "@context": {},
    "@graph": [{"@id": "urn:openagri:pestInfectationRisk:abc", "@type": ["ObservationCollection"]}],
}

SAMPLE_RESULT_JSON = {
    "result_time": "2024-06-01T10:00:00",
    "models": [
        {
            "name": "TestPest",
            "location": {"lat": 45.0, "lon": 14.0},
            "observations": [{"timestamp": "2024-06-01T12:00:00", "risk": "Low"}],
        }
    ],
}


# ─── helpers ─────────────────────────────────────────────────────────────────

def _make_pest_model(unit_name: str = "atmospheric_temperature") -> MagicMock:
    condition = MagicMock()
    condition.unit.name = unit_name

    rule = MagicMock()
    rule.conditions = [condition]

    pm = MagicMock()
    pm.id = PEST_MODEL_ID
    pm.name = "TestPest"
    pm.rules = [rule]
    return pm


# ─── fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_db_session() -> MagicMock:
    return MagicMock(spec=Session)


@pytest.fixture
def client(mock_db_session: MagicMock) -> TestClient:
    app = FastAPI()
    app.include_router(model_router)
    app.dependency_overrides[deps.get_db] = lambda: mock_db_session
    app.dependency_overrides[deps.is_offline_deployment] = lambda: None
    with TestClient(app) as c:
        yield c


# ─── endpoint tests ───────────────────────────────────────────────────────────

class TestOfflineRiskIndexForecast:

    def test_happy_path(self, client: TestClient, mock_db_session: MagicMock, mocker):
        mocker.patch(f"{ENDPOINT_MODULE}.crud.pest_model.get", return_value=_make_pest_model())
        mocker.patch(
            f"{ENDPOINT_MODULE}.fetch_weather_service_history_weather_data_offline",
            return_value=SAMPLE_WEATHER_DF,
        )
        mocker.patch(
            f"{ENDPOINT_MODULE}.calculate_risk_index_forecast_wd",
            return_value=SAMPLE_RESULT_JSONLD,
        )

        r = client.get(OFFLINE_URL, params={"latitude": 45.0, "longitude": 14.0})

        assert r.status_code == 200
        body = r.json()
        assert "@context" in body
        assert "@graph" in body

    def test_json_format_returns_plain_dict(self, client: TestClient, mock_db_session: MagicMock, mocker):
        mocker.patch(f"{ENDPOINT_MODULE}.crud.pest_model.get", return_value=_make_pest_model())
        mocker.patch(
            f"{ENDPOINT_MODULE}.fetch_weather_service_history_weather_data_offline",
            return_value=SAMPLE_WEATHER_DF,
        )
        mocker.patch(
            f"{ENDPOINT_MODULE}.calculate_risk_index_forecast_wd",
            return_value=SAMPLE_RESULT_JSON,
        )

        r = client.get(OFFLINE_URL, params={"latitude": 45.0, "longitude": 14.0, "formatting": "JSON"})

        assert r.status_code == 200
        body = r.json()
        assert "models" in body
        assert "result_time" in body
        assert "@context" not in body

    def test_blocked_when_flag_false(self, mock_db_session: MagicMock, monkeypatch):
        monkeypatch.setattr(settings, "OFFLINE_DEPLOYMENT", False)

        app = FastAPI()
        app.include_router(model_router)
        app.dependency_overrides[deps.get_db] = lambda: mock_db_session

        with TestClient(app) as c:
            r = c.get(OFFLINE_URL, params={"latitude": 45.0, "longitude": 14.0})

        assert r.status_code == 403
        assert "offline deployment" in r.json()["detail"].lower()

    def test_invalid_pest_model_id(self, client: TestClient, mock_db_session: MagicMock, mocker):
        mocker.patch(f"{ENDPOINT_MODULE}.crud.pest_model.get", return_value=None)

        r = client.get(OFFLINE_URL, params={"latitude": 45.0, "longitude": 14.0})

        assert r.status_code == 400
        assert "does not exist" in r.json()["detail"]

    def test_weather_service_failure(self, client: TestClient, mocker):
        mocker.patch(f"{ENDPOINT_MODULE}.crud.pest_model.get", return_value=_make_pest_model())
        mocker.patch(
            f"{ENDPOINT_MODULE}.fetch_weather_service_history_weather_data_offline",
            side_effect=HTTPException(status_code=400, detail="Error during direct weather service call"),
        )

        r = client.get(OFFLINE_URL, params={"latitude": 45.0, "longitude": 14.0})

        assert r.status_code == 400
        assert "direct weather service call" in r.json()["detail"]

    def test_past_days_too_low(self, client: TestClient, mocker):
        mocker.patch(f"{ENDPOINT_MODULE}.crud.pest_model.get", return_value=_make_pest_model())

        r = client.get(OFFLINE_URL, params={"latitude": 45.0, "longitude": 14.0, "past_days": 0})

        assert r.status_code == 400
        assert "past_days" in r.json()["detail"]

    def test_past_days_too_high(self, client: TestClient, mocker):
        mocker.patch(f"{ENDPOINT_MODULE}.crud.pest_model.get", return_value=_make_pest_model())

        r = client.get(OFFLINE_URL, params={"latitude": 45.0, "longitude": 14.0, "past_days": 93})

        assert r.status_code == 400
        assert "past_days" in r.json()["detail"]

    def test_past_days_default_is_accepted(self, client: TestClient, mocker):
        mocker.patch(f"{ENDPOINT_MODULE}.crud.pest_model.get", return_value=_make_pest_model())
        mocker.patch(
            f"{ENDPOINT_MODULE}.fetch_weather_service_history_weather_data_offline",
            return_value=SAMPLE_WEATHER_DF,
        )
        mocker.patch(
            f"{ENDPOINT_MODULE}.calculate_risk_index_forecast_wd",
            return_value=SAMPLE_RESULT_JSONLD,
        )

        # no past_days param → uses default 30
        r = client.get(OFFLINE_URL, params={"latitude": 45.0, "longitude": 14.0})

        assert r.status_code == 200

    def test_history_called_with_derived_variables(self, client: TestClient, mocker):
        mocker.patch(f"{ENDPOINT_MODULE}.crud.pest_model.get", return_value=_make_pest_model("atmospheric_temperature"))
        mock_fetch = mocker.patch(
            f"{ENDPOINT_MODULE}.fetch_weather_service_history_weather_data_offline",
            return_value=SAMPLE_WEATHER_DF,
        )
        mocker.patch(
            f"{ENDPOINT_MODULE}.calculate_risk_index_forecast_wd",
            return_value=SAMPLE_RESULT_JSONLD,
        )

        client.get(OFFLINE_URL, params={"latitude": 45.0, "longitude": 14.0})

        call_kwargs = mock_fetch.call_args.kwargs
        assert "temperature_2m" in call_kwargs["variables"]

    def test_history_called_with_correct_date_range(self, client: TestClient, mocker):
        mocker.patch(f"{ENDPOINT_MODULE}.crud.pest_model.get", return_value=_make_pest_model())
        mock_fetch = mocker.patch(
            f"{ENDPOINT_MODULE}.fetch_weather_service_history_weather_data_offline",
            return_value=SAMPLE_WEATHER_DF,
        )
        mocker.patch(
            f"{ENDPOINT_MODULE}.calculate_risk_index_forecast_wd",
            return_value=SAMPLE_RESULT_JSONLD,
        )

        client.get(OFFLINE_URL, params={"latitude": 45.0, "longitude": 14.0, "past_days": 30})

        call_kwargs = mock_fetch.call_args.kwargs
        expected_end = datetime.date.today()
        expected_start = expected_end - datetime.timedelta(days=30)
        assert call_kwargs["end_date"] == expected_end
        assert call_kwargs["start_date"] == expected_start

    def test_no_mappable_variables(self, client: TestClient, mocker):
        mocker.patch(
            f"{ENDPOINT_MODULE}.crud.pest_model.get",
            return_value=_make_pest_model("unknown_unit_xyz"),
        )

        r = client.get(OFFLINE_URL, params={"latitude": 45.0, "longitude": 14.0})

        assert r.status_code == 400
        assert "mappable" in r.json()["detail"].lower()


# ─── wdutils unit tests ───────────────────────────────────────────────────────

class TestCalculateRiskIndexForecastWd:

    def test_soil_temperature_column_survives_normalization(self):
        """soil_temperature_10cm has no OWM equivalent — must not be silently dropped."""
        from utils.wdutils import calculate_risk_index_forecast_wd

        condition = MagicMock()
        condition.unit.name = "soil_temperature_10cm"
        condition.operator.symbol = ">"
        condition.value = 10

        rule = MagicMock()
        rule.conditions = [condition]
        rule.probability_value = "High"

        pm = MagicMock()
        pm.name = "SoilPest"
        pm.rules = [rule]

        df = pd.DataFrame([{
            "timestamp": pd.Timestamp("2024-06-01T12:00:00"),
            "soil_temperature_10cm": 15.0,
        }])

        parcel = {"@id": "urn:test", "location": {"lat": 45.0, "long": 14.0}}

        result = calculate_risk_index_forecast_wd(
            df=df, parcel=parcel, pest_models=[pm], formatting="JSON"
        )

        obs = result["models"][0]["observations"][0]
        assert obs["risk"] == "High"

    def test_standard_owm_columns_still_work(self):
        """atmospheric_temperature (has OWM equivalent) must still be evaluated."""
        from utils.wdutils import calculate_risk_index_forecast_wd

        condition = MagicMock()
        condition.unit.name = "atmospheric_temperature"
        condition.operator.symbol = ">"
        condition.value = 20

        rule = MagicMock()
        rule.conditions = [condition]
        rule.probability_value = "Medium"

        pm = MagicMock()
        pm.name = "TempPest"
        pm.rules = [rule]

        df = pd.DataFrame([{
            "timestamp": pd.Timestamp("2024-06-01T12:00:00"),
            "atmospheric_temperature": 25.0,
        }])

        parcel = {"@id": "urn:test", "location": {"lat": 45.0, "long": 14.0}}

        result = calculate_risk_index_forecast_wd(
            df=df, parcel=parcel, pest_models=[pm], formatting="JSON"
        )

        obs = result["models"][0]["observations"][0]
        assert obs["risk"] == "Medium"


class TestFetchWeatherServiceHistoryOffline:

    def test_raises_500_when_url_not_configured(self, monkeypatch):
        from utils.wdutils import fetch_weather_service_history_weather_data_offline
        monkeypatch.setattr(settings, "WEATHER_SERVICE_BASE_URL", None)

        with pytest.raises(HTTPException) as exc_info:
            fetch_weather_service_history_weather_data_offline(
                45.0, 14.0,
                datetime.date(2024, 1, 1), datetime.date(2024, 1, 31),
                ["temperature_2m"],
            )

        assert exc_info.value.status_code == 500
        assert "WEATHER_SERVICE_BASE_URL" in exc_info.value.detail

    def test_raises_400_on_request_exception(self, mocker):
        from utils.wdutils import fetch_weather_service_history_weather_data_offline
        from requests import RequestException
        mocker.patch("utils.weather_service_client.requests.post", side_effect=RequestException())

        with pytest.raises(HTTPException) as exc_info:
            fetch_weather_service_history_weather_data_offline(
                45.0, 14.0,
                datetime.date(2024, 1, 1), datetime.date(2024, 1, 31),
                ["temperature_2m"],
            )

        assert exc_info.value.status_code == 400

    def test_raises_400_on_404(self, mocker):
        from utils.wdutils import fetch_weather_service_history_weather_data_offline
        mock_post = mocker.patch("utils.weather_service_client.requests.post")
        mock_post.return_value.status_code = 404

        with pytest.raises(HTTPException) as exc_info:
            fetch_weather_service_history_weather_data_offline(
                45.0, 14.0,
                datetime.date(2024, 1, 1), datetime.date(2024, 1, 31),
                ["temperature_2m"],
            )

        assert exc_info.value.status_code == 400
        assert "404" in exc_info.value.detail

    def test_raises_400_on_non_ok_response(self, mocker):
        from utils.wdutils import fetch_weather_service_history_weather_data_offline
        mock_post = mocker.patch("utils.weather_service_client.requests.post")
        mock_post.return_value.status_code = 503
        mock_post.return_value.ok = False

        with pytest.raises(HTTPException) as exc_info:
            fetch_weather_service_history_weather_data_offline(
                45.0, 14.0,
                datetime.date(2024, 1, 1), datetime.date(2024, 1, 31),
                ["temperature_2m"],
            )

        assert exc_info.value.status_code == 400

    def test_happy_path_returns_dataframe(self, mocker):
        from utils.wdutils import fetch_weather_service_history_weather_data_offline
        mock_post = mocker.patch("utils.weather_service_client.requests.post")
        mock_post.return_value.status_code = 200
        mock_post.return_value.ok = True
        mock_post.return_value.json.return_value = {
            "location": {"lat": 45.0, "lon": 14.0},
            "data": [
                {
                    "timestamp": "2024-01-01T00:00:00",
                    "values": {"temperature_2m": 20.0, "relative_humidity_2m": 65.0}
                }
            ],
            "source": "openmeteo"
        }

        import pandas as pd
        result = fetch_weather_service_history_weather_data_offline(
            45.0, 14.0,
            datetime.date(2024, 1, 1), datetime.date(2024, 1, 31),
            ["temperature_2m", "relative_humidity_2m"],
        )

        assert isinstance(result, pd.DataFrame)
        assert "timestamp" in result.columns
        assert "atmospheric_temperature" in result.columns
        assert "atmospheric_relative_humidity" in result.columns

    def test_post_body_matches_hourly_query_schema(self, mocker):
        from utils.wdutils import fetch_weather_service_history_weather_data_offline
        mock_post = mocker.patch("utils.weather_service_client.requests.post")
        mock_post.return_value.status_code = 200
        mock_post.return_value.ok = True
        mock_post.return_value.json.return_value = {
            "location": {"lat": 45.0, "lon": 14.0},
            "data": [
                {"timestamp": "2024-01-01T00:00:00", "values": {"temperature_2m": 20.0}}
            ],
            "source": "openmeteo"
        }

        start = datetime.date(2024, 1, 1)
        end = datetime.date(2024, 1, 31)
        fetch_weather_service_history_weather_data_offline(
            45.0, 14.0, start, end, ["temperature_2m"]
        )

        body = mock_post.call_args.kwargs["json"]
        assert body["lat"] == 45.0
        assert body["lon"] == 14.0
        assert body["start"] == start.isoformat()
        assert body["end"] == end.isoformat()
        assert body["variables"] == ["temperature_2m"]
        assert "radius_km" in body


class TestConvertHistoryWeatherData:

    def test_returns_dataframe_from_valid_response(self):
        from utils.wdutils import convert_weather_service_history_weather_data_to_dataframe
        response = {
            "location": {"lat": 45.0, "lon": 14.0},
            "data": [
                {"timestamp": "2024-01-01T00:00:00", "values": {"temperature_2m": 20.0}},
                {"timestamp": "2024-01-01T01:00:00", "values": {"temperature_2m": 21.0}},
            ],
            "source": "openmeteo"
        }

        import pandas as pd
        df = convert_weather_service_history_weather_data_to_dataframe(response)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    def test_openmeteo_columns_mapped_to_openagri_standard(self):
        from utils.wdutils import convert_weather_service_history_weather_data_to_dataframe
        response = {
            "location": {"lat": 45.0, "lon": 14.0},
            "data": [
                {
                    "timestamp": "2024-01-01T00:00:00",
                    "values": {
                        "temperature_2m": 20.0,
                        "relative_humidity_2m": 65.0,
                        "precipitation": 0.0,
                        "wind_speed_10m": 3.5,
                    }
                }
            ],
            "source": "openmeteo"
        }

        df = convert_weather_service_history_weather_data_to_dataframe(response)

        assert "atmospheric_temperature" in df.columns
        assert "atmospheric_relative_humidity" in df.columns
        assert "precipitation" in df.columns
        assert "average_wind_speed" in df.columns
        # raw openmeteo names should not appear
        assert "temperature_2m" not in df.columns
        assert "relative_humidity_2m" not in df.columns

    def test_raises_400_on_empty_data_list(self):
        from utils.wdutils import convert_weather_service_history_weather_data_to_dataframe

        with pytest.raises(HTTPException) as exc_info:
            convert_weather_service_history_weather_data_to_dataframe(
                {"location": {"lat": 45.0, "lon": 14.0}, "data": [], "source": "openmeteo"}
            )

        assert exc_info.value.status_code == 400

    def test_timestamp_parsed_as_datetime(self):
        from utils.wdutils import convert_weather_service_history_weather_data_to_dataframe
        import pandas as pd
        response = {
            "location": {"lat": 45.0, "lon": 14.0},
            "data": [
                {"timestamp": "2024-01-01T12:00:00", "values": {"temperature_2m": 20.0}}
            ],
            "source": "openmeteo"
        }

        df = convert_weather_service_history_weather_data_to_dataframe(response)

        assert pd.api.types.is_datetime64_any_dtype(df["timestamp"])

    def test_unknown_openmeteo_variables_are_skipped(self):
        from utils.wdutils import convert_weather_service_history_weather_data_to_dataframe
        response = {
            "location": {"lat": 45.0, "lon": 14.0},
            "data": [
                {
                    "timestamp": "2024-01-01T00:00:00",
                    "values": {
                        "temperature_2m": 20.0,
                        "some_unknown_variable": 99.9,
                    }
                }
            ],
            "source": "openmeteo"
        }

        df = convert_weather_service_history_weather_data_to_dataframe(response)

        assert "some_unknown_variable" not in df.columns
        assert "atmospheric_temperature" in df.columns


class TestWeatherServiceClient:

    GOOD_HOURLY_RESPONSE = {
        "location": {"lat": 45.0, "lon": 14.0},
        "data": [{"timestamp": "2024-01-01T00:00:00", "values": {"temperature_2m": 20.0}}],
        "source": "openmeteo",
    }

    def _mock_ok_post(self, mocker, body: dict):
        mock = mocker.patch("utils.weather_service_client.requests.post")
        mock.return_value.status_code = 200
        mock.return_value.ok = True
        mock.return_value.json.return_value = body
        return mock

    def _mock_ok_get(self, mocker, body: dict):
        mock = mocker.patch("utils.weather_service_client.requests.get")
        mock.return_value.status_code = 200
        mock.return_value.ok = True
        mock.return_value.json.return_value = body
        return mock

    def test_get_hourly_history_posts_to_correct_path(self, mocker):
        from utils.weather_service_client import WeatherServiceClient
        mock = self._mock_ok_post(mocker, self.GOOD_HOURLY_RESPONSE)

        client = WeatherServiceClient("http://weather-service:8000")
        client.get_hourly_history(45.0, 14.0, datetime.date(2024, 1, 1), datetime.date(2024, 1, 7), ["temperature_2m"])

        url = mock.call_args.args[0] if mock.call_args.args else mock.call_args.kwargs.get("url", mock.call_args[0][0])
        assert url == "http://weather-service:8000/api/v1/history/hourly/"

    def test_get_hourly_history_sends_correct_body(self, mocker):
        from utils.weather_service_client import WeatherServiceClient
        mock = self._mock_ok_post(mocker, self.GOOD_HOURLY_RESPONSE)

        client = WeatherServiceClient("http://weather-service:8000")
        client.get_hourly_history(45.0, 14.0, datetime.date(2024, 1, 1), datetime.date(2024, 1, 7), ["temperature_2m"], radius_km=15)

        body = mock.call_args.kwargs["json"]
        assert body["lat"] == 45.0
        assert body["lon"] == 14.0
        assert body["start"] == "2024-01-01"
        assert body["end"] == "2024-01-07"
        assert body["variables"] == ["temperature_2m"]
        assert body["radius_km"] == 15

    def test_get_daily_history_posts_to_correct_path(self, mocker):
        from utils.weather_service_client import WeatherServiceClient
        daily_response = {
            "location": {"lat": 45.0, "lon": 14.0},
            "data": [{"date": "2024-01-01", "values": {"temperature_2m_max": 20.0}}],
            "source": "openmeteo",
        }
        mock = self._mock_ok_post(mocker, daily_response)

        client = WeatherServiceClient("http://weather-service:8000")
        client.get_daily_history(45.0, 14.0, datetime.date(2024, 1, 1), datetime.date(2024, 1, 7), ["temperature_2m_max"])

        url = mock.call_args.args[0] if mock.call_args.args else mock.call_args[0][0]
        assert url == "http://weather-service:8000/api/v1/history/daily/"

    def test_get_hourly_forecast_gets_correct_path(self, mocker):
        from utils.weather_service_client import WeatherServiceClient
        mock = self._mock_ok_get(mocker, self.GOOD_HOURLY_RESPONSE)

        client = WeatherServiceClient("http://weather-service:8000")
        client.get_hourly_forecast(45.0, 14.0, days=3)

        url = mock.call_args.args[0] if mock.call_args.args else mock.call_args[0][0]
        assert url == "http://weather-service:8000/api/v1/forecast/hourly/"
        params = mock.call_args.kwargs["params"]
        assert params["days"] == 3

    def test_raises_400_on_connection_error(self, mocker):
        from utils.weather_service_client import WeatherServiceClient
        from requests import RequestException
        mocker.patch("utils.weather_service_client.requests.post", side_effect=RequestException())

        client = WeatherServiceClient("http://weather-service:8000")
        with pytest.raises(HTTPException) as exc_info:
            client.get_hourly_history(45.0, 14.0, datetime.date(2024, 1, 1), datetime.date(2024, 1, 7), ["temperature_2m"])

        assert exc_info.value.status_code == 400

    def test_raises_400_on_404(self, mocker):
        from utils.weather_service_client import WeatherServiceClient
        mock = mocker.patch("utils.weather_service_client.requests.post")
        mock.return_value.status_code = 404
        mock.return_value.ok = False

        client = WeatherServiceClient("http://weather-service:8000")
        with pytest.raises(HTTPException) as exc_info:
            client.get_hourly_history(45.0, 14.0, datetime.date(2024, 1, 1), datetime.date(2024, 1, 7), ["temperature_2m"])

        assert exc_info.value.status_code == 400
        assert "404" in exc_info.value.detail

    def test_raises_400_on_non_ok(self, mocker):
        from utils.weather_service_client import WeatherServiceClient
        mock = mocker.patch("utils.weather_service_client.requests.post")
        mock.return_value.status_code = 503
        mock.return_value.ok = False

        client = WeatherServiceClient("http://weather-service:8000")
        with pytest.raises(HTTPException) as exc_info:
            client.get_hourly_history(45.0, 14.0, datetime.date(2024, 1, 1), datetime.date(2024, 1, 7), ["temperature_2m"])

        assert exc_info.value.status_code == 400

    def test_base_url_trailing_slash_stripped(self, mocker):
        from utils.weather_service_client import WeatherServiceClient
        mock = self._mock_ok_post(mocker, self.GOOD_HOURLY_RESPONSE)

        client = WeatherServiceClient("http://weather-service:8000/")
        client.get_hourly_history(45.0, 14.0, datetime.date(2024, 1, 1), datetime.date(2024, 1, 7), ["temperature_2m"])

        url = mock.call_args.args[0] if mock.call_args.args else mock.call_args[0][0]
        assert "//" not in url.replace("http://", "")
