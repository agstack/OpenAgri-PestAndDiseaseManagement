import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from sqlalchemy.orm import Session
from unittest.mock import MagicMock

from app.api.api_v1.endpoints.parcel import router as parcel_router
from api import deps
from models import Parcel
from shapely import errors as shapely_errors


@pytest.fixture
def mock_db_session() -> MagicMock:
    """Provides a MagicMock object that mimics a SQLAlchemy Session."""
    return MagicMock(spec=Session)


@pytest.fixture
def mock_parcel_db_obj() -> MagicMock:
    """A mock 'Parcel' object as returned by CRUD."""
    parcel = MagicMock(spec=Parcel)
    parcel.id = 1
    parcel.name = "Test Parcel"
    parcel.latitude = 10.0
    parcel.longitude = 20.0
    return parcel


@pytest.fixture
def client(mock_db_session: MagicMock) -> TestClient:
    """
    Creates a FastAPI TestClient with 'get_db' and 'get_jwt' mocked.
    """
    app = FastAPI()
    app.include_router(parcel_router)

    app.dependency_overrides[deps.get_db] = lambda: mock_db_session
    app.dependency_overrides[deps.get_jwt] = lambda: {"user_id": 1, "role": "admin"}

    with TestClient(app) as test_client:
        yield test_client


class TestParcelAPI:
    """Groups all tests for the Parcel API."""

    CRUD_PATCH_TARGET = "app.api.api_v1.endpoints.parcel.crud"
    UTILS_PATCH_TARGET = "app.api.api_v1.endpoints.parcel.fetch_historical_data_for_parcel"
    WKT_PATCH_TARGET = "app.api.api_v1.endpoints.parcel.wkt"
    WKT_ERRORS_PATCH_TARGET = "app.api.api_v1.endpoints.parcel.errors"

    def test_get_all_parcels(
            self,
            client: TestClient,
            mocker: MockerFixture,
            mock_db_session: MagicMock,
            mock_parcel_db_obj: MagicMock
    ):
        """Tests GET / - successfully returning all parcels."""
        mock_parcels_list = [mock_parcel_db_obj]
        mock_crud = mocker.patch(self.CRUD_PATCH_TARGET)
        mock_crud.parcel.get_all.return_value = mock_parcels_list

        response = client.get("/")

        assert response.status_code == 200
        mock_crud.parcel.get_all.assert_called_with(db=mock_db_session)

        response_data = response.json()
        assert "elements" in response_data
        assert len(response_data["elements"]) == 1
        assert response_data["elements"][0]["id"] == mock_parcel_db_obj.id
        assert response_data["elements"][0]["name"] == mock_parcel_db_obj.name

    def test_upload_parcel_lat_lon_success(
            self,
            client: TestClient,
            mocker: MockerFixture,
            mock_db_session: MagicMock,
            mock_parcel_db_obj: MagicMock
    ):
        """Tests POST / - successfully uploading a new parcel."""
        payload = {"name": "New Farm", "latitude": 12.3, "longitude": 45.6}

        mock_crud = mocker.patch(self.CRUD_PATCH_TARGET)
        mock_crud.parcel.create.return_value = mock_parcel_db_obj

        mock_fetch_data = mocker.patch(self.UTILS_PATCH_TARGET)

        response = client.post("/", json=payload)

        assert response.status_code == 200
        assert response.json() == {"message": "Successfully uploaded parcel information!"}

        mock_crud.parcel.create.assert_called_once()
        call_args = mock_crud.parcel.create.call_args
        assert call_args[1]["db"] == mock_db_session
        assert call_args[1]["obj_in"].name == payload["name"]
        assert call_args[1]["obj_in"].latitude == payload["latitude"]

        mock_fetch_data.assert_called_with(db=mock_db_session, parcel=mock_parcel_db_obj)

    def test_upload_parcel_wkt_success(
            self,
            client: TestClient,
            mocker: MockerFixture,
            mock_db_session: MagicMock,
            mock_parcel_db_obj: MagicMock
    ):
        """Tests POST /wkt-format/ - successfully uploading a WKT polygon."""
        wkt_payload = {"name": "WKT Farm", "wkt_polygon": "POLYGON ((...))"}

        mock_geom = MagicMock()
        mock_geom.centroid.x = 30.0  # This will be the latitude
        mock_geom.centroid.y = 40.0  # This will be the longitude
        mock_wkt_loads = mocker.patch(f"{self.WKT_PATCH_TARGET}.loads", return_value=mock_geom)

        mock_crud = mocker.patch(self.CRUD_PATCH_TARGET)
        mock_crud.parcel.create.return_value = mock_parcel_db_obj
        mock_fetch_data = mocker.patch(self.UTILS_PATCH_TARGET)

        response = client.post("/wkt-format/", json=wkt_payload)

        assert response.status_code == 200
        assert response.json() == {"message": "Successfully uploaded parcel information!"}

        mock_wkt_loads.assert_called_with(wkt_payload["wkt_polygon"])

        mock_crud.parcel.create.assert_called_once()
        call_args = mock_crud.parcel.create.call_args
        assert call_args[1]["db"] == mock_db_session
        assert call_args[1]["obj_in"].name == "WKT Farm"
        assert call_args[1]["obj_in"].latitude == 30.0  # from mock_geom.centroid.x
        assert call_args[1]["obj_in"].longitude == 40.0  # from mock_geom.centroid.y

        mock_fetch_data.assert_called_with(db=mock_db_session, parcel=mock_parcel_db_obj)

    def test_upload_parcel_wkt_fail_shapely_error(
            self,
            client: TestClient,
            mocker: MockerFixture,
            mock_db_session: MagicMock
    ):
        """Tests POST /wkt-format/ - failing on a bad WKT string."""
        wkt_payload = {"name": "Bad WKT", "wkt_polygon": "INVALID WKT STRING"}

        test_exception = shapely_errors.ShapelyError("test error message")
        mocker.patch(f"{self.WKT_PATCH_TARGET}.loads", side_effect=test_exception)

        mocker.patch(f"{self.WKT_ERRORS_PATCH_TARGET}.ShapelyError", shapely_errors.ShapelyError)

        mock_crud = mocker.patch(self.CRUD_PATCH_TARGET)
        mock_fetch_data = mocker.patch(self.UTILS_PATCH_TARGET)

        response = client.post("/wkt-format/", json=wkt_payload)

        assert response.status_code == 400
        assert "Error during WKT parsing" in response.json()["detail"]
        assert "test error message" in response.json()["detail"]

        mock_crud.parcel.create.assert_not_called()
        mock_fetch_data.assert_not_called()

    def test_delete_parcel_success(
            self,
            client: TestClient,
            mocker: MockerFixture,
            mock_db_session: MagicMock,
            mock_parcel_db_obj: MagicMock
    ):
        """Tests DELETE /{parcel_id}/ - successfully deleting a parcel."""
        parcel_id_to_delete = 123

        mock_crud = mocker.patch(self.CRUD_PATCH_TARGET)
        mock_crud.parcel.get.return_value = mock_parcel_db_obj
        mock_crud.parcel.remove.return_value = None

        response = client.delete(f"/{parcel_id_to_delete}/")

        assert response.status_code == 200
        assert response.json() == {"message": f"Successfully removed parcel with ID:{parcel_id_to_delete}"}

        mock_crud.parcel.get.assert_called_with(db=mock_db_session, id=parcel_id_to_delete)
        mock_crud.parcel.remove.assert_called_with(db=mock_db_session, id=parcel_id_to_delete)

    def test_delete_parcel_fail_not_found(
            self,
            client: TestClient,
            mocker: MockerFixture,
            mock_db_session: MagicMock
    ):
        """Tests DELETE /{parcel_id}/ - failing when parcel ID doesn't exist."""
        parcel_id_to_delete = 404

        mock_crud = mocker.patch(self.CRUD_PATCH_TARGET)
        mock_crud.parcel.get.return_value = None

        response = client.delete(f"/{parcel_id_to_delete}/")

        assert response.status_code == 400
        assert response.json() == {"detail": f"Error, no parcel with ID:{parcel_id_to_delete} found."}

        mock_crud.parcel.get.assert_called_with(db=mock_db_session, id=parcel_id_to_delete)
        mock_crud.parcel.remove.assert_not_called()