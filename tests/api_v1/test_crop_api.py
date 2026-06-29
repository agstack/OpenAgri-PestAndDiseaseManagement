import uuid
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.api.api_v1.endpoints.crop import router as crop_router
from api import deps
from models.crop import Crop


@pytest.fixture
def mock_db_session() -> MagicMock:
    return MagicMock(spec=Session)


def _make_crop(name: str = "Apple") -> MagicMock:
    obj = MagicMock(spec=Crop)
    obj.id = uuid.uuid4()
    obj.name = name
    obj.description = None
    return obj


@pytest.fixture
def client(mock_db_session: MagicMock) -> TestClient:
    app = FastAPI()
    app.include_router(crop_router)
    app.dependency_overrides[deps.get_db]  = lambda: mock_db_session
    app.dependency_overrides[deps.get_jwt] = lambda: "token"
    with TestClient(app) as c:
        yield c


class TestCropAPI:
    CRUD = "app.api.api_v1.endpoints.crop.crud"

    def test_list_crops_empty(self, client, mocker, mock_db_session):
        mocker.patch(self.CRUD).crop.get_multi.return_value = []
        r = client.get("/")
        assert r.status_code == 200
        assert r.json() == []

    def test_list_crops_returns_items(self, client, mocker, mock_db_session):
        crop = _make_crop("Olive")
        mocker.patch(self.CRUD).crop.get_multi.return_value = [crop]
        r = client.get("/")
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 1
        assert data[0]["name"] == "Olive"

    def test_create_crop_success(self, client, mocker, mock_db_session):
        created = _make_crop("Vineyard")
        mock_crud = mocker.patch(self.CRUD)
        mock_crud.crop.create.return_value = created
        r = client.post("/", json={"name": "Vineyard"})
        assert r.status_code == 200
        assert r.json()["name"] == "Vineyard"

    def test_create_crop_missing_name(self, client, mocker):
        r = client.post("/", json={})
        assert r.status_code == 422

    def test_delete_crop_success(self, client, mocker, mock_db_session):
        crop = _make_crop()
        mock_crud = mocker.patch(self.CRUD)
        mock_crud.crop.get.return_value = crop
        mock_crud.crop.remove.return_value = crop
        r = client.delete(f"/{crop.id}/")
        assert r.status_code == 200

    def test_delete_crop_not_found(self, client, mocker, mock_db_session):
        mock_crud = mocker.patch(self.CRUD)
        mock_crud.crop.get.return_value = None
        r = client.delete(f"/{uuid.uuid4()}/")
        assert r.status_code == 404
