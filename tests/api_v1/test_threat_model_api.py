import uuid
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.api.api_v1.endpoints.threat_model import router as tm_router
from api import deps
from models.threat_model import ThreatModel
from models.crop import Crop


SAMPLE_DEFINITION = {
    "bio_params": {
        "t_base": 5.0,
        "t_lethal_min": -5.0,
        "t_lethal_max": 40.0,
        "min_streak": 1,
    },
    "fuzzy_rules": [
        {
            "hum_lo": 70.0, "hum_hi": 100.0,
            "temp_lo": 5.0, "temp_hi": 25.0,
            "rain_min": 2.0,
            "risk_level": "high",
        }
    ],
}


@pytest.fixture
def mock_db_session() -> MagicMock:
    return MagicMock(spec=Session)


def _make_tm(scientific_name: str = "Venturia inaequalis") -> MagicMock:
    crop_id = uuid.uuid4()
    obj = MagicMock(spec=ThreatModel)
    obj.id = uuid.uuid4()
    obj.scientific_name = scientific_name
    obj.common_name     = "Apple scab"
    obj.label           = None
    obj.note            = None
    obj.definition      = SAMPLE_DEFINITION
    obj.crop_id         = crop_id
    return obj


@pytest.fixture
def client(mock_db_session: MagicMock) -> TestClient:
    app = FastAPI()
    app.include_router(tm_router)
    app.dependency_overrides[deps.get_db]  = lambda: mock_db_session
    app.dependency_overrides[deps.get_jwt] = lambda: "token"
    with TestClient(app) as c:
        yield c


class TestThreatModelAPI:
    CRUD = "app.api.api_v1.endpoints.threat_model.crud"

    def test_list_all(self, client, mocker):
        tm = _make_tm()
        mock_crud = mocker.patch(self.CRUD)
        mock_crud.threat_model.get_multi.return_value = [tm]
        r = client.get("/")
        assert r.status_code == 200
        assert len(r.json()) == 1

    def test_list_by_crop_id(self, client, mocker):
        crop_id = uuid.uuid4()
        tm = _make_tm()
        mock_crud = mocker.patch(self.CRUD)
        mock_crud.threat_model.get_by_crop.return_value = [tm]
        r = client.get(f"/?crop_id={crop_id}")
        assert r.status_code == 200
        mock_crud.threat_model.get_by_crop.assert_called_once()

    def test_create_success(self, client, mocker):
        crop_id = uuid.uuid4()
        tm = _make_tm()
        mock_crud = mocker.patch(self.CRUD)
        mock_crud.crop.get.return_value = MagicMock(spec=Crop)
        mock_crud.threat_model.create.return_value = tm
        payload = {
            "scientific_name": "Venturia inaequalis",
            "common_name":     "Apple scab",
            "definition":      SAMPLE_DEFINITION,
            "crop_id":         str(crop_id),
        }
        r = client.post("/", json=payload)
        assert r.status_code == 200
        assert r.json()["scientific_name"] == "Venturia inaequalis"

    def test_create_crop_not_found(self, client, mocker):
        mock_crud = mocker.patch(self.CRUD)
        mock_crud.crop.get.return_value = None
        payload = {
            "scientific_name": "X",
            "common_name":     "Y",
            "definition":      SAMPLE_DEFINITION,
            "crop_id":         str(uuid.uuid4()),
        }
        r = client.post("/", json=payload)
        assert r.status_code == 404

    def test_update_success(self, client, mocker):
        tm = _make_tm()
        updated = _make_tm()
        updated.common_name = "Scab"
        mock_crud = mocker.patch(self.CRUD)
        mock_crud.threat_model.get.return_value = tm
        mock_crud.threat_model.update.return_value = updated
        r = client.patch(f"/{tm.id}/", json={"common_name": "Scab"})
        assert r.status_code == 200

    def test_update_not_found(self, client, mocker):
        mock_crud = mocker.patch(self.CRUD)
        mock_crud.threat_model.get.return_value = None
        r = client.patch(f"/{uuid.uuid4()}/", json={"common_name": "X"})
        assert r.status_code == 404

    def test_delete_success(self, client, mocker):
        tm = _make_tm()
        mock_crud = mocker.patch(self.CRUD)
        mock_crud.threat_model.get.return_value = tm
        mock_crud.threat_model.remove.return_value = tm
        r = client.delete(f"/{tm.id}/")
        assert r.status_code == 200

    def test_delete_not_found(self, client, mocker):
        mock_crud = mocker.patch(self.CRUD)
        mock_crud.threat_model.get.return_value = None
        r = client.delete(f"/{uuid.uuid4()}/")
        assert r.status_code == 404

    def test_create_invalid_definition_no_rules(self, client, mocker):
        mock_crud = mocker.patch(self.CRUD)
        mock_crud.crop.get.return_value = MagicMock(spec=Crop)
        payload = {
            "scientific_name": "X",
            "common_name":     "Y",
            "definition":      {"bio_params": {}, "fuzzy_rules": []},
            "crop_id":         str(uuid.uuid4()),
        }
        r = client.post("/", json=payload)
        assert r.status_code == 422

    def test_create_null_t_base_rejected(self, client, mocker):
        mock_crud = mocker.patch(self.CRUD)
        mock_crud.crop.get.return_value = MagicMock(spec=Crop)
        payload = {
            "scientific_name": "X",
            "common_name":     "Y",
            "definition": {
                "bio_params": {"t_base": None},
                "fuzzy_rules": [
                    {"hum_lo": 70.0, "hum_hi": 100.0, "temp_lo": 5.0, "temp_hi": 25.0,
                     "rain_min": 1.0, "risk_level": "high"},
                ],
            },
            "crop_id": str(uuid.uuid4()),
        }
        r = client.post("/", json=payload)
        assert r.status_code == 422

    def test_create_missing_t_base_uses_default(self, client, mocker):
        crop_id = uuid.uuid4()
        tm = _make_tm()
        mock_crud = mocker.patch(self.CRUD)
        mock_crud.crop.get.return_value = MagicMock(spec=Crop)
        mock_crud.threat_model.create.return_value = tm
        payload = {
            "scientific_name": "Venturia inaequalis",
            "common_name":     "Apple scab",
            "definition": {
                "bio_params": {},  # t_base omitted — should default to 5.0
                "fuzzy_rules": [
                    {"hum_lo": 70.0, "hum_hi": 100.0, "temp_lo": 5.0, "temp_hi": 25.0,
                     "rain_min": 1.0, "risk_level": "high"},
                ],
            },
            "crop_id": str(crop_id),
        }
        r = client.post("/", json=payload)
        assert r.status_code == 200

    def test_no_warnings_well_formed_model(self, client, mocker):
        tm = _make_tm()
        mock_crud = mocker.patch(self.CRUD)
        mock_crud.crop.get.return_value = MagicMock(spec=Crop)
        mock_crud.threat_model.create.return_value = tm
        payload = {
            "scientific_name": "Venturia inaequalis",
            "common_name":     "Apple scab",
            "definition":      SAMPLE_DEFINITION,
            "crop_id":         str(uuid.uuid4()),
        }
        r = client.post("/", json=payload)
        assert r.status_code == 200
        assert r.json()["warnings"] == []

    def test_warnings_trivial_rules(self, client, mocker):
        trivial_def = {
            "bio_params": {"t_lethal_min": -5.0, "t_lethal_max": 40.0},
            "fuzzy_rules": [{"risk_level": "high"}],  # all range defaults
        }
        tm = _make_tm()
        tm.definition = trivial_def
        mock_crud = mocker.patch(self.CRUD)
        mock_crud.crop.get.return_value = MagicMock(spec=Crop)
        mock_crud.threat_model.create.return_value = tm
        payload = {
            "scientific_name": "X",
            "common_name":     "Y",
            "definition":      trivial_def,
            "crop_id":         str(uuid.uuid4()),
        }
        r = client.post("/", json=payload)
        assert r.status_code == 200
        warns = r.json()["warnings"]
        assert any("identical risk" in w for w in warns)

    def test_warnings_empty_bio_params(self, client, mocker):
        empty_bio_def = {
            "bio_params": {},
            "fuzzy_rules": [
                {"hum_lo": 70.0, "hum_hi": 100.0, "temp_lo": 5.0, "temp_hi": 25.0,
                 "rain_min": 2.0, "risk_level": "high"},
            ],
        }
        tm = _make_tm()
        tm.definition = empty_bio_def
        mock_crud = mocker.patch(self.CRUD)
        mock_crud.crop.get.return_value = MagicMock(spec=Crop)
        mock_crud.threat_model.create.return_value = tm
        payload = {
            "scientific_name": "X",
            "common_name":     "Y",
            "definition":      empty_bio_def,
            "crop_id":         str(uuid.uuid4()),
        }
        r = client.post("/", json=payload)
        assert r.status_code == 200
        warns = r.json()["warnings"]
        assert any("No biological parameters" in w for w in warns)
