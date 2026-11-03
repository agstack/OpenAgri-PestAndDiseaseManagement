import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from sqlalchemy.orm import Session
from unittest.mock import MagicMock
import requests

from app.api.api_v1.endpoints.user import router as user_router
from api import deps
from models import User


@pytest.fixture
def mock_db_session() -> MagicMock:
    """
    Provides a MagicMock object that mimics a SQLAlchemy Session.
    """
    return MagicMock(spec=Session)


@pytest.fixture
def mock_current_user() -> MagicMock:
    """
    Provides a MagicMock object that mimics a User model.
    This is what `get_current_user` will return.
    """
    user = MagicMock(spec=User)
    user.id = 1
    user.email = "test@example.com"
    return user


@pytest.fixture
def client(mock_db_session: MagicMock, mock_current_user: MagicMock) -> TestClient:
    """
    Creates a FastAPI TestClient with all dependencies mocked.
    This fixture is the core of our test setup.
    """
    app = FastAPI()

    app.include_router(user_router)

    app.dependency_overrides[deps.get_db] = lambda: mock_db_session
    app.dependency_overrides[deps.get_current_user] = lambda: mock_current_user
    app.dependency_overrides[deps.is_not_using_gatekeeper] = lambda: True

    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def valid_user_payload() -> dict:
    """
    A simple, valid user registration payload.
    """
    return {
        "email": "newuser@example.com",
        "password": "ValidPassword123"
    }

class TestRegister:
    """Groups all tests for the /register/ endpoint."""

    SETTINGS_PATCH_TARGET = "app.api.api_v1.endpoints.user.settings"
    CRUD_USER_PATCH_TARGET = "app.api.api_v1.endpoints.user.user"
    REQUESTS_PATCH_TARGET = "utils.gatekeeper_client.requests"

    def test_register_success_no_gatekeeper(
            self,
            client: TestClient,
            mocker: MockerFixture,
            mock_db_session: MagicMock,
            valid_user_payload: dict
    ):
        """
        Tests successful registration when USING_GATEKEEPER is False.
        """
        mocker.patch(f"{self.SETTINGS_PATCH_TARGET}.USING_GATEKEEPER", False)
        mocker.patch(f"{self.SETTINGS_PATCH_TARGET}.PASSWORD_SCHEMA_OBJ.validate", return_value=True)

        mock_crud_user = mocker.patch(self.CRUD_USER_PATCH_TARGET)
        mock_crud_user.get_by_email.return_value = None  # Simulate user not existing

        response = client.post("/register/", json=valid_user_payload)

        assert response.status_code == 200
        assert response.json() == {"message": "You have successfully registered!"}

        mock_crud_user.get_by_email.assert_called_with(db=mock_db_session, email="newuser@example.com")
        mock_crud_user.create.assert_called_once()

    def test_register_success_with_gatekeeper(
            self,
            client: TestClient,
            mocker: MockerFixture,
            valid_user_payload: dict
    ):
        """
        Tests successful registration when USING_GATEKEEPER is True.
        """
        mocker.patch(f"{self.SETTINGS_PATCH_TARGET}.USING_GATEKEEPER", True)
        mocker.patch(f"{self.SETTINGS_PATCH_TARGET}.PASSWORD_SCHEMA_OBJ.validate", return_value=True)
        mocker.patch(f"{self.SETTINGS_PATCH_TARGET}.GATEKEEPER_BASE_URL", "http://mock-gatekeeper.com")

        mock_response = MagicMock(spec=requests.Response)

        mock_response.status_code = 200

        mock_requests_post = mocker.patch(f"{self.REQUESTS_PATCH_TARGET}.post", return_value=mock_response)

        response = client.post("/register/", json=valid_user_payload)

        assert response.status_code == 200
        assert response.json() == {"message": "You have successfully registered!"}

        mock_requests_post.assert_called_with(
            "http://mock-gatekeeper.com/api/register/",
            headers={"Content-Type": "application/json"},
            json={
                "username": "newuser@example.com",
                "email": "newuser@example.com",
                "password": "ValidPassword123"
            }
        )

    def test_register_fail_weak_password(
            self,
            client: TestClient,
            mocker: MockerFixture,
            valid_user_payload: dict
    ):
        """
        Tests registration failure due to a weak password.
        """
        mocker.patch(f"{self.SETTINGS_PATCH_TARGET}.PASSWORD_SCHEMA_OBJ.validate", return_value=False)

        response = client.post("/register/", json=valid_user_payload)

        assert response.status_code == 400
        assert "Password needs to be at least 8 characters long" in response.json()["detail"]

    def test_register_fail_user_exists_no_gatekeeper(
            self,
            client: TestClient,
            mocker: MockerFixture,
            valid_user_payload: dict
    ):
        """
        Tests registration failure when user already exists (no gatekeeper).
        """
        mocker.patch(f"{self.SETTINGS_PATCH_TARGET}.USING_GATEKEEPER", False)
        mocker.patch(f"{self.SETTINGS_PATCH_TARGET}.PASSWORD_SCHEMA_OBJ.validate", return_value=True)

        mock_crud_user = mocker.patch(self.CRUD_USER_PATCH_TARGET)
        mock_crud_user.get_by_email.return_value = MagicMock(spec=User)  # User exists!

        response = client.post("/register/", json=valid_user_payload)

        assert response.status_code == 400
        assert response.json()["detail"] == "User with email:newuser@example.com already exists."
        mock_crud_user.create.assert_not_called()

    def test_register_fail_gatekeeper_connection_error(
            self,
            client: TestClient,
            mocker: MockerFixture,
            valid_user_payload: dict
    ):
        """
        Tests registration failure when gatekeeper is unreachable.
        """
        mocker.patch(f"{self.SETTINGS_PATCH_TARGET}.USING_GATEKEEPER", True)
        mocker.patch(f"{self.SETTINGS_PATCH_TARGET}.PASSWORD_SCHEMA_OBJ.validate", return_value=True)
        mocker.patch(f"{self.SETTINGS_PATCH_TARGET}.GATEKEEPER_BASE_URL", "http://mock-gatekeeper.com")

        mocker.patch(f"{self.REQUESTS_PATCH_TARGET}.post", side_effect=requests.RequestException)

        response = client.post("/register/", json=valid_user_payload)

        assert response.status_code == 400
        assert "Error, can't connect to gatekeeper instance" in response.json()["detail"]

    def test_register_fail_gatekeeper_rejects_user(
            self,
            client: TestClient,
            mocker: MockerFixture,
            valid_user_payload: dict
    ):
        """
        Tests registration failure when gatekeeper returns a 4xx/5xx error.
        """
        mocker.patch(f"{self.SETTINGS_PATCH_TARGET}.USING_GATEKEEPER", True)
        mocker.patch(f"{self.SETTINGS_PATCH_TARGET}.PASSWORD_SCHEMA_OBJ.validate", return_value=True)
        mocker.patch(f"{self.SETTINGS_PATCH_TARGET}.GATEKEEPER_BASE_URL", "http://mock-gatekeeper.com")

        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 400  # Gatekeeper rejection
        mocker.patch(f"{self.REQUESTS_PATCH_TARGET}.post", return_value=mock_response)

        response = client.post("/register/", json=valid_user_payload)

        assert response.status_code == 400
        assert response.json()["detail"] == "Error, gatekeeper raise issue with request."


def test_get_me_success(client: TestClient, mock_current_user: MagicMock):
    """
    Tests successful response from /me/.
    This test relies entirely on the `client` fixture's dependency overrides.
    """
    response = client.get("/me/")

    assert response.status_code == 200

    response_data = response.json()
    assert response_data["email"] == mock_current_user.email