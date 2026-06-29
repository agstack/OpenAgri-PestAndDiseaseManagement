from unittest.mock import MagicMock

from sqlalchemy.orm import Session

from api import deps
from core.config import settings


def test_get_jwt_returns_disabled_when_flag_set(monkeypatch):
    monkeypatch.setattr(settings, "DISABLE_AUTH", True)
    result = deps.get_jwt(token=None, db=MagicMock(spec=Session))
    assert result == "disabled"


def test_get_current_user_returns_dummy_when_flag_set(monkeypatch):
    monkeypatch.setattr(settings, "DISABLE_AUTH", True)
    user = deps.get_current_user(token="disabled", db=MagicMock(spec=Session))
    assert user.email == "admin@local"


def test_get_refresh_token_returns_disabled_when_flag_set(monkeypatch):
    monkeypatch.setattr(settings, "DISABLE_AUTH", True)
    result = deps.get_refresh_token(refresh_token=None)
    assert result == "disabled"
