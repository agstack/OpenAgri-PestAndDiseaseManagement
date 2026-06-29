from core import settings
from utils.gatekeeper_client import GatekeeperClient


def _client() -> GatekeeperClient:
    return GatekeeperClient(str(settings.GATEKEEPER_BASE_URL))


def gatekeeper_logout(refresh_token: str):
    _client().logout(refresh_token)


def check_token_for_validity(token: str, token_type: str):
    return _client().validate_token(token, token_type)
