import requests

from fastapi import APIRouter
from core import settings
from requests.exceptions import RequestException


from api.api_v1.endpoints import operator, pest_model, rule, tool, unit, model, disease


def register_apis_to_gatekeeper():

    # Login
    try:
        at = requests.post(
            url=str(settings.GATEKEEPER_BASE_URL).strip("/") + "/api/login/",
            headers={"Content-Type": "application/json"},
            json={
                "username": "{}".format(settings.GATEKEEPER_USERNAME),
                "password": "{}".format(settings.GATEKEEPER_PASSWORD)
            }
        )
    except RequestException:
        return

    temp = at.json()

    access = temp["access"]
    refresh = temp["refresh"]

    # Register APIs
    apis_to_register = APIRouter()
    apis_to_register.include_router(operator.router, prefix="/operator")
    apis_to_register.include_router(pest_model.router, prefix="/pest-model")
    apis_to_register.include_router(rule.router, prefix="/rule")
    apis_to_register.include_router(tool.router, prefix="/tool")
    apis_to_register.include_router(unit.router, prefix="/unit")
    apis_to_register.include_router(model.router, prefix="/model")
    apis_to_register.include_router(disease.router, prefix="/disease")

    for api in apis_to_register.routes:
        try:
            requests.post(
                url=str(settings.GATEKEEPER_BASE_URL).strip("/") + "/api/register_service/",
                headers={"Content-Type": "application/json", "Authorization" : "Bearer {}".format(access)},
                json={
                    "base_url": "http://{}:{}/".format(settings.SERVICE_NAME, settings.SERVICE_PORT),
                    "service_name": settings.SERVICE_NAME,
                    "endpoint": "api/v1/" + api.path.lstrip("/"),
                    "methods": list(api.methods)
                }
            )
        except RequestException:
            try:
                requests.post(
                    url=str(settings.GATEKEEPER_BASE_URL).strip("/") + "/api/logout/",
                    headers={"Content-Type": "application/json"},
                    json={"refresh": refresh}
                )
            except RequestException:
                return
            return

    # Logout
    try:
        requests.post(
            url=str(settings.GATEKEEPER_BASE_URL).strip("/") + "/api/logout/",
            headers={"Content-Type": "application/json"},
            json={"refresh": refresh}
        )
    except RequestException:
        return

    return
