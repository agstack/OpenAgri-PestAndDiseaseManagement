import datetime
import uuid
from typing import Literal, Union, get_args, get_origin

from fastapi import APIRouter, HTTPException
from core import settings

from api.api_v1.endpoints import operator, pest_model, rule, tool, unit, model, disease, fuzzy_risk, crop, threat_model
from utils.gatekeeper_client import GatekeeperClient


def _type_token(annotation) -> str:
    origin = get_origin(annotation)
    if origin is Union:
        args = [a for a in get_args(annotation) if a is not type(None)]
        return _type_token(args[0]) if args else "string"
    if origin is Literal:
        return "string"
    if annotation is int:
        return "int"
    if annotation is float:
        return "float"
    if annotation is str:
        return "string"
    if annotation is datetime.date:
        return "date"
    if annotation is uuid.UUID:
        return "uuid"
    return "string"


def _build_params(route) -> str | None:
    fields = route.dependant.query_params
    if not fields:
        return None
    parts = []
    for f in fields:
        token = _type_token(f.field_info.annotation)
        if f.required:
            parts.append(f"{f.alias}={{{token}}}")
        else:
            parts.append(f"{f.alias}?={{{token}}}")
    return "&".join(parts)


def register_apis_to_gatekeeper():
    client = GatekeeperClient(str(settings.GATEKEEPER_BASE_URL))

    # Login
    try:
        tokens = client.login(
            username=str(settings.GATEKEEPER_USERNAME),
            password=str(settings.GATEKEEPER_PASSWORD),
        )
    except HTTPException:
        return

    access = tokens["access"]
    refresh = tokens["refresh"]

    # Register APIs
    apis_to_register = APIRouter()
    apis_to_register.include_router(operator.router, prefix="/operator")
    apis_to_register.include_router(pest_model.router, prefix="/pest-model")
    apis_to_register.include_router(rule.router, prefix="/rule")
    apis_to_register.include_router(tool.router, prefix="/tool")
    apis_to_register.include_router(unit.router, prefix="/unit")
    apis_to_register.include_router(model.router, prefix="/model")
    apis_to_register.include_router(disease.router, prefix="/disease")
    apis_to_register.include_router(fuzzy_risk.router, prefix="/fuzzy-risk")
    apis_to_register.include_router(crop.router, prefix="/crop")
    apis_to_register.include_router(threat_model.router, prefix="/threat-model")

    # Group by path: merge methods, keep first non-None params (GET typically precedes POST)
    route_groups: dict[str, dict] = {}
    for api in apis_to_register.routes:
        if api.path not in route_groups:
            route_groups[api.path] = {"methods": [], "params": None}
        route_groups[api.path]["methods"].extend(list(api.methods))
        if not route_groups[api.path]["params"]:
            route_groups[api.path]["params"] = _build_params(api)

    for path, info in route_groups.items():
        try:
            payload = {
                "base_url": "http://{}:{}/".format(settings.SERVICE_NAME, settings.SERVICE_PORT),
                "service_name": settings.SERVICE_NAME,
                "endpoint": "api/v1/" + path.lstrip("/"),
                "methods": info["methods"],
            }
            if info["params"]:
                payload["params"] = info["params"]
            client.register_service(access_token=access, payload=payload)
        except HTTPException:
            try:
                client.logout(refresh)
            except HTTPException:
                return
            return

    # Logout
    try:
        client.logout(refresh)
    except HTTPException:
        return

    return
