from fastapi import APIRouter
from .endpoints import login, user, data, rule, unit, operator, tool, pest_model, parcel, disease, model

api_router = APIRouter()
api_router.include_router(login.router, prefix="/login", tags=["login"])
api_router.include_router(user.router, prefix="/user", tags=["user"])
api_router.include_router(data.router, prefix="/data", tags=["data"])
api_router.include_router(rule.router, prefix="/rule", tags=["rule"])
api_router.include_router(unit.router, prefix="/unit", tags=["unit"])
api_router.include_router(operator.router, prefix="/operator", tags=["operator"])
api_router.include_router(tool.router, prefix="/tool", tags=["tool"])
api_router.include_router(pest_model.router, prefix="/pest-model", tags=["pest-model"])
api_router.include_router(parcel.router, prefix="/parcel", tags=["parcel"])
api_router.include_router(disease.router, prefix="/disease", tags=["disease"])
api_router.include_router(model.router, prefix="/model", tags=["model"])