from contextlib import asynccontextmanager

from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from api.api_v1.api import api_router
from core.config import settings
from init.db_init import init_db
from init.init_gatekeeper import register_apis_to_gatekeeper

from jobs.background_tasks import get_open_meteo_data


import logging

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(fa: FastAPI):
    if settings.DISABLE_AUTH:
        logger.warning("DISABLE_AUTH=True — all authentication is disabled. Do not use in production.")
    init_db()
    if settings.USING_GATEKEEPER:
        register_apis_to_gatekeeper()
    scheduler.add_job(get_open_meteo_data, 'cron', day_of_week='*', hour=0, minute=5, second=0)
    scheduler.start()
    yield
    scheduler.shutdown()
    logging.shutdown()


app = FastAPI(
    title="Pest and Disease Management", openapi_url="/api/v1/openapi.json", lifespan=lifespan
)

jobstores = {
    'default': MemoryJobStore()
}

scheduler = AsyncIOScheduler(jobstores=jobstores)

# Set all CORS enabled origins
if settings.CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.include_router(api_router, prefix="/api/v1")
