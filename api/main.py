"""FastAPI application entrypoint — Moonwalk Operational API."""
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass  # Railway sets env vars natively

from api.database import init_db
from api.routers import employees, health
from config import API_VERSION, FASTAPI_API_KEY

logger = logging.getLogger("api.startup")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not FASTAPI_API_KEY:
        logger.warning("MOONWALK_API_KEY not set — all requests will be rejected")
    init_db()
    try:
        from seed_employees import seed
        n = seed()
        if n:
            logger.info("Seeded %d demo employees on first start", n)
    except Exception as exc:
        logger.warning("Seed skipped: %s", exc)
    yield


app = FastAPI(title="Moonwalk Operational API", version=API_VERSION, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://app.appsmith.com"],
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["X-API-Key", "Content-Type"],
)

app.include_router(health.router)
app.include_router(employees.router, prefix="/employees", tags=["employees"])
