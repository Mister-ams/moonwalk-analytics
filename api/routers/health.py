"""Health check — no auth (Railway probes + Appsmith connectivity test)."""
from fastapi import APIRouter

from api.database import get_db
from config import API_VERSION

router = APIRouter()


@router.get("/health")
def health_check():
    db_status = "connected"
    try:
        with get_db() as con:
            con.execute("SELECT 1")
    except Exception:
        db_status = "error"
    return {"status": "ok", "db": db_status, "version": API_VERSION}
