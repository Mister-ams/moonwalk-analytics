"""API key authentication dependency."""
import os

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(key: str | None = Security(api_key_header)) -> str:
    # Read from env at request time so tests can set the key after process start.
    # Fail-closed: if no key configured on server, reject all requests.
    api_key = os.environ.get("MOONWALK_API_KEY", "")
    if not api_key or not key or key != api_key:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or missing API key",
        )
    return key
