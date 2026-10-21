"""Optional shared-secret auth for endpoints that touch rider PII.

Set SERVICE_API_KEY to require callers to send a matching X-API-Key header.
Leave it unset for frictionless local development: every request is then
allowed through unchecked. Meant for server-to-server calls (e.g. from the
aspc-website-v2 backend), not browser clients.
"""

import os

from fastapi import Header, HTTPException

SERVICE_API_KEY = os.getenv("SERVICE_API_KEY")


async def require_api_key(x_api_key: str = Header(default=None)) -> None:
    if SERVICE_API_KEY and x_api_key != SERVICE_API_KEY:
        raise HTTPException(status_code=401, detail="Missing or invalid X-API-Key header.")
