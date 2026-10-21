import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, Field

from .auth import require_api_key
from .matching import rider_lite_from_input, run_matching, serialize_result
from .rider_fetch import fetch_unmatched_riders
from .supabase_client import SupabaseNotConfigured, get_client, is_configured

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("ml_microservice")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    logger.info(json.dumps({"event": "matching_engine_ready"}))
    yield


app = FastAPI(
    title="Pickup Matching Service",
    version="2.0.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def observe_requests(request: Request, call_next):
    request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
    started_at = time.perf_counter()
    response = None
    status_code = 500

    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        logger.info(json.dumps({
            "event": "http_request",
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "status_code": status_code,
            "duration_ms": duration_ms,
        }))
        if response is not None:
            response.headers["x-request-id"] = request_id


class RiderInput(BaseModel):
    user_id: str = Field(..., description="Opaque rider identifier")
    flight_id: int = Field(..., description="Unique id for this rider's flight form")
    date: str = Field(..., description="Flight date, YYYY-MM-DD", example="2026-11-21")
    earliest_time: str = Field(..., description="Earliest pickup-window time, HH:MM:SS", example="08:00:00")
    latest_time: str = Field(..., description="Latest pickup-window time, HH:MM:SS", example="10:00:00")
    airport: str = Field(..., description="Airport code", example="LAX")
    to_airport: bool = Field(..., description="True if heading to the airport, False if arriving from it")
    school: str = Field(..., description="Rider's school", example="POMONA")
    terminal: Optional[str] = Field(None, description="Terminal, if known")
    flight_no: Optional[int] = Field(None, description="Flight number")
    airline_iata: Optional[str] = Field(None, description="Airline IATA code")
    name: Optional[str] = Field(None, description="Rider's display name")
    bags_no: Optional[int] = Field(0, description="Number of normal bags")
    bags_no_large: Optional[int] = Field(0, description="Number of large bags")
    bag_no_personal: Optional[int] = Field(0, description="Number of personal bags")


class MatchRequest(BaseModel):
    riders: Optional[List[RiderInput]] = Field(
        None,
        description=(
            "Riders to bucket and match. Omit this field to fetch unmatched riders "
            "from Supabase instead (requires SUPABASE_URL/SUPABASE_SECRET_KEY)."
        ),
    )
    days_ahead: Optional[int] = Field(
        10, description="DB fetch only: include flights through today + N days."
    )
    days_ahead_start: Optional[int] = Field(
        None, description="DB fetch only: start of window (0 = include today)."
    )


@app.get("/")
async def service_info():
    return {
        "service": "pickup-matching",
        "status": "ok",
        "supabase_configured": is_configured(),
        "endpoints": {
            "health": "/health",
            "readiness": "/ready",
            "match": "/match",
            "docs": "/docs",
        },
    }


@app.get("/favicon.ico", status_code=204)
async def favicon():
    return Response(status_code=204)


@app.post("/match", status_code=200, dependencies=[Depends(require_api_key)])
async def match_riders(payload: MatchRequest) -> Dict[str, Any]:
    if payload.riders is not None:
        try:
            riders = [rider_lite_from_input(r.model_dump()) for r in payload.riders]
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=f"Invalid rider payload: {exc}") from exc
    else:
        try:
            sb = get_client()
            riders = fetch_unmatched_riders(
                sb,
                max_days_ahead=payload.days_ahead,
                min_days_ahead=payload.days_ahead_start,
            )
        except SupabaseNotConfigured as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Supabase fetch failed: {exc}") from exc

    flight_ids = [r.flight_id for r in riders]
    if len(set(flight_ids)) != len(flight_ids):
        raise HTTPException(status_code=400, detail="Duplicate flight_id in request payload.")

    try:
        result = run_matching(riders)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Matching error: {exc}") from exc

    return serialize_result(result)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.get("/ready")
async def readiness_check():
    return {"status": "ready"}
