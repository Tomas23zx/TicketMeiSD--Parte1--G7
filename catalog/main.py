import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from time import perf_counter

from fastapi import FastAPI, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest


DEFAULT_EVENTS_FILE = Path(__file__).resolve().parent.parent / "data" / "events.json"
EVENTS_FILE = Path(os.getenv("EVENTS_FILE", str(DEFAULT_EVENTS_FILE)))

catalog_requests_total = Counter(
    "catalog_requests_total",
    "Total number of requests handled by the Catalog service",
    ["endpoint", "http_status"],
)
catalog_request_duration_seconds = Histogram(
    "catalog_request_duration_seconds",
    "Duration of requests handled by the Catalog service in seconds",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    with EVENTS_FILE.open(encoding="utf-8") as events_file:
        events = json.load(events_file)
    if not isinstance(events, list):
        raise ValueError(f"Expected an array of events in {EVENTS_FILE}")
    app.state.events = events
    app.state.events_by_id = {event["id"]: event for event in events}
    yield


app = FastAPI(lifespan=lifespan)


@app.middleware("http")
async def observe_requests(request: Request, call_next):
    if request.url.path == "/metrics":
        return await call_next(request)

    started_at = perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        route = request.scope.get("route")
        endpoint = route.path if route else "unmatched"
        catalog_requests_total.labels(endpoint, str(status_code)).inc()
        catalog_request_duration_seconds.observe(perf_counter() - started_at)


@app.get("/events")
async def get_events(request: Request):
    return request.app.state.events


@app.get("/events/{event_id}")
async def get_event(event_id: str, request: Request):
    event = request.app.state.events_by_id.get(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return event


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)