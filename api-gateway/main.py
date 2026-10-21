import os
from contextlib import asynccontextmanager
from time import perf_counter

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest


CATALOG_URL = os.getenv("CATALOG_URL", "http://catalog:8001").rstrip("/")
ORDER_URL = os.getenv("ORDER_URL", "http://order:8002").rstrip("/")

gateway_requests_total = Counter(
    "gateway_requests_total",
    "Total number of requests handled by the API Gateway",
    ["method", "endpoint", "http_status"],
)
gateway_request_duration_seconds = Histogram(
    "gateway_request_duration_seconds",
    "Duration of requests handled by the API Gateway in seconds",
    ["endpoint"],
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.catalog_client = httpx.AsyncClient(timeout=10.0)
    app.state.order_client = httpx.AsyncClient(timeout=10.0)
    yield
    await app.state.catalog_client.aclose()
    await app.state.order_client.aclose()


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
        endpoint = route.path if route else request.url.path
        gateway_requests_total.labels(
            request.method, endpoint, str(status_code)
        ).inc()
        gateway_request_duration_seconds.labels(endpoint).observe(
            perf_counter() - started_at
        )


@app.get("/events")
async def get_events(request: Request):
    try:
        upstream_response = await request.app.state.catalog_client.get(
            f"{CATALOG_URL}/events", params=request.query_params
        )
    except httpx.TimeoutException as exc:
        raise HTTPException(status_code=504, detail="Catalog request timed out") from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail="Catalog is unavailable") from exc

    return Response(
        content=upstream_response.content,
        status_code=upstream_response.status_code,
        headers={
            "content-type": upstream_response.headers.get(
                "content-type", "application/json"
            )
        },
    )


@app.get("/events/{event_id}")
async def get_event(event_id: str, request: Request):
    try:
        upstream_response = await request.app.state.catalog_client.get(
            f"{CATALOG_URL}/events/{event_id}", params=request.query_params
        )
    except httpx.TimeoutException as exc:
        raise HTTPException(status_code=504, detail="Catalog request timed out") from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail="Catalog is unavailable") from exc

    return Response(
        content=upstream_response.content,
        status_code=upstream_response.status_code,
        headers={
            "content-type": upstream_response.headers.get(
                "content-type", "application/json"
            )
        },
    )


@app.post("/orders")
async def create_order(request: Request):
    body = await request.body()
    try:
        upstream_response = await request.app.state.order_client.post(
            f"{ORDER_URL}/orders",
            content=body,
            headers={
                "content-type": request.headers.get("content-type", "application/json")
            },
        )
    except httpx.TimeoutException as exc:
        raise HTTPException(status_code=504, detail="Order service timed out") from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail="Order service is unavailable") from exc

    return Response(
        content=upstream_response.content,
        status_code=upstream_response.status_code,
        headers={
            "content-type": upstream_response.headers.get(
                "content-type", "application/json"
            )
        },
    )


@app.get("/orders/{order_id}")
async def get_order(order_id: int, request: Request):
    try:
        upstream_response = await request.app.state.order_client.get(
            f"{ORDER_URL}/orders/{order_id}"
        )
    except httpx.TimeoutException as exc:
        raise HTTPException(status_code=504, detail="Order service timed out") from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail="Order service is unavailable") from exc

    return Response(
        content=upstream_response.content,
        status_code=upstream_response.status_code,
        headers={
            "content-type": upstream_response.headers.get(
                "content-type", "application/json"
            )
        },
    )


@app.get("/orders")
async def list_orders(request: Request):
    try:
        upstream_response = await request.app.state.order_client.get(
            f"{ORDER_URL}/orders", params=request.query_params
        )
    except httpx.TimeoutException as exc:
        raise HTTPException(status_code=504, detail="Order service timed out") from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail="Order service is unavailable") from exc

    return Response(
        content=upstream_response.content,
        status_code=upstream_response.status_code,
        headers={
            "content-type": upstream_response.headers.get(
                "content-type", "application/json"
            )
        },
    )


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)