import os
from contextlib import asynccontextmanager
from math import ceil
from time import perf_counter

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from redis import RedisError
from redis.asyncio import Redis


CATALOG_URL = os.getenv("CATALOG_URL", "http://catalog:8001").rstrip("/")
ORDER_URL = os.getenv("ORDER_URL", "http://order:8002").rstrip("/")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "100"))
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))
# Limite específico para checkout de ordens (conforme RNF3: ex. 5 pedidos/segundo por IP)
RATE_LIMIT_ORDER_REQUESTS = int(os.getenv("RATE_LIMIT_ORDER_REQUESTS", "5"))
RATE_LIMIT_ORDER_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_ORDER_WINDOW_SECONDS", "1"))

RATE_LIMIT_SCRIPT = """
local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
local ttl = redis.call('PTTL', KEYS[1])
return {count, ttl}
"""

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
    app.state.redis = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        await app.state.redis.ping()
        yield
    finally:
        await app.state.catalog_client.aclose()
        await app.state.order_client.aclose()
        await app.state.redis.aclose()


app = FastAPI(lifespan=lifespan)


@app.middleware("http")
async def rate_limit_requests(request: Request, call_next):
    if request.url.path == "/metrics":
        return await call_next(request)

    client_host = request.client.host if request.client else "unknown"

    # Aplica limite dedicado para POST /orders (ou /order) conforme RNF3 (ex.: 5 req/s)
    path = request.url.path.rstrip("/")
    is_order_post = request.method == "POST" and path in ("/orders", "/order")

    if is_order_post:
        limit_requests = RATE_LIMIT_ORDER_REQUESTS
        window_seconds = RATE_LIMIT_ORDER_WINDOW_SECONDS
        key = f"ticketmei:rate-limit:order:{client_host}"
    else:
        limit_requests = RATE_LIMIT_REQUESTS
        window_seconds = RATE_LIMIT_WINDOW_SECONDS
        key = f"ticketmei:rate-limit:general:{client_host}"

    try:
        count, ttl_milliseconds = await request.app.state.redis.eval(
            RATE_LIMIT_SCRIPT,
            1,
            key,
            window_seconds,
        )
    except RedisError as exc:
        raise HTTPException(
            status_code=503,
            detail="Rate limiting service is unavailable",
        ) from exc

    remaining = max(limit_requests - int(count), 0)
    headers = {
        "X-RateLimit-Limit": str(limit_requests),
        "X-RateLimit-Remaining": str(remaining),
    }
    if int(count) > limit_requests:
        retry_after = max(ceil(int(ttl_milliseconds) / 1000), 1)
        headers["Retry-After"] = str(retry_after)
        return JSONResponse(
            status_code=429,
            content={"detail": "Too many requests. Rate limit exceeded."},
            headers=headers,
        )

    response = await call_next(request)
    for header_name, header_value in headers.items():
        response.headers[header_name] = header_value
    return response


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


@app.post("/order")
async def create_order_alias(request: Request):
    return await create_order(request)


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


@app.get("/order/{order_id}")
async def get_order_alias(order_id: int, request: Request):
    return await get_order(order_id, request)


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


@app.get("/order")
async def list_orders_alias(request: Request):
    return await list_orders(request)


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)