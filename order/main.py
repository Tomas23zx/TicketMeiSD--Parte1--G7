import asyncio
import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from typing import Optional

import aio_pika
import httpx
from aiobreaker import CircuitBreaker, CircuitBreakerError, CircuitBreakerListener
from aiobreaker.state import CircuitBreakerState
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_random_exponential

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("order-service")

DATABASE_PATH = Path(os.getenv("DATABASE_PATH", "/app/data/order.db"))
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
PAYMENT_URL = os.getenv("PAYMENT_URL", "http://payment:8004/payments")
QUEUE_NAME = "orders"
ORDERS_EXCHANGE = "orders.exchange"
PAYMENT_TIMEOUT_SECONDS = float(os.getenv("PAYMENT_TIMEOUT_SECONDS", "3"))
PAYMENT_RETRY_ATTEMPTS = int(os.getenv("PAYMENT_RETRY_ATTEMPTS", "3"))
CIRCUIT_BREAKER_FAIL_MAX = int(os.getenv("CIRCUIT_BREAKER_FAIL_MAX", "3"))
CIRCUIT_BREAKER_RESET_TIMEOUT_SECONDS = float(
    os.getenv("CIRCUIT_BREAKER_RESET_TIMEOUT_SECONDS", "30")
)

# Prometheus metrics
orders_initiated_total = Counter(
    "orders_initiated_total",
    "Total number of orders initiated",
    ["status"],
)
order_requests_total = Counter(
    "order_requests_total",
    "Total requests processed by the Order service",
    ["endpoint", "http_status"],
)
order_request_duration_seconds = Histogram(
    "order_request_duration_seconds",
    "Duration of Order service requests in seconds",
    ["endpoint"],
)
circuit_breaker_state = Gauge(
    "circuit_breaker_state",
    "Payment circuit breaker state (0=closed, 1=open, 2=half-open)",
)
circuit_breaker_fallbacks_total = Counter(
    "circuit_breaker_fallbacks_total",
    "Total requests handled by the payment circuit breaker fallback",
)
payment_client_requests_total = Counter(
    "payment_client_requests_total",
    "Total requests sent from Order to Payment",
    ["status"],
)


class PaymentRequestError(Exception):
    def __init__(self, status_label: str, message: str):
        super().__init__(message)
        self.status_label = status_label


class PaymentCircuitBreakerListener(CircuitBreakerListener):
    def state_change(self, _breaker, old_state, new_state):
        # Keep the numeric Prometheus value aligned with the circuit state.
        state_name = type(new_state).__name__.lower()
        if "open" in state_name:
            circuit_breaker_state.set(1)
        elif "halfopen" in state_name or "half_open" in state_name:
            circuit_breaker_state.set(2)
        else:
            circuit_breaker_state.set(0)
        logger.info(
            "Payment circuit breaker changed from %s to %s (%s)",
            old_state,
            new_state,
            _breaker,
        )


payment_breaker = CircuitBreaker(
    fail_max=CIRCUIT_BREAKER_FAIL_MAX,
    timeout_duration=timedelta(seconds=CIRCUIT_BREAKER_RESET_TIMEOUT_SECONDS),
    listeners=[PaymentCircuitBreakerListener()],
)
circuit_breaker_state.set(0)


def should_retry_payment(exception: BaseException) -> bool:
    # Do not start another retry after this failure opened the circuit.
    return (
        isinstance(exception, PaymentRequestError)
        and payment_breaker.current_state != CircuitBreakerState.OPEN
    )


def init_db():
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER NOT NULL,
                event_id TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                total_amount REAL NOT NULL,
                customer_email TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
    logger.info("Order database initialized at %s", DATABASE_PATH)


async def get_rabbitmq_connection(retries: int = 10, delay: float = 3.0):
    for attempt in range(1, retries + 1):
        try:
            logger.info("Connecting to RabbitMQ (attempt %d/%d)...", attempt, retries)
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            logger.info("Connected to RabbitMQ successfully.")
            return connection
        except Exception as exc:
            logger.warning("RabbitMQ connection failed: %s. Retrying in %.1fs...", exc, delay)
            if attempt == retries:
                raise
            await asyncio.sleep(delay)


async def _send_payment(order_id: int, amount: float) -> None:
    # The timeout prevents a slow Payment service from blocking order creation.
    try:
        async with httpx.AsyncClient(timeout=PAYMENT_TIMEOUT_SECONDS) as client:
            response = await client.post(PAYMENT_URL, json={"order_id": order_id, "amount": amount})
    except httpx.TimeoutException as exc:
        payment_client_requests_total.labels(status="timeout").inc()
        raise PaymentRequestError("timeout", "Payment request timed out") from exc
    except httpx.HTTPError as exc:
        payment_client_requests_total.labels(status="500").inc()
        raise PaymentRequestError("500", "Payment request failed") from exc

    if 200 <= response.status_code < 300:
        payment_client_requests_total.labels(status="2xx").inc()
        return

    status_label = "500" if response.status_code >= 500 else str(response.status_code)
    payment_client_requests_total.labels(status=status_label).inc()
    raise PaymentRequestError(str(response.status_code), "Payment returned an unsuccessful status")


@retry(
    retry=retry_if_exception(should_retry_payment),
    wait=wait_random_exponential(multiplier=0.1, max=1.5),
    stop=stop_after_attempt(PAYMENT_RETRY_ATTEMPTS),
    reraise=True,
)
async def process_payment(order_id: int, amount: float) -> None:
    # The breaker wraps every attempt, so failures count toward opening it.
    await payment_breaker.call_async(_send_payment, order_id, amount)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    connection = await get_rabbitmq_connection()
    channel = await connection.channel()
    exchange = await channel.declare_exchange(
        ORDERS_EXCHANGE,
        aio_pika.ExchangeType.DIRECT,
        durable=True,
    )
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
    await queue.bind(exchange, routing_key=QUEUE_NAME)
    logger.info("RabbitMQ queue '%s' declared.", queue.name)

    app.state.rabbit_connection = connection
    app.state.rabbit_channel = channel
    app.state.orders_exchange = exchange

    yield

    logger.info("Closing RabbitMQ connection...")
    await channel.close()
    await connection.close()


app = FastAPI(title="TicketMEI - Order Service", lifespan=lifespan)


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
        order_requests_total.labels(endpoint, str(status_code)).inc()
        order_request_duration_seconds.labels(endpoint).observe(perf_counter() - started_at)


class CreateOrderRequest(BaseModel):
    customer_id: int = Field(..., description="Unique customer ID", example=1001)
    event_id: str = Field(..., description="Unique event ID", example="evt-101")
    quantity: int = Field(..., description="Number of tickets (max 4)", ge=1, le=4, example=2)
    total_amount: Optional[float] = Field(None, description="Total amount in EUR", example=170.0)
    customer_email: Optional[str] = Field(None, description="Customer email address", example="client@example.com")


@app.post("/orders", status_code=status.HTTP_201_CREATED)
async def create_order(order_req: CreateOrderRequest, request: Request):
    orders_initiated_total.labels(status="initiated").inc()
    if order_req.quantity < 1 or order_req.quantity > 4:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Ticket purchase limit exceeded. Maximum 4 tickets allowed per order.",
        )

    customer_email = order_req.customer_email or f"customer_{order_req.customer_id}@ticketmei.pt"
    total_amount = order_req.total_amount if order_req.total_amount is not None else (order_req.quantity * 50.0)
    created_at = datetime.now(timezone.utc).isoformat()
    order_status = "PENDING"

    # 1. Persist order in SQLite database
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO orders (customer_id, event_id, quantity, total_amount, customer_email, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_req.customer_id,
                    order_req.event_id,
                    order_req.quantity,
                    total_amount,
                    customer_email,
                    order_status,
                    created_at,
                ),
            )
            order_id = cursor.lastrowid
            conn.commit()
    except Exception as exc:
        logger.error("Failed to persist order: %s", exc)
        orders_initiated_total.labels(status="FAILED").inc()
        raise HTTPException(status_code=500, detail="Database error while saving order") from exc

    # Payment must succeed before confirming the order or publishing its event.
    try:
        await process_payment(order_id, total_amount)
    except (CircuitBreakerError, PaymentRequestError) as exc:
        circuit_breaker_fallbacks_total.inc()
        with sqlite3.connect(DATABASE_PATH) as conn:
            conn.execute("UPDATE orders SET status = ? WHERE id = ?", ("FAILED", order_id))
            conn.commit()
        logger.warning("Payment unavailable for order %s: %s", order_id, exc)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"error": "Payment system unavailable, try again later"},
        )

    order_status = "CONFIRMED"
    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.execute("UPDATE orders SET status = ? WHERE id = ?", (order_status, order_id))
        conn.commit()

    # 3. Publish OrderCreated message to RabbitMQ after payment confirmation.
    message_payload = {
        "event_type": "OrderCreated",
        "order_id": order_id,
        "customer_id": order_req.customer_id,
        "event_id": order_req.event_id,
        "quantity": order_req.quantity,
        "total_amount": total_amount,
        "customer_email": customer_email,
        "status": order_status,
        "created_at": created_at,
    }

    try:
        exchange: aio_pika.Exchange = request.app.state.orders_exchange
        message = aio_pika.Message(
            body=json.dumps(message_payload).encode("utf-8"),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
        )
        await exchange.publish(message, routing_key=QUEUE_NAME)
        logger.info("Published OrderCreated event to RabbitMQ for order ID %d", order_id)
    except Exception as exc:
        logger.error("Failed to publish message to RabbitMQ: %s", exc)
        # Even if message publication fails, order was registered; increment metric as failed/retry
        orders_initiated_total.labels(status="FAILED").inc()
        raise HTTPException(status_code=503, detail="Message broker unavailable") from exc

    return {
        "message": "Order created successfully",
        "order": {
            "id": order_id,
            "customer_id": order_req.customer_id,
            "event_id": order_req.event_id,
            "quantity": order_req.quantity,
            "total_amount": total_amount,
            "customer_email": customer_email,
            "status": order_status,
            "created_at": created_at,
        },
    }


@app.get("/orders/{order_id}")
async def get_order(order_id: int):
    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
        row = cursor.fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="Order not found")

    return dict(row)


@app.get("/orders")
async def list_orders(limit: int = 50):
    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM orders ORDER BY id DESC LIMIT ?", (limit,))
        rows = cursor.fetchall()

    return [dict(row) for row in rows]


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
