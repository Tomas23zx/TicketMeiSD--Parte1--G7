import asyncio
import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Optional

import aio_pika
from fastapi import FastAPI, HTTPException, Request, Response, status
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("order-service")

DATABASE_PATH = Path(os.getenv("DATABASE_PATH", "/app/data/order.db"))
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
QUEUE_NAME = "orders"

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    connection = await get_rabbitmq_connection()
    channel = await connection.channel()
    # Declare durable queue for orders
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
    logger.info("RabbitMQ queue '%s' declared.", queue.name)

    app.state.rabbit_connection = connection
    app.state.rabbit_channel = channel

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


@app.post("/orders", status_code=status.HTTP_202_ACCEPTED)
async def create_order(order_req: CreateOrderRequest, request: Request):
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

    # 2. Publish OrderCreated message to RabbitMQ
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
        channel: aio_pika.Channel = request.app.state.rabbit_channel
        message = aio_pika.Message(
            body=json.dumps(message_payload).encode("utf-8"),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
        )
        await channel.default_exchange.publish(message, routing_key=QUEUE_NAME)
        logger.info("Published OrderCreated event to RabbitMQ for order ID %d", order_id)
    except Exception as exc:
        logger.error("Failed to publish message to RabbitMQ: %s", exc)
        # Even if message publication fails, order was registered; increment metric as failed/retry
        orders_initiated_total.labels(status="FAILED").inc()
        raise HTTPException(status_code=503, detail="Message broker unavailable") from exc

    orders_initiated_total.labels(status="PENDING").inc()

    return {
        "message": "Order checkout initiated successfully",
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
