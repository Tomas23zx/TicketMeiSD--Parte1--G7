import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from time import perf_counter

from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("inventory-service")

DATABASE_PATH = Path(os.getenv("DATABASE_PATH", "/app/data/inventory.db"))
EVENTS_FILE = Path(os.getenv("EVENTS_FILE", "/app/data/events.json"))

inventory_requests_total = Counter(
    "inventory_requests_total",
    "Total requests processed by the Inventory service",
    ["endpoint", "http_status"],
)
inventory_request_duration_seconds = Histogram(
    "inventory_request_duration_seconds",
    "Duration of Inventory service requests in seconds",
    ["endpoint"],
)
inventory_available_tickets = Gauge(
    "inventory_available_tickets",
    "Currently available tickets for an event",
    ["event_id"],
)


class StockRequest(BaseModel):
    event_id: str
    total_tickets: int = Field(..., ge=0)


class QuantityRequest(BaseModel):
    event_id: str
    quantity: int = Field(1, ge=1)


def init_db() -> None:
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DATABASE_PATH) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS inventory (
                event_id TEXT PRIMARY KEY,
                total_tickets INTEGER NOT NULL CHECK (total_tickets >= 0),
                available_tickets INTEGER NOT NULL CHECK (available_tickets >= 0)
            )
            """
        )
        if EVENTS_FILE.exists():
            events = json.loads(EVENTS_FILE.read_text(encoding="utf-8"))
            for event in events:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO inventory (event_id, total_tickets, available_tickets)
                    VALUES (?, ?, ?)
                    """,
                    (event["id"], event["total_tickets"], event["total_tickets"]),
                )
        connection.commit()

        rows = connection.execute(
            "SELECT event_id, available_tickets FROM inventory"
        ).fetchall()
    for event_id, available in rows:
        inventory_available_tickets.labels(event_id=event_id).set(available)
    logger.info("Inventory database initialized at %s", DATABASE_PATH)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="TicketMEI - Inventory Service", lifespan=lifespan)


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
        inventory_requests_total.labels(endpoint, str(status_code)).inc()
        inventory_request_duration_seconds.labels(endpoint).observe(perf_counter() - started_at)


@app.post("/inventory/stock")
def set_stock(stock: StockRequest):
    with sqlite3.connect(DATABASE_PATH, timeout=10) as connection:
        connection.execute(
            """
            INSERT INTO inventory (event_id, total_tickets, available_tickets)
            VALUES (?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                total_tickets = excluded.total_tickets,
                available_tickets = excluded.available_tickets
            """,
            (stock.event_id, stock.total_tickets, stock.total_tickets),
        )
        connection.commit()
    inventory_available_tickets.labels(event_id=stock.event_id).set(stock.total_tickets)
    return {
        "event_id": stock.event_id,
        "total_tickets": stock.total_tickets,
        "available_tickets": stock.total_tickets,
    }


@app.post("/inventory/reserve")
def reserve(request: QuantityRequest):
    with sqlite3.connect(DATABASE_PATH, timeout=10) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT total_tickets, available_tickets FROM inventory WHERE event_id = ?",
            (request.event_id,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Event not found in inventory")

        total_tickets, available_tickets = row
        if available_tickets < request.quantity:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "Insufficient tickets",
                    "event_id": request.event_id,
                    "available_tickets": available_tickets,
                },
            )

        available_tickets -= request.quantity
        connection.execute(
            "UPDATE inventory SET available_tickets = ? WHERE event_id = ?",
            (available_tickets, request.event_id),
        )
        connection.commit()

    inventory_available_tickets.labels(event_id=request.event_id).set(available_tickets)
    return {
        "event_id": request.event_id,
        "total_tickets": total_tickets,
        "available_tickets": available_tickets,
        "reserved": request.quantity,
    }


@app.post("/inventory/release")
def release(request: QuantityRequest):
    with sqlite3.connect(DATABASE_PATH, timeout=10) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT total_tickets, available_tickets FROM inventory WHERE event_id = ?",
            (request.event_id,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Event not found in inventory")

        total_tickets, available_tickets = row
        available_tickets = min(total_tickets, available_tickets + request.quantity)
        connection.execute(
            "UPDATE inventory SET available_tickets = ? WHERE event_id = ?",
            (available_tickets, request.event_id),
        )
        connection.commit()

    inventory_available_tickets.labels(event_id=request.event_id).set(available_tickets)
    return {
        "event_id": request.event_id,
        "total_tickets": total_tickets,
        "available_tickets": available_tickets,
        "released": request.quantity,
    }


@app.get("/inventory/{event_id}")
def get_inventory(event_id: str):
    with sqlite3.connect(DATABASE_PATH) as connection:
        row = connection.execute(
            "SELECT event_id, total_tickets, available_tickets FROM inventory WHERE event_id = ?",
            (event_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Event not found in inventory")
    return {
        "event_id": row[0],
        "total_tickets": row[1],
        "available_tickets": row[2],
    }


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
