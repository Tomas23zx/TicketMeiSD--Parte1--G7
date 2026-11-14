import asyncio
import logging
import os
import random
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from time import perf_counter

from fastapi import FastAPI, HTTPException, Request, Response, status
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("payment-service")

DATABASE_PATH = Path(os.getenv("DATABASE_PATH", "/app/data/payment.db"))

# As configurações de falha são mantidas em memória e aplicadas aos pedidos seguintes.
# A taxa de erro é expressa em percentagem e a latência em segundos.
fault_config = {
    "error_rate": 0.0,
    "latency_seconds": 0.0,
}

payment_requests_total = Counter(
    "payment_requests_total",
    "Total requests processed by the Payment service",
    ["endpoint", "http_status"],
)
payment_processing_duration_seconds = Histogram(
    "payment_processing_duration_seconds",
    "Time spent processing payment requests in seconds",
)


def init_db() -> None:
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DATABASE_PATH) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS Payment (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('SUCCESS', 'FAILED')),
                processed_at TIMESTAMP NOT NULL
            )
            """
        )
        connection.commit()
    logger.info("Payment database initialized at %s", DATABASE_PATH)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="TicketMEI - Payment Service", lifespan=lifespan)


@app.middleware("http")
async def observe_requests(request: Request, call_next):
    started_at = perf_counter()
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        endpoint = request.scope.get("route")
        endpoint_path = endpoint.path if endpoint else request.url.path
        payment_requests_total.labels(endpoint_path, str(status_code)).inc()
        if request.url.path == "/payments":
            payment_processing_duration_seconds.observe(perf_counter() - started_at)


class ConfigureFaultsRequest(BaseModel):
    error_rate: float = Field(0.0, ge=0.0, le=100.0, description="Failure percentage")
    latency_seconds: float = Field(0.0, ge=0.0, description="Artificial delay in seconds")


class PaymentStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class CreatePaymentRequest(BaseModel):
    order_id: int = Field(..., gt=0)
    amount: float = Field(..., gt=0)


class PaymentResponse(BaseModel):
    id: int
    order_id: int
    amount: float
    status: PaymentStatus
    processed_at: datetime


@app.post("/payments/configure-faults")
async def configure_faults(configuration: ConfigureFaultsRequest):
    # A substituição destes valores altera imediatamente as falhas dos novos pedidos.
    fault_config["error_rate"] = configuration.error_rate
    fault_config["latency_seconds"] = configuration.latency_seconds
    return {
        "error_rate": fault_config["error_rate"],
        "latency_seconds": fault_config["latency_seconds"],
    }


@app.post("/payments", status_code=status.HTTP_201_CREATED, response_model=PaymentResponse)
async def create_payment(payment: CreatePaymentRequest):
    latency_seconds = fault_config["latency_seconds"]
    if latency_seconds:
        # O atraso é aplicado antes da decisão de falha ou da operação na base de dados.
        await asyncio.sleep(latency_seconds)

    # Um valor aleatório abaixo da percentagem configurada simula uma falha de processamento.
    # Os pedidos falhados devolvem HTTP 500 e não são guardados como pagamentos.
    if random.random() < fault_config["error_rate"] / 100:
        raise HTTPException(status_code=500, detail="Payment processing failed")

    processed_at = datetime.now(timezone.utc)
    try:
        with sqlite3.connect(DATABASE_PATH) as connection:
            cursor = connection.execute(
                """
                INSERT INTO Payment (order_id, amount, status, processed_at)
                VALUES (?, ?, ?, ?)
                """,
                (payment.order_id, payment.amount, PaymentStatus.SUCCESS.value, processed_at.isoformat()),
            )
            payment_id = cursor.lastrowid
            connection.commit()
    except sqlite3.Error as exc:
        logger.error("Failed to persist payment: %s", exc)
        raise HTTPException(status_code=500, detail="Database error while saving payment") from exc

    return {
        "id": payment_id,
        "order_id": payment.order_id,
        "amount": payment.amount,
        "status": PaymentStatus.SUCCESS,
        "processed_at": processed_at,
    }


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
