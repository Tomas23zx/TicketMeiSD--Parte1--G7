import asyncio
import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

import aio_pika
from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("notification-service")

DATABASE_PATH = Path(os.getenv("DATABASE_PATH", "/app/data/notification.db"))
LOGS_FILE = Path(os.getenv("LOGS_FILE", "/app/data/notification_logs.json"))
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
QUEUE_NAME = "orders"
SIMULATED_EMAIL_DELAY_SECONDS = 0.1  # 100 ms as required by specification

# Prometheus metrics
notifications_processed_total = Counter(
    "notifications_processed_total",
    "Total number of notifications processed",
    ["status"],
)
notification_processing_duration_seconds = Histogram(
    "notification_processing_duration_seconds",
    "Duration of notification processing in seconds",
)


def init_storage():
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOGS_FILE.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                email TEXT NOT NULL,
                status TEXT NOT NULL,
                duration_seconds REAL NOT NULL,
                sent_at TEXT NOT NULL
            )
            """
        )
        conn.commit()

    if not LOGS_FILE.exists():
        LOGS_FILE.write_text("[]", encoding="utf-8")

    logger.info("Notification DB ready at %s and log file at %s", DATABASE_PATH, LOGS_FILE)


def append_notification_log(entry: dict):
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            conn.execute(
                """
                INSERT INTO notifications (order_id, email, status, duration_seconds, sent_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    entry["order_id"],
                    entry["email"],
                    entry["status"],
                    entry["duration_seconds"],
                    entry["sent_at"],
                ),
            )
            conn.commit()
    except Exception as exc:
        logger.error("Failed to insert notification into DB: %s", exc)

    # Append to notification_logs.json as JSON line or JSON list entry
    try:
        with open(LOGS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as exc:
        logger.error("Failed to append to %s: %s", LOGS_FILE, exc)


async def consume_orders(shutdown_event: asyncio.Event):
    while not shutdown_event.is_set():
        try:
            logger.info("Connecting to RabbitMQ at %s...", RABBITMQ_URL)
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            async with connection:
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=10)
                queue = await channel.declare_queue(QUEUE_NAME, durable=True)
                logger.info("Listening for messages on queue '%s'...", queue.name)

                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        if shutdown_event.is_set():
                            break

                        started_at = perf_counter()
                        delivery_status = "SENT"
                        order_id = -1
                        email = "unknown@ticketmei.pt"

                        try:
                            async with message.process():
                                payload = json.loads(message.body.decode("utf-8"))
                                order_id = payload.get("order_id", -1)
                                email = payload.get("customer_email") or payload.get("email", "unknown@ticketmei.pt")

                                logger.info(
                                    "Processing notification for Order #%s (Recipient: %s)...",
                                    order_id,
                                    email,
                                )

                                # Simula o envio de um email com um intervalo de tempo de 100 ms
                                await asyncio.sleep(SIMULATED_EMAIL_DELAY_SECONDS)

                                duration = perf_counter() - started_at
                                notifications_processed_total.labels(status="SENT").inc()
                                notification_processing_duration_seconds.observe(duration)

                                log_entry = {
                                    "order_id": order_id,
                                    "email": email,
                                    "status": "SENT",
                                    "duration_seconds": round(duration, 4),
                                    "sent_at": datetime.now(timezone.utc).isoformat(),
                                    "details": f"Confirmation email sent for Order #{order_id}",
                                }
                                append_notification_log(log_entry)
                                logger.info(
                                    "Notification for Order #%s dispatched successfully in %.3fs",
                                    order_id,
                                    duration,
                                )
                        except Exception as exc:
                            logger.error("Error processing notification message: %s", exc)
                            duration = perf_counter() - started_at
                            notifications_processed_total.labels(status="FAILED").inc()
                            notification_processing_duration_seconds.observe(duration)

                            log_entry = {
                                "order_id": order_id,
                                "email": email,
                                "status": "FAILED",
                                "duration_seconds": round(duration, 4),
                                "sent_at": datetime.now(timezone.utc).isoformat(),
                                "error": str(exc),
                            }
                            append_notification_log(log_entry)

        except asyncio.CancelledError:
            logger.info("Notification consumer task cancelled.")
            break
        except Exception as exc:
            if shutdown_event.is_set():
                break
            logger.warning("RabbitMQ consumer error: %s. Reconnecting in 3 seconds...", exc)
            await asyncio.sleep(3.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_storage()
    shutdown_event = asyncio.Event()
    consumer_task = asyncio.create_task(consume_orders(shutdown_event))
    app.state.shutdown_event = shutdown_event
    app.state.consumer_task = consumer_task

    yield

    logger.info("Stopping notification consumer...")
    shutdown_event.set()
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass
    logger.info("Notification service stopped gracefully.")


app = FastAPI(title="TicketMEI - Notification Service", lifespan=lifespan)


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/notifications")
async def list_notifications(limit: int = 50):
    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM notifications ORDER BY id DESC LIMIT ?", (limit,))
        rows = cursor.fetchall()
    return [dict(row) for row in rows]


@app.get("/health")
async def health():
    return {"status": "ok"}
