"""Webhook ingestion: acknowledge quickly, process exactly once, deliver later.

The request path only validates the idempotency key and commits one SQLite row.
CPU-bound PII scrubbing runs in a process pool so it cannot hold the GIL, and
downstream delivery runs on the event loop with retries.
"""

import asyncio
import logging
import os
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.database import Database
from app.downstream import DownstreamClient, PermanentDeliveryError
from app.processor import init_pool_worker, pool_warmup, scrub_and_fingerprint

logger = logging.getLogger(__name__)

# Leave one core for the event loop and the load generator.
_CPU = os.cpu_count() or 2
POOL_WORKERS = max(1, min(4, _CPU - 1))
DELIVERY_BATCH = 32
POLL_SECONDS = 0.05
# Outer attempts, after the HTTP client has already retried one 503 burst.
# At a 2s cap this still covers a downstream outage of about a minute.
MAX_DELIVERY_ATTEMPTS = 40

db = Database()
downstream = DownstreamClient()
_executor: ProcessPoolExecutor | None = None
_stop = asyncio.Event()
_worker_task: asyncio.Task | None = None


class WebhookPayload(BaseModel):
    event_id: str
    tenant_id: str
    content: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _executor, _worker_task
    _stop.clear()
    await db.connect()
    await db.init_schema()
    # A previous process may have died after claiming a row and before delivery.
    await db.reclaim_processing()
    await downstream.start()

    _executor = ProcessPoolExecutor(max_workers=POOL_WORKERS, initializer=init_pool_worker)
    loop = asyncio.get_running_loop()
    # Spawn workers now. The first submit() otherwise blocks the loop on Windows.
    await asyncio.gather(
        *(loop.run_in_executor(_executor, pool_warmup) for _ in range(POOL_WORKERS))
    )

    _worker_task = asyncio.create_task(delivery_loop())
    try:
        yield
    finally:
        _stop.set()
        if _worker_task is not None:
            _worker_task.cancel()
            await asyncio.gather(_worker_task, return_exceptions=True)
            _worker_task = None
        await downstream.aclose()
        if _executor is not None:
            _executor.shutdown(wait=False, cancel_futures=True)
            _executor = None
        await db.close()


app = FastAPI(title="Webhook Ingestion Service", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/webhooks/ingest")
async def ingest_webhook(
    payload: WebhookPayload,
    x_idempotency_key: str | None = Header(default=None),
):
    if not x_idempotency_key:
        raise HTTPException(status_code=400, detail="Missing X-Idempotency-Key header")

    inserted = await db.insert_pending(
        event_id=payload.event_id,
        idempotency_key=x_idempotency_key,
        tenant_id=payload.tenant_id,
        raw_content=payload.content,
    )
    # 202 in both cases: delivery is not on the request path. A duplicate
    # retry must not enqueue the event a second time.
    return JSONResponse(
        status_code=202,
        content={
            "status": "accepted" if inserted else "duplicate",
            "event_id": payload.event_id,
        },
    )


async def delivery_loop() -> None:
    while not _stop.is_set():
        try:
            claimed = await db.claim_due(limit=DELIVERY_BATCH)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("failed to claim webhook events")
            await asyncio.sleep(0.1)
            continue

        if not claimed:
            await asyncio.sleep(POLL_SECONDS)
            continue

        await asyncio.gather(*(process_claimed(event_id) for event_id in claimed))


async def process_claimed(event_id: str) -> None:
    try:
        await _process_claimed(event_id)
    except asyncio.CancelledError:
        raise
    except PermanentDeliveryError:
        logger.exception("permanent delivery failure for %s", event_id)
        await db.mark_failed(event_id)
    except Exception:
        logger.exception("delivery failed for %s", event_id)
        await _backoff(event_id)


async def _process_claimed(event_id: str) -> None:
    row = await db.get_event(event_id)
    if row is None or row["status"] == "delivered":
        return

    cleaned = row["cleaned_text"]
    fingerprint = row["fingerprint"]
    if not fingerprint:
        raw = row["raw_content"]
        if not raw:
            raise PermanentDeliveryError(f"event {event_id} has no payload to process")
        if _executor is None:
            raise RuntimeError("process pool is not running")
        # The 120k SHA-256 loop holds the GIL. A worker thread would still
        # stall /health; a child process has its own interpreter.
        cleaned, fingerprint = await asyncio.get_running_loop().run_in_executor(
            _executor, scrub_and_fingerprint, raw
        )
        await db.save_cleaned(event_id, cleaned, fingerprint)

    await downstream.forward(event_id, cleaned, fingerprint)
    await db.mark_delivered(event_id)


async def _backoff(event_id: str) -> None:
    row = await db.get_event(event_id)
    attempts = int(row["attempts"]) if row is not None else MAX_DELIVERY_ATTEMPTS
    if attempts >= MAX_DELIVERY_ATTEMPTS:
        await db.mark_failed(event_id)
        return
    delay = min(0.05 * (2 ** min(attempts, 6)), 2.0)
    await db.schedule_retry(event_id, delay)
