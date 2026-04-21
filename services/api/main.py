"""
API + Dashboard Service

REST endpoints for metrics, schema management, and DLQ control.
Also serves the live dashboard at GET /.

Builds metrics by observing valid.orders and invalid.orders.dlq
as a background Kafka consumer — no shared memory with other services.
"""

import json
import logging
import sys
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

sys.path.insert(0, "/app")

from shared import config
from shared.kafka_utils import make_producer, publish, wait_for_kafka
from shared.metrics_store import MetricsStore
from shared.models import DLQMessage
from shared.schema_registry import SchemaRegistry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [API] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ── Singletons ────────────────────────────────────────────────────────────

store    = MetricsStore()
registry = SchemaRegistry(config.SCHEMAS_DIR)
producer = None


# ── Background metric observers ───────────────────────────────────────────

def _make_observer(base_group_id: str, topics: list[str]) -> "Consumer":
    """
    Each API startup uses a unique group ID (base + run ID).
    This guarantees no committed offsets exist in Kafka, so
    auto.offset.reset=earliest always replays the full topic history
    and the in-memory MetricsStore is rebuilt correctly.
    """
    import uuid
    from confluent_kafka import Consumer
    run_id = uuid.uuid4().hex[:8]
    group_id = f"{base_group_id}-{run_id}"
    c = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c.subscribe(topics)
    logger.info(f"Observer group: {group_id}")
    return c


def _observe_valid_orders(stop: threading.Event) -> None:
    """Consume valid.orders to track valid + reprocessed counts."""
    consumer = _make_observer("dq-api-valid-observer", [config.TOPIC_VALID_ORDERS])

    try:
        while not stop.is_set():
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                data = json.loads(msg.value().decode("utf-8"))
                version = data.get("schema_version", "unknown")
                event_id = data.get("event_id")
                if store.has_seen_valid_event(event_id):
                    continue
                store.mark_valid_event_seen(event_id)
                if data.get("_reprocessed"):
                    store.record_reprocessed(data.get("_dlq_id", "unknown"), version)
                else:
                    store.record_valid(version)
            except Exception:
                pass
    finally:
        consumer.close()


def _observe_dlq(stop: threading.Event) -> None:
    """Consume invalid.orders.dlq to track invalid counts + DLQ state."""
    consumer = _make_observer("dq-api-dlq-observer", [config.TOPIC_DLQ])

    try:
        while not stop.is_set():
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                data = json.loads(msg.value().decode("utf-8"))
                dlq_msg = DLQMessage.from_dict(data)
                store.record_invalid(
                    dlq_msg.schema_version,
                    dlq_msg.error_details,
                    dlq_msg.original_message,
                )
                store.add_dlq_message(dlq_msg.to_dict())
            except Exception:
                pass
    finally:
        consumer.close()


# ── App lifecycle ─────────────────────────────────────────────────────────

_stop_event = threading.Event()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    logger.info("API starting up...")
    wait_for_kafka()
    producer = make_producer()

    t1 = threading.Thread(target=_observe_valid_orders, args=(_stop_event,), daemon=True)
    t2 = threading.Thread(target=_observe_dlq,          args=(_stop_event,), daemon=True)
    t1.start()
    t2.start()
    logger.info("Background observers started")

    yield

    logger.info("API shutting down...")
    _stop_event.set()


app = FastAPI(title="DQ Pipeline API", version="1.0.0", lifespan=lifespan)

# Serve dashboard static files
app.mount("/static", StaticFiles(directory="/app/static"), name="static")


# ── Routes ────────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse("/app/static/dashboard.html")


@app.get("/api/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/api/metrics")
def get_metrics():
    return store.get_summary()


@app.get("/api/metrics/schema/{version}")
def get_metrics_by_version(version: str):
    data = store.get_version_summary(version)
    if not data:
        raise HTTPException(status_code=404, detail=f"No metrics for version '{version}'")
    return {"version": version, **data}


@app.get("/api/metrics/throughput")
def get_throughput():
    return {"series": store.get_throughput_series()}


@app.get("/api/violations")
def get_violations():
    return {"violations": store.get_violations()}


@app.get("/api/dlq")
def get_dlq():
    return store.get_dlq_state()


@app.get("/api/schemas")
def list_schemas():
    versions = registry.list_versions()
    return {
        "versions": [registry.get_schema_info(v) for v in versions],
        "latest": registry.get_latest_version(),
    }


@app.post("/api/schemas", status_code=201)
def register_schema(payload: dict):
    """
    Register a new schema version and trigger DLQ reprocessing.

    Body: { "version": "v3", "schema_doc": { ...full JSON Schema... } }
    The schema_doc is optional — if omitted, the version is registered
    using whatever is already on disk (useful for signalling only).
    """
    version = payload.get("version")
    if not version:
        raise HTTPException(status_code=400, detail="'version' field is required")

    schema_doc = payload.get("schema_doc")
    if schema_doc:
        registry.register_version(version, schema_doc)

    store.record_schema_registered(version)

    # Signal DLQ processor to reprocess
    signal = {
        "version": version,
        "triggered_at": datetime.now(timezone.utc).isoformat(),
    }
    if schema_doc:
        signal["schema_doc"] = schema_doc

    publish(producer, config.TOPIC_SCHEMA_UPDATES, signal, key=version)
    logger.info(f"Schema '{version}' registered. Reprocess signal sent.")

    return {
        "message": f"Schema '{version}' registered. DLQ reprocessing triggered.",
        "version": version,
    }


@app.post("/api/reprocess")
def manual_reprocess():
    """Manually trigger DLQ reprocessing using the current latest schema."""
    latest = registry.get_latest_version()
    if not latest:
        raise HTTPException(status_code=400, detail="No schemas registered")

    signal = {
        "version": latest,
        "triggered_at": datetime.now(timezone.utc).isoformat(),
        "manual": True,
    }
    publish(producer, config.TOPIC_SCHEMA_UPDATES, signal, key=latest)
    logger.info(f"Manual reprocess triggered using schema '{latest}'")

    return {
        "message": f"Reprocess triggered using schema '{latest}'",
        "version": latest,
    }


# ── Entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=config.API_HOST,
        port=config.API_PORT,
        log_level="info",
    )
