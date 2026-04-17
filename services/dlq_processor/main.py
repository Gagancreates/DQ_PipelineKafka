import json
import logging
import sys
import threading
import time

sys.path.insert(0, "/app")

from confluent_kafka import Consumer, KafkaException

from shared import config
from shared.kafka_utils import flush, make_producer, publish, wait_for_kafka
from shared.models import DLQMessage
from shared.schema_registry import SchemaRegistry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DLQ-PROCESSOR] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

MAX_RETRIES = 3

# In-memory buffer: dlq_id -> DLQMessage
_pending: dict[str, DLQMessage] = {}
_lock = threading.Lock()

registry: SchemaRegistry = None
producer = None


# ── Reprocessing ──────────────────────────────────────────────────────────

def reprocess_all() -> None:
    latest = registry.get_latest_version()
    logger.info(f"Reprocessing DLQ against latest schema: {latest}")

    with _lock:
        pending_snapshot = list(_pending.values())

    resolved, still_failing, dropped = 0, 0, 0

    for dlq_msg in pending_snapshot:
        result = registry.validate_against_latest(dlq_msg.original_message)

        if result.is_valid:
            # Publish to valid.orders with reprocessed flag
            outgoing = {
                **dlq_msg.original_message,
                "schema_version": latest,
                "_reprocessed": True,
                "_dlq_id": dlq_msg.dlq_id,
            }
            publish(
                producer,
                config.TOPIC_VALID_ORDERS,
                outgoing,
                key=dlq_msg.original_message.get("order_id"),
            )
            with _lock:
                _pending.pop(dlq_msg.dlq_id, None)
            resolved += 1
            logger.info(
                f"RESOLVED dlq_id={dlq_msg.dlq_id} "
                f"order_id={dlq_msg.original_message.get('order_id', '?')} "
                f"-> valid.orders"
            )

        else:
            dlq_msg.retry_count += 1
            if dlq_msg.retry_count >= MAX_RETRIES:
                with _lock:
                    _pending.pop(dlq_msg.dlq_id, None)
                dropped += 1
                logger.warning(
                    f"DROPPED dlq_id={dlq_msg.dlq_id} after {MAX_RETRIES} retries. "
                    f"Errors: {result.errors}"
                )
            else:
                still_failing += 1
                logger.info(
                    f"STILL FAILING dlq_id={dlq_msg.dlq_id} "
                    f"retry={dlq_msg.retry_count}/{MAX_RETRIES} "
                    f"errors={result.errors}"
                )

    flush(producer)
    logger.info(
        f"Reprocess complete — resolved={resolved} still_failing={still_failing} dropped={dropped}"
    )


# ── Consumer loop ─────────────────────────────────────────────────────────

def run(stop_event: threading.Event) -> None:
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": config.CONSUMER_GROUP_DLQ_PROCESSOR,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
        "session.timeout.ms": 30000,
    })

    consumer.subscribe([config.TOPIC_DLQ, config.TOPIC_SCHEMA_UPDATES])
    logger.info(f"Subscribed to [{config.TOPIC_DLQ}, {config.TOPIC_SCHEMA_UPDATES}]")

    try:
        while not stop_event.is_set():
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            topic = msg.topic()
            raw = msg.value()

            if not raw:
                continue

            try:
                data = json.loads(raw.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning(f"Could not decode message on {topic}: {e}")
                continue

            if topic == config.TOPIC_DLQ:
                # Buffer incoming DLQ message
                try:
                    dlq_msg = DLQMessage.from_dict(data)
                    with _lock:
                        _pending[dlq_msg.dlq_id] = dlq_msg
                    logger.debug(f"Buffered dlq_id={dlq_msg.dlq_id} (pending={len(_pending)})")
                except (KeyError, TypeError) as e:
                    logger.warning(f"Malformed DLQ message, skipping: {e}")

            elif topic == config.TOPIC_SCHEMA_UPDATES:
                # New schema registered — trigger reprocessing
                new_version = data.get("version", "unknown")
                logger.info(f"Schema update received: version={new_version} — triggering reprocess")

                # Update registry with new schema if payload included
                if "schema_doc" in data:
                    registry.register_version(new_version, data["schema_doc"])

                reprocess_all()

    except KafkaException as e:
        logger.error(f"Kafka exception: {e}")
    finally:
        consumer.close()
        logger.info("Consumer closed")


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    global registry, producer

    logger.info(f"DLQ Processor starting. Kafka: {config.KAFKA_BOOTSTRAP_SERVERS}")
    wait_for_kafka()

    registry = SchemaRegistry(config.SCHEMAS_DIR)
    producer = make_producer()

    logger.info(f"Schema versions loaded: {registry.list_versions()}")

    stop_event = threading.Event()
    try:
        run(stop_event)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        stop_event.set()
    finally:
        flush(producer)


if __name__ == "__main__":
    main()
