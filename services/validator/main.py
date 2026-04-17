import logging
import sys
import threading
import time
from datetime import datetime, timezone

sys.path.insert(0, "/app")

from shared import config
from shared.kafka_utils import (
    flush,
    make_consumer,
    make_producer,
    publish,
    wait_for_kafka,
)
from shared.metrics_store import store
from shared.models import DLQMessage
from shared.schema_registry import SchemaRegistry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [VALIDATOR] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# Shared registry and semaphore — initialized in main()
registry: SchemaRegistry = None
semaphore: threading.Semaphore = None
producer = None


# Handler

def handle_message(message: dict, kafka_msg=None, consumer=None) -> None:
    """Called for every decoded JSON message from raw.orders."""
    acquired = semaphore.acquire(timeout=5)
    if not acquired:
        logger.warning("Backpressure: semaphore timeout, dropping message")
        return

    try:
        result = registry.validate(message)
        version = result.schema_version or message.get("schema_version", "unknown")

        if result.is_valid:
            publish(producer, config.TOPIC_VALID_ORDERS, message, key=message.get("order_id"))
            store.record_valid(version)
            logger.info(f"VALID   [{version}] order_id={message.get('order_id', '?')}")

        else:
            failed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            dlq_msg = DLQMessage.create(
                original=message,
                errors=result.errors,
                schema_version=version,
                failed_at=failed_at,
            )
            publish(producer, config.TOPIC_DLQ, dlq_msg.to_dict(), key=dlq_msg.dlq_id)
            store.record_invalid(version, result.errors, message)
            store.add_dlq_message(dlq_msg.to_dict())
            logger.info(
                f"INVALID [{version}] order_id={message.get('order_id', '?')} "
                f"errors={result.errors}"
            )

        # Commit offset only after message is fully processed (at-least-once delivery)
        if kafka_msg and consumer:
            consumer.commit(kafka_msg)

    finally:
        semaphore.release()


def handle_raw(data: dict) -> None:
    """Thin wrapper — lets consume_loop call us with the decoded dict."""
    handle_message(data)


# Consumer loop with backpressure

def run_consumer(stop_event: threading.Event) -> None:
    from confluent_kafka import Consumer
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": config.CONSUMER_GROUP_VALIDATOR,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,   # manual commit for at-least-once delivery
        "session.timeout.ms": 30000,
    })
    consumer.subscribe([config.TOPIC_RAW_ORDERS])

    logger.info(f"Subscribed to '{config.TOPIC_RAW_ORDERS}'")

    try:
        while not stop_event.is_set():
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            raw_value = msg.value()

            # Handle empty payload edge case
            if not raw_value:
                logger.warning("Empty message received, routing to DLQ")
                failed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                dlq_msg = DLQMessage.create(
                    original={},
                    errors=["Empty message payload"],
                    schema_version="unknown",
                    failed_at=failed_at,
                )
                publish(producer, config.TOPIC_DLQ, dlq_msg.to_dict())
                store.record_invalid("unknown", ["Empty message payload"])
                store.add_dlq_message(dlq_msg.to_dict())
                consumer.commit(msg)
                continue

            # Decode JSON — handle malformed edge case
            try:
                import json
                data = json.loads(raw_value.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning(f"Malformed JSON, routing to DLQ: {e}")
                failed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                dlq_msg = DLQMessage.create(
                    original={"raw": raw_value.decode("utf-8", errors="replace")},
                    errors=[f"Malformed JSON: {e}"],
                    schema_version="unknown",
                    failed_at=failed_at,
                )
                publish(producer, config.TOPIC_DLQ, dlq_msg.to_dict())
                store.record_invalid("unknown", [f"Malformed JSON: {e}"])
                store.add_dlq_message(dlq_msg.to_dict())
                consumer.commit(msg)
                continue

            # Backpressure check: if semaphore is fully occupied, pause partition
            if semaphore._value == 0:
                consumer.pause(consumer.assignment())
                logger.warning("Backpressure: pausing consumer partition")
                # Wait until a slot frees up
                while semaphore._value == 0 and not stop_event.is_set():
                    time.sleep(0.1)
                consumer.resume(consumer.assignment())
                logger.info("Backpressure: resuming consumer partition")

            # Process in a thread so we don't block the poll loop
            # Pass msg so the thread can commit offset after processing (at-least-once)
            t = threading.Thread(target=handle_message, args=(data, msg, consumer), daemon=True)
            t.start()

    finally:
        consumer.close()
        logger.info("Consumer closed")


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    global registry, semaphore, producer

    logger.info(f"Validator starting. Kafka: {config.KAFKA_BOOTSTRAP_SERVERS}")
    wait_for_kafka()

    registry  = SchemaRegistry(config.SCHEMAS_DIR)
    semaphore = threading.Semaphore(config.MAX_CONCURRENT_VALIDATIONS)
    producer  = make_producer()

    logger.info(f"Schema versions loaded: {registry.list_versions()}")
    logger.info(f"Max concurrent validations: {config.MAX_CONCURRENT_VALIDATIONS}")

    stop_event = threading.Event()
    try:
        run_consumer(stop_event)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        stop_event.set()
    finally:
        flush(producer)


if __name__ == "__main__":
    main()
