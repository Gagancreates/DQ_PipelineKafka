import json
import logging
import time
from typing import Any, Callable

from confluent_kafka import Consumer, Producer, KafkaException

from . import config

logger = logging.getLogger(__name__)


# ── Producer ───────────────────────────────────────────────────────────────

def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "linger.ms": 10,
    })


def publish(producer: Producer, topic: str, message: dict[str, Any], key: str | None = None) -> None:
    """Serialize and publish a message. Blocks until delivery confirmed."""
    payload = json.dumps(message).encode("utf-8")
    key_bytes = key.encode("utf-8") if key else None

    def on_delivery(err, msg):
        if err:
            logger.error(f"Delivery failed to {topic}: {err}")
        else:
            logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

    producer.produce(topic, value=payload, key=key_bytes, on_delivery=on_delivery)
    producer.poll(0)  # trigger callbacks without blocking


def flush(producer: Producer, timeout: float = 10.0) -> None:
    producer.flush(timeout)


# ── Consumer ───────────────────────────────────────────────────────────────

def make_consumer(group_id: str, topics: list[str], auto_offset_reset: str = "earliest") -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 30000,
    })
    consumer.subscribe(topics)
    return consumer


def consume_loop(
    consumer: Consumer,
    handler: Callable[[dict[str, Any]], None],
    poll_timeout: float = 1.0,
    stop_event=None,
) -> None:
    """
    Consume messages in a loop, calling handler for each valid JSON message.
    Stops when stop_event is set (or runs forever if None).
    """
    try:
        while stop_event is None or not stop_event.is_set():
            msg = consumer.poll(poll_timeout)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            try:
                data = json.loads(msg.value().decode("utf-8"))
                handler(data)
            except json.JSONDecodeError as e:
                logger.warning(f"Non-JSON message received, skipping: {e}")
            except Exception as e:
                logger.error(f"Handler error: {e}", exc_info=True)
    except KafkaException as e:
        logger.error(f"Kafka exception in consume loop: {e}")
    finally:
        consumer.close()


def wait_for_kafka(retries: int = 20, delay: float = 3.0) -> None:
    """Block until Kafka is reachable. Used at service startup."""
    from confluent_kafka.admin import AdminClient
    client = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    for attempt in range(1, retries + 1):
        try:
            meta = client.list_topics(timeout=5)
            if meta:
                logger.info(f"Kafka reachable at {config.KAFKA_BOOTSTRAP_SERVERS}")
                return
        except Exception as e:
            logger.warning(f"Waiting for Kafka (attempt {attempt}/{retries}): {e}")
        time.sleep(delay)
    raise RuntimeError(f"Kafka not reachable after {retries} attempts")
