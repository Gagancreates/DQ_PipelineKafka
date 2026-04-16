"""
Producer Service
Continuously generates order events with the following mix:
  40% valid   — spread evenly across v1, v2, v3
  40% invalid — bad currency, negative amount, future date, missing customer_id
  20% edge    — unknown schema version, malformed JSON, empty payload
"""

import json
import logging
import random
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/app")

from shared import config
from shared.kafka_utils import flush, make_producer, publish, wait_for_kafka

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ── Helpers ────────────────────────────────────────────────────────────────

def _order_id() -> str:
    return f"ORD-2026-{random.randint(1, 999):03d}"

def _past_date() -> str:
    days_ago = random.randint(1, 365)
    dt = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def _future_date() -> str:
    days_ahead = random.randint(1, 3650)
    dt = datetime.now(timezone.utc) + timedelta(days=days_ahead)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def _customer_id() -> str:
    return f"CUST-{random.randint(1000, 9999)}"

# ── Valid message generators ───────────────────────────────────────────────

def valid_v1() -> dict:
    return {
        "schema_version": "v1",
        "order_id": _order_id(),
        "customer_id": _customer_id(),
        "amount": round(random.uniform(1, 5000), 2),
        "order_date": _past_date(),
    }

def valid_v2() -> dict:
    return {
        "schema_version": "v2",
        "order_id": _order_id(),
        "customer_id": _customer_id(),
        "amount": round(random.uniform(1, 999999), 2),
        "order_date": _past_date(),
        "currency": random.choice(["USD", "EUR", "GBP"]),
    }

def valid_v3() -> dict:
    msg = {
        "schema_version": "v3",
        "order_id": _order_id(),
        "customer_id": _customer_id(),
        "amount": round(random.uniform(1, 999999), 2),
        "order_date": _past_date(),
        "currency": random.choice(["USD", "EUR", "GBP", "JPY", "INR"]),
    }
    if random.random() < 0.5:
        msg["discount_pct"] = round(random.uniform(0, 100), 1)
    return msg

# ── Invalid message generators ─────────────────────────────────────────────

def invalid_bad_currency() -> dict:
    return {
        "schema_version": "v2",
        "order_id": _order_id(),
        "customer_id": _customer_id(),
        "amount": round(random.uniform(1, 5000), 2),
        "order_date": _past_date(),
        "currency": random.choice(["JPY", "INR", "AUD", "CNY", "XYZ"]),
    }

def invalid_negative_amount() -> dict:
    return {
        "schema_version": random.choice(["v1", "v2", "v3"]),
        "order_id": _order_id(),
        "customer_id": _customer_id(),
        "amount": round(random.uniform(-9999, -1), 2),
        "order_date": _past_date(),
        "currency": "USD",
    }

def invalid_future_date() -> dict:
    return {
        "schema_version": random.choice(["v1", "v2", "v3"]),
        "order_id": _order_id(),
        "customer_id": _customer_id(),
        "amount": round(random.uniform(1, 5000), 2),
        "order_date": _future_date(),
        "currency": "EUR",
    }

def invalid_missing_customer() -> dict:
    return {
        "schema_version": random.choice(["v1", "v2"]),
        "order_id": _order_id(),
        "customer_id": "",
        "amount": round(random.uniform(1, 5000), 2),
        "order_date": _past_date(),
        "currency": "GBP",
    }

# ── Edge case generators ───────────────────────────────────────────────────

def edge_unknown_version() -> dict:
    return {
        "schema_version": "v99",
        "order_id": _order_id(),
        "customer_id": _customer_id(),
        "amount": 100.0,
        "order_date": _past_date(),
    }

def edge_malformed() -> bytes:
    """Returns raw bytes that are not valid JSON."""
    return b'{"order_id": "ORD-2026-BAD", "schema_version": "v1", MALFORMED'

def edge_empty() -> bytes:
    return b""

# ── Weighted sampler ───────────────────────────────────────────────────────

VALID_GENERATORS   = [valid_v1, valid_v2, valid_v3]
INVALID_GENERATORS = [invalid_bad_currency, invalid_negative_amount, invalid_future_date, invalid_missing_customer]
EDGE_GENERATORS    = [edge_unknown_version, edge_malformed, edge_empty]

def next_message() -> tuple[dict | None, bytes | None, str]:
    """
    Returns (dict_msg, raw_bytes, label).
    dict_msg is None for raw byte edge cases.
    raw_bytes is None for normal dict messages.
    """
    roll = random.random()

    if roll < 0.40:                      # 40% valid
        gen = random.choice(VALID_GENERATORS)
        msg = gen()
        return msg, None, f"VALID/{msg['schema_version']}"

    elif roll < 0.80:                    # 40% invalid
        gen = random.choice(INVALID_GENERATORS)
        msg = gen()
        return msg, None, f"INVALID/{msg.get('schema_version', '?')}"

    else:                                # 20% edge
        gen = random.choice(EDGE_GENERATORS)
        result = gen()
        if isinstance(result, bytes):
            label = "EDGE/malformed" if result else "EDGE/empty"
            return None, result, label
        else:
            return result, None, f"EDGE/{result.get('schema_version', '?')}"


# ── Main loop ─────────────────────────────────────────────────────────────

def main():
    logger.info(f"Producer starting. Kafka: {config.KAFKA_BOOTSTRAP_SERVERS}")
    wait_for_kafka()

    producer = make_producer()
    interval = config.PRODUCE_INTERVAL_MS / 1000.0
    count = 0

    logger.info(f"Publishing to '{config.TOPIC_RAW_ORDERS}' every {config.PRODUCE_INTERVAL_MS}ms")

    while True:
        dict_msg, raw_bytes, label = next_message()

        try:
            if raw_bytes is not None:
                # Send raw bytes directly (bypasses JSON serialization)
                producer.produce(
                    config.TOPIC_RAW_ORDERS,
                    value=raw_bytes if raw_bytes else None,
                )
                producer.poll(0)
            else:
                key = dict_msg.get("order_id")
                publish(producer, config.TOPIC_RAW_ORDERS, dict_msg, key=key)

            count += 1
            logger.info(f"[#{count}] Sent {label}")

        except Exception as e:
            logger.error(f"Failed to produce message: {e}")

        time.sleep(interval)

        # Flush every 10 messages to keep latency low
        if count % 10 == 0:
            flush(producer)


if __name__ == "__main__":
    main()
