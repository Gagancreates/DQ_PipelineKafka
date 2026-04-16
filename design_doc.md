# Design Document — DQ Pipeline (Kafka + Python)

## 1. Architecture Overview

<img width="1079" height="976" alt="Image" src="https://github.com/user-attachments/assets/3ec8458c-9c2c-4333-b6f7-7e8f131b2d85" />

---

## 2. Component Design

### 2.1 Producer
Generates realistic order events continuously at a configurable interval (`PRODUCE_INTERVAL_MS`, default 800ms).

Message distribution:
- **40% valid** — evenly spread across v1, v2, v3 to demonstrate cross-version tracking
- **40% invalid** — bad currency (JPY/INR/CNY in v2), negative amount, future order_date, empty customer_id
- **20% edge** — unknown schema version (v99), malformed JSON, empty payload

The producer uses `order_id` as the Kafka partition key to ensure ordering per order across the pipeline.

### 2.2 Validator
Two-phase validation on every message:

**Phase 1 — JSON Schema check** (`jsonschema` library)
Validates structure: required fields, field types, string length constraints. Fast, declarative, version-specific.

**Phase 2 — Business rules check** (custom code in `schema_registry.py`)
Validates semantics that JSON Schema cannot express:
- `order_id` regex pattern match
- `customer_id` not blank (JSON Schema can enforce non-empty string type but not empty-after-strip)
- `amount` strictly greater than 0 (and ≤ max if defined)
- `order_date` is a valid ISO 8601 datetime and not in the future
- `currency` in the schema-version-specific allowed list

**Routing:**
- PASS → `valid.orders`
- FAIL → `invalid.orders.dlq` with enriched DLQ envelope

**Backpressure:**
A `threading.Semaphore(MAX_CONCURRENT_VALIDATIONS)` caps concurrent in-flight validations. If the semaphore is fully occupied, the consumer pauses its Kafka partition assignment, preventing unbounded queue growth. The partition is resumed once a validation slot frees up. This provides deterministic backpressure without an external queue.

### 2.3 DLQ Processor
Subscribes to two topics in a single consumer:
- `invalid.orders.dlq` — buffers incoming failed messages in an in-memory dict keyed by `dlq_id`
- `schema.updates` — triggers reprocessing on any new message

On reprocess trigger:
1. Snapshot the current pending buffer
2. Revalidate each message against the **latest** registered schema using `validate_against_latest()`
3. Messages that now pass → published to `valid.orders` with `_reprocessed=True` and `_dlq_id` flags
4. Messages that still fail → `retry_count` incremented
5. After `MAX_RETRIES` (3) → message is dropped with a WARNING log

The use of `_reprocessed=True` flag lets the API observer distinguish reprocessed records from organically valid ones, maintaining accurate counters.

### 2.4 API + Dashboard
FastAPI application with two background threads:
- **Valid observer** — consumes `valid.orders` from the beginning, counts valid and reprocessed messages
- **DLQ observer** — consumes `invalid.orders.dlq` from the beginning, tracks pending DLQ state

This observer pattern means the API builds its own view of the system state from Kafka — no shared memory, no IPC between containers. Each service is fully independent.

The dashboard is a single HTML file served as a static asset. It polls 5 API endpoints every 3 seconds and updates:
- Live counters (total, valid, invalid, reprocessed)
- Donut chart (valid vs invalid ratio)
- Bar chart (per schema version breakdown)
- Line chart (throughput over last 60 seconds)
- Violations table (last 10 rule failures)
- DLQ panel (pending count, last reprocess time, manual trigger button)
- Schema versions panel (all registered versions)

---

## 3. Schema Evolution Approach

Schemas are defined as JSON files in `schemas/vN.json`. Each file contains:
1. A standard JSON Schema (`$schema`, `properties`, `required`) for structural validation
2. A `business_rules` block for semantic validation (not a standard JSON Schema keyword)

The `SchemaRegistry` class loads all files from disk at startup. New schema versions can be registered at runtime via `POST /api/schemas`. This allows zero-downtime evolution — the validator picks up new versions through the shared registry, and the DLQ processor uses `validate_against_latest()` to reprocess against the newest version.

**Evolution chain:**
- v1 → v2: `currency` field becomes mandatory; allowed values restricted to `[USD, EUR, GBP]`
- v2 → v3: `currency` expands to `[USD, EUR, GBP, JPY, INR]`; optional `discount_pct` added

The v2→v3 transition is the core demo: records that failed v2 due to `currency=JPY` are rescued by the DLQ processor once v3 is registered, without any manual intervention on the records themselves.

---

## 4. DLQ Design

Each DLQ message carries the following envelope:

```json
{
  "dlq_id": "uuid4",
  "original_message": { ... },
  "error_details": ["currency 'JPY' not allowed; must be one of ['USD', 'EUR', 'GBP']"],
  "schema_version": "v2",
  "failed_at": "2026-04-16T09:24:13Z",
  "retry_count": 0,
  "resolved": false
}
```

**Why a separate DLQ topic rather than a retry topic?**
A retry topic re-ingests messages into `raw.orders`, which revalidates them against the same schema version — pointless for schema-related failures. The DLQ topic keeps failed messages isolated until a meaningful change (schema update) makes reprocessing viable.

**Reprocessing trigger flow:**
```
POST /api/schemas {"version": "v3"}
       │
       ├── API registers v3 in its local registry
       └── API publishes {"version": "v3"} to schema.updates
                  │
                  └── DLQ Processor receives signal
                      └── Iterates pending buffer, revalidates against v3
                          ├── PASS → valid.orders (_reprocessed=True)
                          └── FAIL → retry_count++ (drop after 3)
```

---

## 5. Kafka Topic Design

| Topic | Partitions | Key | Rationale |
|---|---|---|---|
| `raw.orders` | 3 | `order_id` | Ordering per order; 3 partitions allows parallel validator instances |
| `valid.orders` | 3 | `order_id` | Consistent with raw — downstream consumers get ordered view per order |
| `invalid.orders.dlq` | 3 | `dlq_id` | Spread DLQ load; keyed by dlq_id not order_id to avoid rebalance issues |
| `schema.updates` | 1 | `version` | Single partition ensures all DLQ processor instances see schema updates in order |

`schema.updates` with 1 partition is intentional: if multiple DLQ processor instances were running, a single partition guarantees every instance sees the same sequence of schema events, preventing split-brain reprocessing.

---

## 6. Backpressure Control

The validator uses a two-level backpressure mechanism:

**Level 1 — Semaphore**
`threading.Semaphore(MAX_CONCURRENT_VALIDATIONS)` limits how many messages are being validated simultaneously. Each validation thread acquires the semaphore and releases it on completion.

**Level 2 — Partition Pause**
If the semaphore has zero slots available when a new message arrives, the validator calls `consumer.pause(assignment())`, which stops Kafka from delivering more messages until the consumer calls `consumer.resume()`. This prevents unbounded memory growth — Kafka holds the undelivered messages on the broker side.

Together these provide deterministic flow control: throughput adjusts automatically to processing capacity without dropping messages or requiring an external queue.

---

## 7. Operational Considerations

**Single broker setup**: This is a demo deployment. Production would use at minimum 3 brokers with replication factor 3 for fault tolerance.

**In-memory metrics**: Metrics reset on container restart. Production would use Redis or a time-series DB (InfluxDB, Prometheus) for persistence and alerting.

**Schema registry**: Schemas are stored as files. Production would use Confluent Schema Registry for centralized governance, compatibility enforcement (BACKWARD/FORWARD/FULL), and schema versioning history.

**Consumer group isolation**: Each service uses a dedicated consumer group ID. This means every service gets its own independent offset tracking — the API observers don't interfere with the validator or DLQ processor.

**Retry limit**: DLQ messages are dropped after 3 failed reprocessing attempts. Production would route these to a separate "dead-dead letter queue" or alert on them rather than silently dropping.

**kafka-init dependency**: The `service_completed_successfully` condition in docker-compose ensures no service starts consuming before topics exist — avoiding the race condition where a consumer starts before its topic is created.
