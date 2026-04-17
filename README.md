# DQ Pipeline - Kafka + Python

A production-style, event-driven Data Quality pipeline that validates streaming order data against versioned JSON schemas, routes valid/invalid records, automatically reprocesses failed records when schemas evolve, and exposes a live monitoring dashboard.

---

https://github.com/user-attachments/assets/1b258c90-9e25-4611-8336-3c3874691ab3

---

<img width="1079" height="976" alt="Image" src="https://github.com/user-attachments/assets/3ec8458c-9c2c-4333-b6f7-7e8f131b2d85" />

---

<img width="1854" height="1008" alt="Image" src="https://github.com/user-attachments/assets/2d04b0cb-e8df-4cf6-8ad8-592b78f8f954" />

---

<img width="1332" height="713" alt="Image" src="https://github.com/user-attachments/assets/cfbc750b-7f1d-41a2-9950-b384801fff4d" />

---

<img width="1912" height="708" alt="Image" src="https://github.com/user-attachments/assets/d8f006e8-0ca7-41c9-ac77-0ed0ed327347" />

---

<img width="1917" height="807" alt="Image" src="https://github.com/user-attachments/assets/7f15f070-acff-4373-abb7-81fe81515069" />

---

## Quick Start

```bash
git clone https://github.com/Gagancreates/DQ_Pipeline_Kafka
cd DQ_Pipeline_Kafka

docker-compose up --build
```

That's it. All services start automatically.

| URL | What you'll see |
|---|---|
| http://localhost:8000 | Live dashboard - counters, charts, violations, DLQ |
| http://localhost:8000/docs | FastAPI auto-generated API docs |
| http://localhost:8080 | Kafka UI - browse topics and raw messages |

---

## Services

| Container | Role |
|---|---|
| `zookeeper` | ZooKeeper coordination for Kafka |
| `kafka` | Single Kafka broker (port 9092 external, 29092 internal) |
| `kafka-init` | One-shot container that creates the 4 topics on startup |
| `kafka-ui` | Browser UI for inspecting Kafka topics (port 8080) |
| `producer` | Generates continuous mixed order events |
| `validator` | Validates messages, routes to valid topic or DLQ |
| `dlq_processor` | Reprocesses DLQ messages when schema updates arrive |
| `api` | FastAPI REST API + live dashboard (port 8000) |

---

## Kafka Topics

| Topic | Partitions | Purpose |
|---|---|---|
| `raw.orders` | 3 | Unvalidated incoming orders |
| `valid.orders` | 3 | Orders that passed validation |
| `invalid.orders.dlq` | 3 | Failed orders with error metadata |
| `schema.updates` | 1 | Internal signal for DLQ reprocessing |

---

## Schema Versions

### v1 - Baseline
Required: `order_id`, `customer_id`, `amount`, `order_date`
- `order_id` must match `ORD-YYYY-NNN`
- `amount` must be > 0
- `order_date` must not be in the future

### v2 - Adds Currency
Required: all v1 fields + `currency`
- `currency` must be one of: `USD`, `EUR`, `GBP`
- `amount` capped at 1,000,000

### v3 - Expanded Currency + Optional Discount
Required: all v2 fields
Optional: `discount_pct`
- `currency` now also allows: `JPY`, `INR`
- `discount_pct` if present must be 0–100

---

## Producer Message Mix

| Category | Weight | Examples |
|---|---|---|
| Valid | 40% | Spread across v1, v2, v3 |
| Invalid | 40% | Bad currency, negative amount, future date, empty customer_id |
| Edge | 20% | Unknown schema version (`v99`), malformed JSON, empty payload |

---

## REST API

### Health
```bash
curl http://localhost:8000/api/health
```

### Live Metrics
```bash
curl http://localhost:8000/api/metrics
```
```json
{
  "total": 1940,
  "valid": 866,
  "invalid": 1162,
  "reprocessed": 88,
  "throughput_per_sec": 1.31,
  "by_version": {
    "v1": { "valid": 253, "invalid": 214 },
    "v2": { "valid": 258, "invalid": 415 },
    "v3": { "valid": 355, "invalid": 133 }
  }
}
```

### Per-Version Metrics
```bash
curl http://localhost:8000/api/metrics/schema/v2
```

### Recent Violations
```bash
curl http://localhost:8000/api/violations
```

### DLQ State
```bash
curl http://localhost:8000/api/dlq
```

### List Schema Versions
```bash
curl http://localhost:8000/api/schemas
```

### Register New Schema Version (triggers DLQ reprocessing)
```bash
curl -X POST http://localhost:8000/api/schemas \
  -H "Content-Type: application/json" \
  -d '{"version": "v3"}'
```

### Manually Trigger DLQ Reprocessing
```bash
curl -X POST http://localhost:8000/api/reprocess
```

---

## Demo Walkthrough - Schema Evolution

This is the core demo to show the evaluator.

**Step 1** - System is running. Producer is sending v2 messages with `currency=JPY`.
These fail v2 validation (JPY not in allowed list) and land in the DLQ.

Check the DLQ:
```bash
curl http://localhost:8000/api/dlq
```
You'll see records with `error_details: ["currency 'JPY' not allowed; must be one of ['USD', 'EUR', 'GBP']"]`

**Step 2** - v3 schema is introduced. JPY is now valid.
Signal the pipeline:
```bash
curl -X POST http://localhost:8000/api/schemas \
  -H "Content-Type: application/json" \
  -d '{"version": "v3"}'
```

**Step 3** - DLQ processor wakes up, revalidates all pending DLQ messages against v3.
JPY records that were previously invalid now pass. They are published to `valid.orders`.

Check metrics - `reprocessed` counter increases:
```bash
curl http://localhost:8000/api/metrics
```

Watch it live on the dashboard at http://localhost:8000.

---

## Configuration

All configuration lives in `.env`:

```
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
TOPIC_RAW_ORDERS=raw.orders
TOPIC_VALID_ORDERS=valid.orders
TOPIC_DLQ=invalid.orders.dlq
TOPIC_SCHEMA_UPDATES=schema.updates
PRODUCE_INTERVAL_MS=800
MAX_CONCURRENT_VALIDATIONS=10
```

---

## Project Structure

```
DQ_Pipeline_Kafka/
├── docker-compose.yml
├── .env
├── schemas/
│   ├── v1.json
│   ├── v2.json
│   └── v3.json
├── shared/
│   ├── config.py
│   ├── kafka_utils.py
│   ├── metrics_store.py
│   ├── models.py
│   └── schema_registry.py
└── services/
    ├── producer/
    ├── validator/
    ├── dlq_processor/
    └── api/
        └── static/
            └── dashboard.html
```
