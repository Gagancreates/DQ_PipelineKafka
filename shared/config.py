import os

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Topics
TOPIC_RAW_ORDERS       = os.getenv("TOPIC_RAW_ORDERS",      "raw.orders")
TOPIC_VALID_ORDERS     = os.getenv("TOPIC_VALID_ORDERS",    "valid.orders")
TOPIC_DLQ              = os.getenv("TOPIC_DLQ",             "invalid.orders.dlq")
TOPIC_SCHEMA_UPDATES   = os.getenv("TOPIC_SCHEMA_UPDATES",  "schema.updates")

# Consumer groups
CONSUMER_GROUP_VALIDATOR      = os.getenv("CONSUMER_GROUP_VALIDATOR",      "dq-validator")
CONSUMER_GROUP_DLQ_PROCESSOR  = os.getenv("CONSUMER_GROUP_DLQ_PROCESSOR",  "dq-dlq-processor")

# Producer
PRODUCE_INTERVAL_MS = int(os.getenv("PRODUCE_INTERVAL_MS", "800"))

# Validator
MAX_CONCURRENT_VALIDATIONS = int(os.getenv("MAX_CONCURRENT_VALIDATIONS", "10"))

# API
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

# Schemas directory — works both locally and inside Docker
SCHEMAS_DIR = os.getenv(
    "SCHEMAS_DIR",
    os.path.join(os.path.dirname(__file__), "..", "schemas")
)
