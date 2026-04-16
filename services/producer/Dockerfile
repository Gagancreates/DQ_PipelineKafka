FROM python:3.11-slim

WORKDIR /app

# Install system deps needed by confluent-kafka (librdkafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY services/producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared modules and service code
COPY shared/ ./shared/
COPY schemas/ ./schemas/
COPY services/producer/main.py .

CMD ["python", "main.py"]
