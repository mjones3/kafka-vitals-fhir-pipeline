#!/usr/bin/env python3
import os
import json
import time
import random
import logging
from datetime import datetime, timedelta
from confluent_kafka import Producer

# ─── Configuration from ENV ────────────────────────────────────────────────────

BROKER       = os.getenv("KAFKA_BROKER", "kafka:9093")
TOPIC        = os.getenv("KAFKA_TOPIC", "vitals-json")
PATIENT_ID   = os.getenv("PATIENT_ID", "p010124")
START_TIME   = os.getenv("START_TIME")  # e.g. "2025-05-10T00:00:00.000Z"
INTERVAL_MS  = int(os.getenv("INTERVAL_MS", "10"))
LOG_INTERVAL = int(os.getenv("LOG_INTERVAL", "1000"))  # how often to log progress

# ─── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("random-vitals-producer")

# ─── Kafka Producer Setup ───────────────────────────────────────────────────────

conf = {
    "bootstrap.servers": BROKER,
    "linger.ms": 100,
    "batch.num.messages": 500,
    "compression.codec": "lz4",
}
producer = Producer(conf)

# ─── Timestamp init ─────────────────────────────────────────────────────────────

if START_TIME:
    current_time = datetime.fromisoformat(START_TIME.replace("Z", "+00:00"))
else:
    current_time = datetime.utcnow()

delta = timedelta(milliseconds=INTERVAL_MS)
count = 0

# ─── Main loop ─────────────────────────────────────────────────────────────────

def make_record(ts: datetime) -> dict:
    """Build a random vitals JSON record at timestamp ts."""
    return {
        "time": ts.isoformat() + "Z",
        "patient_id": PATIENT_ID,
        "ECG_I": round(random.uniform(0.5, 1.5), 6),
        "ECG_II": round(random.uniform(0.5, 1.5), 6),
        "ECG_III": round(random.uniform(0.5, 1.5), 6),
        "ABP": round(random.uniform(40, 120), 2),
        "RESP": round(random.uniform(8, 25), 2),
        "SpO2": round(random.uniform(90, 100), 2),
        "heart_rate": random.randint(50, 120),
        "systolic_bp": round(random.uniform(90, 140), 2),
        "diastolic_bp": round(random.uniform(60, 90), 2),
        "respiratory_rate": random.randint(8, 25),
        "mean_arterial_pressure": round(random.uniform(65, 105), 2),
    }

logger.info(f"Starting random producer → topic '{TOPIC}' @ {BROKER}")
while True:
    record = make_record(current_time)
    payload = json.dumps(record).encode("utf-8")

    producer.produce(TOPIC, value=payload)
    producer.poll(0)

    count += 1
    if count % LOG_INTERVAL == 0:
        logger.info(f"Produced {count} records; latest time={record['time']}")

    current_time += delta
    time.sleep(INTERVAL_MS / 1000.0)
