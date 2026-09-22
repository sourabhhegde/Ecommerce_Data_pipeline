"""
consumer.py
-----------
Reads e-commerce events from Redpanda (Kafka) and dual-writes to:
  • MinIO (Bronze layer)  — Snappy-compressed Parquet, Hive-partitioned by date/hour
  • PostgreSQL (raw schema) — Bulk-upserted for immediate dbt access

Flush strategy:
  - Per topic: flush when buffer reaches BATCH_SIZE messages
  - Global:    flush all buffers every FLUSH_INTERVAL seconds

Run:
  python scripts/consumer.py
"""

import io
import json
import logging
import signal
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List

import boto3
import pandas as pd
import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID          = "olist-consumer-group"
TOPICS = [
    "ecommerce.orders",
    "ecommerce.payments",
    "ecommerce.clicks",
]

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET    = "bronze"

PG_HOST     = os.getenv("PG_HOST", "localhost")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_DB       = os.getenv("PG_DB", "olist_warehouse")
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgrespassword")

BATCH_SIZE     = int(os.getenv("BATCH_SIZE", "100"))   # Records per topic before flushing
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", "30"))    # Seconds between forced flushes

# Friendly short name for each topic
TOPIC_TABLE: Dict[str, str] = {
    "ecommerce.orders":   "orders",
    "ecommerce.payments": "payments",
    "ecommerce.clicks":   "clicks",
}

# ---------------------------------------------------------------------------
# PyArrow schemas (enforces column types in Parquet)
# ---------------------------------------------------------------------------
ORDER_SCHEMA = pa.schema([
    pa.field("order_id",          pa.string()),
    pa.field("customer_id",       pa.string()),
    pa.field("product_id",        pa.string()),
    pa.field("seller_id",         pa.string()),
    pa.field("product_category",  pa.string()),
    pa.field("quantity",          pa.int32()),
    pa.field("unit_price",        pa.float64()),
    pa.field("total_amount",      pa.float64()),
    pa.field("order_status",      pa.string()),
    pa.field("payment_method",    pa.string()),
    pa.field("event_type",        pa.string()),
    pa.field("created_at",        pa.string()),
    pa.field("loaded_at",         pa.string()),
])

PAYMENT_SCHEMA = pa.schema([
    pa.field("payment_id",   pa.string()),
    pa.field("order_id",     pa.string()),
    pa.field("customer_id",  pa.string()),
    pa.field("payment_type", pa.string()),
    pa.field("installments", pa.int32()),
    pa.field("amount",       pa.float64()),
    pa.field("status",       pa.string()),
    pa.field("event_type",   pa.string()),
    pa.field("created_at",   pa.string()),
    pa.field("loaded_at",    pa.string()),
])

CLICK_SCHEMA = pa.schema([
    pa.field("click_id",        pa.string()),
    pa.field("session_id",      pa.string()),
    pa.field("user_id",         pa.string()),
    pa.field("product_id",      pa.string()),
    pa.field("category",        pa.string()),
    pa.field("action",          pa.string()),
    pa.field("device_type",     pa.string()),
    pa.field("page_url",        pa.string()),
    pa.field("event_type",      pa.string()),
    pa.field("event_timestamp", pa.string()),
    pa.field("loaded_at",       pa.string()),
])

TOPIC_SCHEMA: Dict[str, pa.Schema] = {
    "ecommerce.orders":   ORDER_SCHEMA,
    "ecommerce.payments": PAYMENT_SCHEMA,
    "ecommerce.clicks":   CLICK_SCHEMA,
}


# ---------------------------------------------------------------------------
# MinIO helpers
# ---------------------------------------------------------------------------
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def write_parquet_to_minio(
    s3,
    records: List[dict],
    topic: str,
    schema: pa.Schema,
    now: datetime,
) -> None:
    """Serialise records as Snappy-compressed Parquet and upload to MinIO."""
    table_name = TOPIC_TABLE[topic]
    s3_key = (
        f"{table_name}/"
        f"year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"batch_{int(now.timestamp() * 1000)}.parquet"
    )

    df = pd.DataFrame(records)
    df["loaded_at"] = now.isoformat()

    # Ensure every schema column is present (fill missing with None)
    for field in schema:
        if field.name not in df.columns:
            df[field.name] = None

    arrow_table = pa.Table.from_pandas(
        df[[f.name for f in schema]],
        schema=schema,
        safe=False,
    )

    buf = io.BytesIO()
    pq.write_table(arrow_table, buf, compression="snappy")
    buf.seek(0)

    s3.put_object(Bucket=BRONZE_BUCKET, Key=s3_key, Body=buf.getvalue())
    logger.info(
        f"📦  MinIO  [{table_name}] {len(records):>4} records "
        f"→ s3://{BRONZE_BUCKET}/{s3_key}"
    )


# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------
def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )


def bulk_upsert_to_pg(
    conn,
    records: List[dict],
    topic: str,
    now: datetime,
) -> None:
    """Bulk upsert records into the appropriate raw.* table."""
    table_name = TOPIC_TABLE[topic]
    loaded_at  = now.isoformat()

    if table_name == "orders":
        sql = """
            INSERT INTO raw.orders (
                order_id, customer_id, product_id, seller_id,
                product_category, quantity, unit_price, total_amount,
                order_status, payment_method, created_at, loaded_at
            ) VALUES %s
            ON CONFLICT (order_id) DO NOTHING
        """
        data = [
            (
                r["order_id"], r["customer_id"], r["product_id"], r["seller_id"],
                r["product_category"], r["quantity"], r["unit_price"], r["total_amount"],
                r["order_status"], r["payment_method"], r["created_at"], loaded_at,
            )
            for r in records
        ]

    elif table_name == "payments":
        sql = """
            INSERT INTO raw.payments (
                payment_id, order_id, customer_id, payment_type,
                installments, amount, status, created_at, loaded_at
            ) VALUES %s
            ON CONFLICT (payment_id) DO NOTHING
        """
        data = [
            (
                r["payment_id"], r["order_id"], r["customer_id"], r["payment_type"],
                r["installments"], r["amount"], r["status"], r["created_at"], loaded_at,
            )
            for r in records
        ]

    elif table_name == "clicks":
        sql = """
            INSERT INTO raw.clicks (
                click_id, session_id, user_id, product_id,
                category, action, device_type, page_url, event_timestamp, loaded_at
            ) VALUES %s
            ON CONFLICT (click_id) DO NOTHING
        """
        data = [
            (
                r["click_id"], r["session_id"], r["user_id"], r["product_id"],
                r["category"], r["action"], r["device_type"], r["page_url"],
                r["event_timestamp"], loaded_at,
            )
            for r in records
        ]
    else:
        return

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, data, page_size=500)
    conn.commit()
    logger.info(f"🐘  Postgres [{table_name}] {len(records):>4} records → raw.{table_name}")


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------
class StreamingConsumer:

    def __init__(self) -> None:
        self.running   = True
        self.buffers: Dict[str, List[dict]] = defaultdict(list)
        self.last_flush = time.time()
        self.total      = 0

        self.s3      = get_s3_client()
        self.pg_conn = get_pg_connection()

        signal.signal(signal.SIGINT,  self._on_shutdown)
        signal.signal(signal.SIGTERM, self._on_shutdown)

    def _on_shutdown(self, signum, frame) -> None:
        logger.info("⛔  Shutdown signal — flushing remaining buffers...")
        self.running = False

    # ── flush helpers ─────────────────────────────────────────────────────
    def _flush_topic(self, topic: str) -> None:
        records = self.buffers[topic]
        if not records:
            return

        now    = datetime.now(timezone.utc)
        schema = TOPIC_SCHEMA[topic]

        try:
            write_parquet_to_minio(self.s3, records, topic, schema, now)
        except Exception as exc:
            logger.error(f"❌  MinIO flush failed [{topic}]: {exc}")

        try:
            bulk_upsert_to_pg(self.pg_conn, records, topic, now)
        except Exception as exc:
            logger.error(f"❌  Postgres flush failed [{topic}]: {exc}")
            try:
                self.pg_conn = get_pg_connection()
            except Exception:
                pass

        self.total += len(records)
        self.buffers[topic] = []

    def _flush_all(self) -> None:
        for topic in TOPICS:
            self._flush_topic(topic)
        self.last_flush = time.time()

    # ── main loop ─────────────────────────────────────────────────────────
    def run(self) -> None:
        conf = {
            "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
            "group.id":           KAFKA_GROUP_ID,
            "auto.offset.reset":  "earliest",
            "enable.auto.commit": False,
        }
        consumer = Consumer(conf)
        consumer.subscribe(TOPICS)
        logger.info(f"🎧  Consumer started — subscribed to {TOPICS}")
        logger.info(f"    Batch size: {BATCH_SIZE} | Flush interval: {FLUSH_INTERVAL}s")

        try:
            while self.running:
                msg = consumer.poll(timeout=1.0)

                # No message — check if it's time for a periodic flush
                if msg is None:
                    if time.time() - self.last_flush >= FLUSH_INTERVAL:
                        self._flush_all()
                        consumer.commit(asynchronous=False)
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                topic  = msg.topic()
                record = json.loads(msg.value().decode("utf-8"))
                self.buffers[topic].append(record)

                # Per-topic flush when buffer is full
                if len(self.buffers[topic]) >= BATCH_SIZE:
                    self._flush_topic(topic)
                    consumer.commit(asynchronous=False)

                # Global interval flush
                if time.time() - self.last_flush >= FLUSH_INTERVAL:
                    self._flush_all()
                    consumer.commit(asynchronous=False)

        except KafkaException as exc:
            logger.error(f"Kafka error: {exc}")
        finally:
            self._flush_all()
            consumer.close()
            self.pg_conn.close()
            logger.info(f"✅  Consumer stopped. Total records processed: {self.total:,}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    StreamingConsumer().run()
