"""
setup_infra.py
--------------
One-time setup script. Run this AFTER `docker-compose up -d` to:
  1. Create MinIO buckets  (bronze, silver, gold)
  2. Create Kafka topics   (ecommerce.orders / payments / clicks)
  3. Create PostgreSQL schemas and raw tables
"""

import logging
import time

import boto3
import psycopg2
from botocore.exceptions import ClientError
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (mirrors .env.example defaults)
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKETS = ["bronze", "silver", "gold"]

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPICS = [
    {"name": "ecommerce.orders",   "partitions": 3, "replication": 1},
    {"name": "ecommerce.payments", "partitions": 3, "replication": 1},
    {"name": "ecommerce.clicks",   "partitions": 3, "replication": 1},
]

PG_HOST     = "localhost"
PG_PORT     = 5432
PG_DB       = "olist_warehouse"
PG_USER     = "postgres"
PG_PASSWORD = "postgrespassword"


# ---------------------------------------------------------------------------
# 1. MinIO
# ---------------------------------------------------------------------------
def setup_minio() -> None:
    logger.info("── MinIO ─────────────────────────────────────────")
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    for bucket in BUCKETS:
        try:
            s3.create_bucket(Bucket=bucket)
            logger.info(f"  ✅  Created bucket: s3://{bucket}")
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                logger.info(f"  ℹ️   Bucket already exists: s3://{bucket}")
            else:
                raise


# ---------------------------------------------------------------------------
# 2. Kafka Topics
# ---------------------------------------------------------------------------
def setup_kafka_topics() -> None:
    logger.info("── Kafka Topics ──────────────────────────────────")
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    new_topics = [
        NewTopic(
            t["name"],
            num_partitions=t["partitions"],
            replication_factor=t["replication"],
        )
        for t in TOPICS
    ]
    results = admin.create_topics(new_topics)
    for topic, future in results.items():
        try:
            future.result()
            logger.info(f"  ✅  Created topic: {topic}")
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                logger.info(f"  ℹ️   Topic already exists: {topic}")
            else:
                logger.error(f"  ❌  Failed to create topic {topic}: {exc}")


# ---------------------------------------------------------------------------
# 3. PostgreSQL
# ---------------------------------------------------------------------------
def setup_postgres() -> None:
    logger.info("── PostgreSQL ────────────────────────────────────")
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        # Schemas
        for schema in ["raw", "staging", "marts"]:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            logger.info(f"  ✅  Schema: {schema}")

        # raw.orders
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.orders (
                order_id          TEXT PRIMARY KEY,
                customer_id       TEXT        NOT NULL,
                product_id        TEXT        NOT NULL,
                seller_id         TEXT        NOT NULL,
                product_category  TEXT,
                quantity          INTEGER,
                unit_price        NUMERIC(12, 2),
                total_amount      NUMERIC(12, 2),
                order_status      TEXT,
                payment_method    TEXT,
                created_at        TIMESTAMPTZ,
                loaded_at         TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        logger.info("  ✅  Table: raw.orders")

        # raw.payments
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.payments (
                payment_id    TEXT PRIMARY KEY,
                order_id      TEXT        NOT NULL,
                customer_id   TEXT        NOT NULL,
                payment_type  TEXT,
                installments  INTEGER     DEFAULT 1,
                amount        NUMERIC(12, 2),
                status        TEXT,
                created_at    TIMESTAMPTZ,
                loaded_at     TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        logger.info("  ✅  Table: raw.payments")

        # raw.clicks
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.clicks (
                click_id          TEXT PRIMARY KEY,
                session_id        TEXT        NOT NULL,
                user_id           TEXT        NOT NULL,
                product_id        TEXT        NOT NULL,
                category          TEXT,
                action            TEXT,
                device_type       TEXT,
                page_url          TEXT,
                event_timestamp   TIMESTAMPTZ,
                loaded_at         TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        logger.info("  ✅  Table: raw.clicks")

    conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("🚀  Olist Streaming Pipeline — Infrastructure Setup")
    logger.info("⏳  Waiting 5 s for services to be ready...")
    time.sleep(5)

    setup_minio()
    setup_kafka_topics()
    setup_postgres()

    logger.info("")
    logger.info("🎉  Setup complete! Next steps:")
    logger.info("    Terminal 1 → python scripts/producer.py")
    logger.info("    Terminal 2 → python scripts/consumer.py")
