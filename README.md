# 🛒 Olist E-Commerce Streaming Data Pipeline

A real-time streaming pipeline that **generates synthetic e-commerce events** (orders, payments, clicks) and processes them through a Medallion Architecture (Bronze → Silver → Gold) — fully containerized with Docker.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│              Python Producer (Faker) [Docker]                    │
│        1,000 customers · 200 products · 50 sellers               │
└────────────────────────┬────────────────────────────────────────┘
                         │  JSON events @ configurable rate
                         ▼
              ┌─────────────────────┐
              │   Redpanda (Kafka)  │  3 topics, 3 partitions each
              └──────────┬──────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │  Python Consumer    │  [Docker]
              └──────┬──────┬───────┘
                     │      │
           ┌─────────┘      └─────────┐
           ▼                          ▼
  ┌────────────────┐        ┌──────────────────┐
  │     MinIO      │        │   PostgreSQL      │
  │  Bronze Layer  │        │   raw.* schema    │
  │ Parquet/Snappy │        │  (for dbt input)  │
  │ Hive-partition │        └────────┬─────────┘
  └────────────────┘                 │
                                     ▼
                              ┌─────────────────┐
                              │  dbt Runner     │  [Docker — runs every 60s]
                              │  staging/       │  ← views (cleaned)
                              │  marts/         │  ← tables (aggregated)
                              └────────┬────────┘
                                       │
                                       ▼
                              ┌─────────────────┐
                              │    Metabase      │  [Docker — Live Dashboard]
                              │  localhost:3000  │
                              └─────────────────┘
```

## 📦 Stack

| Component | Technology | Port(s) |
|---|---|---|
| Message Broker | **Redpanda** (Kafka-compatible) | 9092 |
| Broker Web UI | Redpanda Console | **8080** |
| Data Lake | **MinIO** (S3-compatible) | 9000 / **9001** |
| Data Warehouse | **PostgreSQL 15** | 5432 |
| Transformations | **dbt-core** + dbt-postgres | — |
| Dashboard | **Metabase** | **3000** |
| Producer | **Python + Faker** | Docker |
| Consumer | **Python + confluent-kafka** | Docker |

---

## 🚀 Getting Started (Clone & Run)

### Prerequisites
Make sure you have these installed on your machine:
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running)
- [Python 3.11+](https://www.python.org/downloads/)
- [Git](https://git-scm.com/)

### Step 1: Clone the Repository
```bash
git clone https://github.com/YOUR_USERNAME/olist-data-pipeline.git
cd olist-data-pipeline
```

### Step 2: Configure Environment Variables
```bash
# On Mac / Linux
cp .env.example .env

# On Windows (PowerShell)
copy .env.example .env
```
> The default values in `.env` work out of the box. No changes needed for local development.

### Step 3: Start the Entire Pipeline (One Command!)
```bash
docker-compose up -d --build
```
This single command will:
- ✅ Download all Docker images
- ✅ Build the Python Producer and Consumer containers
- ✅ Start Redpanda, MinIO, PostgreSQL, and Metabase
- ✅ Automatically start streaming events (Producer)
- ✅ Automatically save data to MinIO and PostgreSQL (Consumer)
- ✅ Automatically run `dbt` every 60 seconds (dbt Runner)

> ⏳ **Wait ~60 seconds** for all services to fully initialize before proceeding.

### Step 4: Bootstrap Infrastructure (One-Time Setup)
This script creates the MinIO buckets, Kafka topics, and PostgreSQL schemas:
```bash
pip install -r requirements.txt
python scripts/setup_infra.py
```

### Step 5: Open Your Dashboards 🎉
| UI | URL | Login |
|---|---|---|
| **Metabase** (Live Dashboard) | http://localhost:3000 | Set up on first visit |
| **Redpanda Console** (Kafka UI) | http://localhost:8080 | None |
| **MinIO Console** (Data Lake) | http://localhost:9001 | `minioadmin` / `minioadmin` |

### Step 6: Connect Metabase to PostgreSQL
On first launch of Metabase (http://localhost:3000):
1. Create an admin account.
2. When asked to "Add your data", select **PostgreSQL**.
3. Fill in:
   - **Host:** `postgres`
   - **Port:** `5432`
   - **Database:** `olist_warehouse`
   - **Username:** `postgres`
   - **Password:** `postgrespassword`

---

## 🛑 Stopping the Pipeline
```bash
docker-compose down
```
> Your data is **safely persisted** in Docker named volumes (`postgres_data`, `minio_data`). Running `docker-compose up -d` again will restore everything exactly where you left off.

---

## 🌐 Web UIs

| UI | URL | Credentials |
|---|---|---|
| Metabase Dashboard | http://localhost:3000 | *(created on first visit)* |
| Redpanda Console | http://localhost:8080 | *(none)* |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |

---

## 🗂️ Data Model

### Kafka Topics

| Topic | Event Type | ~Share |
|---|---|---|
| `ecommerce.orders` | New order placed | 25% |
| `ecommerce.payments` | Payment attempt (linked to orders) | 15% |
| `ecommerce.clicks` | Product view / cart / wishlist | 60% |

### MinIO Bronze Layer (Hive-partitioned Parquet)
```
bronze/
  orders/year=2026/month=09/day=18/hour=10/batch_<ts>.parquet
  payments/year=2026/month=09/day=18/hour=10/batch_<ts>.parquet
  clicks/year=2026/month=09/day=18/hour=10/batch_<ts>.parquet
```

### dbt Lineage
```
raw.orders   ──► stg_orders   ──► fct_orders         (order + payment join)
raw.payments ──► stg_payments ──┘  fct_revenue_hourly (hourly roll-up by category)
raw.clicks   ──► stg_clicks   ──► dim_products        (sales + engagement metrics)
```

---

## ⚙️ Configuration

Copy `.env.example` to `.env` and adjust as needed:

| Variable | Default | Description |
|---|---|---|
| `EVENTS_PER_SECOND` | `10` | Producer throughput |
| `BATCH_SIZE` | `100` | Consumer records-per-flush |
| `FLUSH_INTERVAL` | `30` | Consumer max seconds between flushes |

---

## 📁 Project Structure

```
olist_data_pipeline/
├── docker-compose.yml          # All 8 services: Redpanda, MinIO, PostgreSQL,
│                               # Metabase, Producer, Consumer, dbt Runner, Console
├── Dockerfile.producer         # Docker image for the event generator
├── Dockerfile.consumer         # Docker image for the Kafka consumer
├── requirements.txt
├── .env.example                # Config template (copy to .env)
├── scripts/
│   ├── setup_infra.py          # One-time: create buckets, topics, DB schema
│   ├── producer.py             # Fake event generator (Faker pt_BR)
│   └── consumer.py             # Kafka → MinIO + PostgreSQL dual-sink
└── dbt_project/
    ├── dbt_project.yml
    ├── profiles.yml
    └── models/
        ├── staging/
        │   ├── _sources.yml
        │   ├── schema.yml
        │   ├── stg_orders.sql
        │   ├── stg_payments.sql
        │   └── stg_clicks.sql
        └── marts/
            ├── schema.yml
            ├── fct_orders.sql
            ├── fct_revenue_hourly.sql
            └── dim_products.sql
```
