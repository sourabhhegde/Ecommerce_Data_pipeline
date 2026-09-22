# 🛒 Olist E-Commerce Streaming Data Pipeline

A real-time streaming pipeline that **generates synthetic e-commerce events** (orders, payments, clicks) and processes them through a Medallion Architecture (Bronze → Silver → Gold).

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   Python Producer (Faker)                        │
│     1,000 customers · 200 products · 50 sellers                 │
└────────────────────────┬────────────────────────────────────────┘
                         │  JSON events @ configurable rate
                         ▼
              ┌─────────────────────┐
              │   Redpanda (Kafka)  │  3 topics, 3 partitions each
              └──────────┬──────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │   Python Consumer   │
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
                              ┌─────────────┐
                              │     dbt     │
                              │  staging/   │  ← views (cleaned)
                              │  marts/     │  ← tables (aggregated)
                              └─────────────┘
```

## 📦 Stack

| Component | Technology | Port(s) |
|---|---|---|
| Message Broker | **Redpanda** (Kafka-compatible) | 9092 |
| Broker Web UI | Redpanda Console | **8080** |
| Data Lake | **MinIO** (S3-compatible) | 9000 / **9001** |
| Data Warehouse | **PostgreSQL 15** | 5432 |
| Transformations | **dbt-core** + dbt-postgres | — |

## 🚀 Quick Start

### 1. Start Infrastructure
```bash
docker-compose up -d
```

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Setup Buckets, Topics & DB Schemas
```bash
python scripts/setup_infra.py
```

### 4. Start Streaming (two terminals)
```bash
# Terminal 1 — produce events
python scripts/producer.py

# Terminal 2 — consume → MinIO + PostgreSQL
python scripts/consumer.py
```

### 5. Run dbt Transformations
```bash
cd dbt_project
dbt run          # build staging views + mart tables
dbt test         # run data quality checks
dbt docs generate && dbt docs serve   # browse lineage
```

---

## 🌐 Web UIs

| UI | URL | Credentials |
|---|---|---|
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

Copy `.env.example` to `.env` and adjust:

```bash
cp .env.example .env
```

Key settings:

| Variable | Default | Description |
|---|---|---|
| `EVENTS_PER_SECOND` | `10` | Producer throughput |
| `BATCH_SIZE` | `100` | Consumer records-per-flush |
| `FLUSH_INTERVAL` | `30` | Consumer max seconds between flushes |

---

## 📁 Project Structure

```
olist_data_pipeline/
├── docker-compose.yml          # Redpanda, MinIO, PostgreSQL
├── requirements.txt
├── .env.example
├── scripts/
│   ├── setup_infra.py          # One-time: create buckets, topics, DB schema
│   ├── producer.py             # Fake event generator
│   └── consumer.py             # Kafka → MinIO + PostgreSQL
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
