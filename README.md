<div align="center">

<!-- Banner -->
<img src="https://capsule-render.vercel.app/api?type=waving&color=0:0ea5e9,100:6366f1&height=200&section=header&text=E-Commerce%20Sales%20Data%20Pipeline&fontSize=36&fontColor=ffffff&fontAlignY=38&desc=Real-Time%20Streaming%20%7C%20Medallion%20Architecture%20%7C%20Business%20Intelligence&descAlignY=58&descSize=16" width="100%"/>

<!-- Badges -->
<p>
  <img src="https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white"/>
  <img src="https://img.shields.io/badge/Microsoft%20Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white"/>
  <img src="https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white"/>
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Delta%20Lake-00ADD4?style=for-the-badge&logo=deltalake&logoColor=white"/>
</p>

<!-- Stats -->
<p>
  <img src="https://img.shields.io/badge/Kafka%20Topics-5-blue?style=flat-square"/>
  <img src="https://img.shields.io/badge/dbt%20Models-10-orange?style=flat-square"/>
  <img src="https://img.shields.io/badge/dbt%20Tests-17%20PASS-brightgreen?style=flat-square"/>
  <img src="https://img.shields.io/badge/Pipeline%20Layers-4-purple?style=flat-square"/>
  <img src="https://img.shields.io/badge/License-MIT-green?style=flat-square"/>
</p>

</div>

---

## 📌 Overview

An end-to-end **streaming data pipeline** that ingests real-time e-commerce events, processes them through a **Medallion Architecture** (Bronze → Silver → Gold → Marts), and delivers actionable business intelligence via a **Databricks Dashboard**.

---

## 🎯 Business Goals

E-commerce businesses generate thousands of events every second — orders, payments, customer signups, inventory updates. This pipeline turns raw event streams into actionable intelligence in **near real-time**:

| # | Business Problem | What We Answer |
|---|---|---|
| 💰 | **Revenue Tracking** | How much revenue are we generating daily? What is the average order value? |
| 📦 | **Product Performance** | Which products are selling the most? Which categories drive the most revenue? |
| 👥 | **Customer Intelligence** | Who are our high-value customers? What is the customer lifetime value? |
| 💳 | **Payment Analytics** | What is the payment success rate? Which payment methods are most used? |
| 🏭 | **Inventory Visibility** | Which products are low on stock? When was the last restock? |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     🐍 Python Faker Producer                         │
│          Simulates real e-commerce events in 5 Kafka topics          │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│              ⚡ Apache Kafka (KRaft Mode — No Zookeeper)             │
│                                                                       │
│  ecom.orders │ ecom.payments │ ecom.customers │ ecom.inventory        │
│                         ecom.clickstream                              │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│            🔌 Kafka Connect (Azure Blob Storage Sink)                │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│              ☁️  Azure Data Lake Gen2 — BRONZE LAYER                 │
│           /bronze/orders/year=2026/month=03/day=17/                  │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│          🧱 Azure Databricks + PySpark (Medallion Architecture)      │
│                                                                       │
│   🥉 Bronze  →  Raw JSON via Auto Loader (no transformation)         │
│   🥈 Silver  →  Cleaned, typed, deduplicated Delta tables            │
│   🥇 Gold    →  Business aggregations (revenue, LTV, payments)       │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│               🔶 dbt on Databricks SQL Warehouse                     │
│                                                                       │
│  Staging → stg_orders, stg_payments, stg_customers, stg_inventory   │
│  Intermediate → int_orders_payments, int_customer_orders             │
│  Marts → fct_orders, dim_customers, dim_products, revenue_daily      │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│           ⏰ Apache Airflow  (Daily Orchestration — 6:00 AM)         │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│               📊 Databricks Dashboard (4 Visualizations)             │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| <img src="https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white"/> | 7.6.0 | Real-time event streaming (KRaft — no Zookeeper) |
| <img src="https://img.shields.io/badge/Kafka%20Connect-231F20?style=flat-square&logo=apachekafka&logoColor=white"/> | 7.6.0 | Sink Kafka events to Azure Data Lake |
| <img src="https://img.shields.io/badge/Azure%20Data%20Lake-0078D4?style=flat-square&logo=microsoftazure&logoColor=white"/> | Gen2 | Scalable raw data storage |
| <img src="https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white"/> | Latest | Unified data processing and analytics platform |
| <img src="https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apachespark&logoColor=white"/> | 4.0.0 | Distributed data transformation |
| <img src="https://img.shields.io/badge/Delta%20Lake-00ADD4?style=flat-square&logo=deltalake&logoColor=white"/> | Latest | ACID transactions on data lake |
| <img src="https://img.shields.io/badge/dbt-FF694B?style=flat-square&logo=dbt&logoColor=white"/> | 1.6.0 | SQL-based data modeling and testing |
| <img src="https://img.shields.io/badge/Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white"/> | 2.8.1 | Pipeline orchestration and scheduling |
| <img src="https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white"/> | Latest | Containerized local development |
| <img src="https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white"/> | 3.10 | Event producer and scripting |

---

## 🔄 How We Built It

<details>
<summary><b>⚡ Step 1 — Data Generation (Kafka Producer)</b></summary>

<br/>

A Python Faker producer simulates real e-commerce events and continuously publishes them to **5 Kafka topics**. Each event follows a JSON schema with realistic fields like order IDs, product names, payment methods, and customer details.

```
ecom.orders      → order placed events
ecom.payments    → payment success/failure events
ecom.customers   → customer signup events
ecom.inventory   → stock update events
ecom.clickstream → product view events
```
</details>

<details>
<summary><b>🔌 Step 2 — Streaming Ingestion (Kafka + Kafka Connect)</b></summary>

<br/>

Apache Kafka running in **KRaft mode** (no Zookeeper) acts as the message broker. Kafka Connect with the Azure Blob Storage Sink Connector automatically flushes topic data into **Azure Data Lake Gen2** in partitioned JSON format:

```
/bronze/orders/year=2026/month=03/day=17/
```
</details>

<details>
<summary><b>🥉 Step 3 — Bronze Layer (Databricks Auto Loader)</b></summary>

<br/>

Databricks Auto Loader incrementally ingests new JSON files from ADLS as they arrive. Raw data is stored **as-is** in Delta format — no transformations, just schema inference and timestamp tracking.
</details>

<details>
<summary><b>🥈 Step 4 — Silver Layer (PySpark Cleaning)</b></summary>

<br/>

PySpark notebooks clean and enrich the raw Bronze data:

- ✅ Cast data types (strings → timestamps, decimals)
- ✅ Handle null values and duplicates
- ✅ Add derived columns (`full_name`, `quantity_available`, `line_total`)
- ✅ Write cleaned data as Delta tables in the `silver` schema
</details>

<details>
<summary><b>🥇 Step 5 — Gold Layer (Business Aggregations)</b></summary>

<br/>

PySpark notebooks aggregate Silver data into business-ready tables:

| Table | Description |
|---|---|
| `daily_revenue` | Revenue, orders, AOV per day |
| `top_products` | Best-selling products by revenue and units |
| `customer_ltv` | Lifetime value, segments, days as customer |
| `payment_summary` | Success rates by payment method and gateway |

</details>

<details>
<summary><b>🔶 Step 6 — dbt Modeling</b></summary>

<br/>

dbt connects to **Databricks SQL Warehouse** and builds a clean analytics layer on top of Silver tables:

```
Staging (Views)           Intermediate (Views)         Marts (Tables)
─────────────────         ────────────────────         ──────────────────
stg_orders            →   int_orders_payments      →   fct_orders
stg_payments          →   int_customer_orders      →   dim_customers
stg_customers                                      →   dim_products
stg_inventory                                      →   revenue_daily
```

> 🟢 **dbt test results: PASS=17 &nbsp; WARN=0 &nbsp; ERROR=0**
</details>

<details>
<summary><b>⏰ Step 7 — Orchestration (Airflow DAG)</b></summary>

<br/>

Apache Airflow DAG `ecommerce_pipeline` runs daily at **6:00 AM** and orchestrates the full pipeline:

```
bronze_ingest
      ↓
silver_clean_orders ──┐
silver_clean_payments─┤  (parallel)
silver_clean_customers┤
silver_clean_inventory┘
      ↓
gold_daily_revenue ───┐
gold_top_products ────┤  (parallel)
gold_customer_ltv ────┤
gold_payment_summary ─┘
      ↓
dbt_run → dbt_test
```
</details>

---

## 📊 Dashboard

> A **Databricks Dashboard** built directly on top of the Gold layer Delta tables, providing real-time business insights across 4 key visualizations.

---

### 📈 Chart 1 — Daily Revenue Trend *(Line Chart)*

Tracks **total revenue**, **total orders**, and **average order value** over time. Helps the business understand revenue growth patterns and identify peak sales periods.

<img width="1090" height="396" alt="Daily Revenue Trend" src="https://github.com/user-attachments/assets/4cefc74e-c80b-44a5-b06b-7c1246caf64c"/>

> 📌 *Source table: `silver_gold.revenue_daily`*

---

### 🏆 Chart 2 — Top 10 Products by Revenue *(Bar Chart)*

Shows the **best-performing products** ranked by total revenue and units sold, broken down by category. Helps merchandising teams make restocking and promotion decisions.

<img width="540" height="396" alt="Top 10 Products by Revenue" src="https://github.com/user-attachments/assets/4da7bc19-9cd2-42ea-a665-8d3262814ab2"/>

> 📌 *Source table: `silver_gold.fct_orders`*

---

### 👥 Chart 3 — Customer Segmentation *(Pie Chart)*

Breaks down customers into **high_value**, **mid_value**, and **low_value** tiers based on total spend. Helps marketing teams target the right customers with the right campaigns.

<img width="540" height="396" alt="Revenue by Customer Tier" src="https://github.com/user-attachments/assets/afd22499-8663-43c8-b1c1-d257523d5fac"/>

> 📌 *Source table: `silver_gold.dim_customers`*

---

### 💳 Chart 4 — Payment Success Rate *(Bar Chart)*

Shows **success and failure rates** per payment method. Helps the payments team identify underperforming gateways and reduce transaction failures.

<img width="1090" height="396" alt="Payment Method Performance" src="https://github.com/user-attachments/assets/8ca3943a-b6d8-409a-8953-7c7eb08462e4"/>

> 📌 *Source table: `silver_gold.fct_orders`*

---

### 🗃️ Dashboard Datasets

| Dataset | Source Table | Description |
|---|---|---|
| `daily_revenue` | `silver_gold.revenue_daily` | Daily revenue aggregations |
| `top_products` | `silver_gold.fct_orders` | Product performance metrics |
| `customer_segments` | `silver_gold.dim_customers` | Customer tier breakdown |
| `payment_summary` | `silver_gold.fct_orders` | Payment method analytics |

---

## 🗂️ Project Structure

```
ecommerce-data-pipeline/
│
├── 🐍 producer/
│   ├── faker_producer.py          # Generates fake e-commerce events
│   ├── Dockerfile
│   ├── requirements.txt
│   └── schema/
│       ├── order_schema.json
│       ├── payment_schema.json
│       ├── customer_schema.json
│       ├── inventory_schema.json
│       └── clickstream_schema.json
│
├── 🔥 databricks/
│   ├── setup/
│   │   ├── 00_mount_storage.py    # Mount ADLS to Databricks
│   │   └── 01_create_databases.py # Create silver/gold schemas
│   ├── bronze/
│   │   └── 01_ingest_raw.py       # Auto Loader ingestion
│   ├── silver/
│   │   ├── clean_orders.py
│   │   ├── clean_payments.py
│   │   ├── clean_customers.py
│   │   └── clean_inventory.py
│   └── gold/
│       ├── daily_revenue.py
│       ├── top_products.py
│       ├── customer_ltv.py
│       └── payment_summary.py
│
├── 🔶 dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_payments.sql
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_inventory.sql
│   │   │   └── schema.yml
│   │   ├── intermediate/
│   │   │   ├── int_orders_payments.sql
│   │   │   └── int_customer_orders.sql
│   │   └── marts/
│   │       ├── fct_orders.sql
│   │       ├── dim_customers.sql
│   │       ├── dim_products.sql
│   │       ├── revenue_daily.sql
│   │       └── schema.yml
│   ├── profiles.yml
│   ├── dbt_project.yml
│   └── packages.yml
│
├── ⏰ airflow/
│   └── dags/
│       └── ecommerce_pipeline_dag.py
│
├── 🔌 kafka-connect-configs/
│   └── adls_sink_connector.json
│
├── 🐳 docker-compose.yml
├── dbt_project/Dockerfile
└── .env.example
```

---

## 🥇 Medallion Architecture

```
┌──────────────────────────────────────────────────────────┐
│  🥉 BRONZE — ADLS Gen2 (JSON → Delta)                    │
│  Raw events, no transformation, schema inference only    │
└──────────────────────────────┬───────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────┐
│  🥈 SILVER — `silver` schema (Delta)                     │
│  Cleaned, typed, deduplicated, enriched with new columns │
└──────────────────────────────┬───────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────┐
│  🥇 GOLD — `gold` schema (Delta)                         │
│  Business aggregations: revenue, LTV, payments, products │
└──────────────────────────────┬───────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────┐
│  📐 MARTS — `silver_gold` schema (Delta via dbt)         │
│  Analytics-ready: fct_orders, dim_customers, dim_products│
└──────────────────────────────────────────────────────────┘
```

---

## 🚀 Setup & Running Locally

### Prerequisites

- 🐳 Docker Desktop
- ☁️ Azure account (Data Lake Gen2 + Databricks workspace)
- 🐍 Python 3.10+

### 1. Clone the repo

```bash
git clone https://github.com/Khanapatro/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline
```

### 2. Create `.env` file

```env
AZURE_STORAGE_ACCOUNT_NAME=your_storage_account
AZURE_STORAGE_ACCOUNT_KEY=your_storage_key
DATABRICKS_HOST=adb-xxxx.azuredatabricks.net
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxx
DATABRICKS_TOKEN=dapixxxxxxxxxxxx
```

### 3. Start all services

```bash
docker-compose up -d
```

### 4. Start the producer

```bash
docker-compose --profile producer up -d
```

### 5. Run dbt models

```bash
docker exec -it dbt dbt run
docker exec -it dbt dbt test
```

### 6. Access services

| Service | URL | Credentials |
|---|---|---|
| 🖥️ Kafka UI | http://localhost:8080 | — |
| ⏰ Airflow | http://localhost:8081 | `admin` / `admin` |
| 🔌 Kafka Connect API | http://localhost:8083 | — |

---

## 🚨 Pipeline Failure Alerting

If anything breaks in the pipeline, **you will be automatically notified via email** — no manual monitoring needed.

Airflow is configured with `email_on_failure=True` in the DAG's `default_args`. The moment any task fails — whether it's `bronze_ingest`, a `silver_clean_*` job, `dbt_run`, or `dbt_test` — Airflow immediately fires an email alert containing the failed task name, error traceback, and timestamp.

```python
default_args = {
    'owner': 'airflow',
    'email': ['yourteam@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
```

The pipeline also retries automatically **2 times** before sending the failure alert, so transient errors (network blips, cluster startup delays) are handled gracefully without spamming your inbox.

| Failure Point | What Triggers the Alert |
|---|---|
| ⚡ `bronze_ingest` | Auto Loader fails to read new files from ADLS |
| 🥈 `silver_clean_*` | PySpark job errors — bad data, schema mismatch |
| 🥇 `gold_*` | Aggregation job fails — missing upstream Silver table |
| 🔶 `dbt_run` | A dbt model fails to build |
| ✅ `dbt_test` | A data quality test fails (nulls, duplicates, row count) |

> 📬 **Every failure at any layer sends an email** — so the pipeline is fully observable end-to-end without any external monitoring tool.

---

## 📊 Results

<div align="center">

| Metric | Value |
|---|---|
| ⚡ Kafka topics | 5 topics, 3 partitions each |
| 🔶 dbt models | 10 models (4 staging · 2 intermediate · 4 marts) |
| ✅ dbt tests | **PASS=17 &nbsp; WARN=0 &nbsp; ERROR=0** |
| 🧱 Pipeline layers | Bronze → Silver → Gold → Marts |
| ⏰ Airflow schedule | Daily at 6:00 AM |
| 📊 Dashboard charts | 4 visualizations |

</div>

---

## 📄 License

<div align="center">

This project is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for details.

<br/>

<img src="https://capsule-render.vercel.app/api?type=waving&color=0:6366f1,100:0ea5e9&height=100&section=footer" width="100%"/>

</div>
