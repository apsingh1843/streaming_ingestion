# Real-Time Banking Data Platform
Version 1.1

## Overview

This project simulates a production-style banking data platform using modern Data Engineering technologies.

The platform ingests customer and transaction events through Kafka, processes them using PySpark Structured Streaming, stores data in an AWS S3-based data lake (Bronze/Silver architecture), and loads curated customer data into a PostgreSQL warehouse using Slowly Changing Dimension (SCD Type 2) processing.

The project demonstrates:

* Real-time streaming ingestion
* Incremental data processing
* Data lake architecture
* Data warehousing
* SCD Type 2 implementation
* Data quality validation
* Metadata-driven ETL
* Pipeline observability and audit logging

---

# Architecture

```text
                    +--------------------+
                    | Kafka Producers    |
                    +--------------------+
                              |
        +---------------------------------------------+
        |                                             |
        v                                             v

banking_transactions topic                 customer_updates topic
        |                                             |
        v                                             v

Transaction Streaming Job               Customer Streaming Job
(PySpark Structured Streaming)          (PySpark Structured Streaming)

        |                                             |
        +------------------+--------------------------+
                           |
                           v

                    AWS S3 Bronze Layer

                           |
                           v

                Bronze → Silver Processing
                    (Incremental ETL)

                           |
                           v

                    AWS S3 Silver Layer

                           |
                           v

                 Customer SCD Type 2 Job

                           |
                           v

                  PostgreSQL Warehouse

                           |
                           v

                 customer_dimension
```

---

# Technology Stack

| Component         | Technology             |
| ----------------- | ---------------------- |
| Programming       | Python                 |
| Streaming         | Apache Kafka           |
| Processing        | Apache Spark (PySpark) |
| Storage           | AWS S3                 |
| Warehouse         | PostgreSQL             |
| Containerization  | Docker                 |
| Metadata Tracking | PostgreSQL             |
| Version Control   | Git / GitHub           |

---

# Project Structure

```text
streaming_ingestion/

├── docker/
│   └── docker-compose.yml
│
├── src/
│
│   ├── producer/
│   │   ├── banking_trans_producer.py
│   │   └── customer_update_producer.py
│   │
│   ├── streaming/
│   │   ├── banking_trans_streaming.py
│   │   └── customer_update_streaming.py
│   │
│   ├── batch/
│   │   ├── trans_bronze_to_silver.py
│   │   └── cust_bronze_to_silver.py
│   │   └── cust_silver_to_wh.py
│   │
│   ├── sql/
│   │   └── cust_etl/
│   │   │    ├── create_customer_dim_data.sql
│   │   │    ├── create_customer_staging.sql
│   │   │    ├── create_customer_changes.sql
│   │   │    ├── load_customer_changes.sql
│   │   │    ├── insert_new_customers.sql
│   │   │    ├── expire_existing_customers.sql
│   │   │    └── insert_new_versions.sql
│   │   └── postgres_ddl.sql
│   │
│   └── utils/
│       ├── postgres_utils.py
│
├── requirements.txt
├── README.md
└── .gitignore
```

---

# Data Lake Layout

```text
s3://banking-data-ap-south-1/

├── bronze/
│   ├── transactions/
│   └── customers/
│
├── silver/
│   ├── transactions/
│   └── customers/
│
├── checkpoints/
│
├── quarantine/
│
└── gold/
```

---

# Getting Started

## Prerequisites

Install the following:

* Python 3.11+
* Java 17
* Apache Spark 3.5+
* Docker Desktop
* AWS CLI

Verify installations:

```bash
python --version
java -version
spark-submit --version
docker --version
aws --version
```

---

## Configure AWS

```bash
aws configure
```

Verify:

```bash
aws sts get-caller-identity
```

---

## Clone Repository

```bash
git clone https://github.com/apsingh1843/streaming_ingestion.git

cd streaming_ingestion
```

---

## Make Scripts Executable

```bash
chmod +x scripts/start_env.sh
chmod +x scripts/bootstrap.sh
```

---

## Create Virtual Environment

```bash
python -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt
```

---

## Start Project

Activate environment:

```bash
source scripts/start_env.sh
```

Bootstrap infrastructure:

```bash
./scripts/bootstrap.sh
```

This will:

* Start Kafka, Zookeeper, PostgreSQL and pgAdmin
* Create Kafka topics
* Create metadata tables
* Seed initial pipeline metadata

## Docker Commands

Start services:

```bash
docker compose up -d
```

Verify:

```bash
docker ps
```

Stop services:

```bash
docker compose down
```

Remove volumes:

```bash
docker compose down -v
```

---

# Components Implemented

## 1. Kafka Producers

### Banking Transaction Producer

Generates synthetic transaction events:

```json
{
  "transaction_id": "a554ab69-1307-4003-8417-b335ac149406",
  "customer_id":"CUST7969",
  "account_id":"ACC33061",
  "transaction_type":"ATM",
  "merchant":"Amazon",
  "amount":112106.97,
  "city":"Lucknow",
  "channel":null,
  "event_timestamp":"2026-06-20T17:03:37.863399"
}
```

---

### Customer Update Producer

Generates customer profile updates:

```json
{
  "customer_id": "CUST1001",
  "customer_name": "Rahul Sharma",
  "email": "rahul@gmail.com",
  "phone": "9999999999",
  "city": "Mumbai",
  "risk_rating": "LOW",
  "update_timestamp": "2026-06-21T10:00:00"
}
```

---

# 2. Streaming Layer

Implemented using PySpark Structured Streaming.

## Transaction Stream

Kafka → Bronze Transactions

Responsibilities:

* Read Kafka messages
* Parse JSON
* Validate schema
* Add ingestion metadata
* Write Parquet files to S3 Bronze

---

## Customer Stream

Kafka → Bronze Customers

Responsibilities:

* Read customer updates
* Parse JSON
* Add ingestion timestamp
* Partition data by date
* Store in Bronze Layer

---

# 3. Bronze → Silver Processing

## Transactions

Script:

```text
src/batch/trans_bronze_to_silver.py
```

Features:

* Incremental processing
* Watermark tracking
* Data validation
* Deduplication
* Run history tracking

Validation Rules:

* transaction_id not null
* customer_id not null
* account_id not null
* amount > 0

---

## Customers

Script:

```text
src/batch/cust_bronze_to_silver.py
```

Features:

* Incremental processing
* Watermark tracking
* Validation
* Deduplication
* Metadata logging

Validation Rules:

* customer_id not null
* customer_name not null
* email not null

---

# Incremental Processing

Implemented using metadata-driven watermarking.

Table:

```sql
pipeline_metadata
```

Example:

| job_name                  | last_processed_timestamp |
| ------------------------- | ------------------------ |
| trans_bronze_to_silver    | 2026-06-21 10:00:00      |
| cust_bronze_to_silver     | 2026-06-21 10:05:00      |
| cust_silver_to_wh         | 2026-06-21 10:10:00      |

Processing Logic:

```text
Read Watermark
        ↓
Read Source
        ↓
Filter New Records
        ↓
Process Delta
        ↓
Update Watermark
```

---

# Pipeline Observability

## Metadata Table

```sql
pipeline_metadata
```

Tracks:

* Job name
* Last processed timestamp
* Job status

---

## Run History Table

```sql
pipeline_run_history
```

Tracks:

* Run ID
* Records Read
* Records Written
* Rejected Records
* Duplicate Records
* Status
* Start Time
* End Time

---

# Warehouse Layer

PostgreSQL is used as the warehouse.

---

## Customer Staging Table

```sql
customer_silver_stg
```

Stores latest incremental customer batch.

---

## Customer Changes Table

```sql
customer_changes
```

Stores detected customer changes requiring SCD processing.

---

## Customer Dimension

```sql
customer_dim_data
```

Columns:

```text
customer_sk
customer_id
customer_name
email
phone
city
risk_rating
effective_from
effective_to
is_current
created_at
```

---

# SCD Type 2 Implementation

Script:

```text
src/batch/cust_silver_to_wh.py
```

Flow:

```text
Read Watermark
        ↓

Read Silver Incrementally
        ↓

Keep Latest Customer Update
        ↓

Load customer_silver_stg
        ↓

Detect Changes
        ↓

Expire Existing Records
        ↓

Insert New Version
        ↓

Update Watermark
```

---

## Example

### Initial Record

| customer_id | city   | risk_rating | is_current |
| ----------- | ------ | ----------- | ---------- |
| CUST1001    | Mumbai | LOW         | TRUE       |

---

### Updated Record

| customer_id | city | risk_rating |
| ----------- | ---- | ----------- |
| CUST1001    | Pune | MEDIUM      |

---

### Dimension Result

| customer_sk | customer_id | city   | risk_rating | is_current |
| ----------- | ----------- | ------ | ----------- | ---------- |
| 1           | CUST1001    | Mumbai | LOW         | FALSE      |
| 2           | CUST1001    | Pune   | MEDIUM      | TRUE       |

---

# Running the Project

## Start Kafka Producers

```bash
python src/producer/banking_trans_producer.py
```

```bash
python src/producer/customer_update_producer.py
```

---

## Start Streaming Jobs

### Transactions

```bash
spark-submit \
--packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
src/streaming/banking_trans_streaming.py
```

---

### Customers

```bash
spark-submit \
--packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
src/streaming/customer_update_streaming.py
```

---

## Run Bronze → Silver Jobs

```bash
spark-submit \
--packages \
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
src/batch/trans_bronze_to_silver.py
```

```bash
spark-submit \
--packages \
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
src/batch/cust_bronze_to_silver.py
```

---

## Run Silver to Warehouse (SCD2) Job

```bash
spark-submit \
--packages \
org.postgresql:postgresql:42.7.3,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
src/scd/cust_silver_to_wh.py
```

---

# Future Enhancements

## Phase 5

Fraud Detection Engine

* High-value transactions
* Velocity checks
* Geographic anomaly detection

---

## Phase 6

Apache Airflow

* DAG orchestration
* Scheduling
* Dependency management
* Monitoring

---

## Phase 7

Analytics Layer

* Gold tables
* Aggregations
* Customer KPIs

---

## Phase 8

Visualization

* Tableau Dashboard
* Transaction Analytics
* Customer Risk Dashboard

---

## Phase 9

Infrastructure as Code

* Terraform
* Automated AWS provisioning