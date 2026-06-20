# Real-Time Banking Transaction Processing Platform

## Overview

This project simulates a real-world banking transaction processing platform using modern Data Engineering technologies.

The platform ingests streaming transaction data from Kafka, processes it using PySpark Structured Streaming, stores raw data in AWS S3 (Bronze Layer), performs data quality validations and incremental processing into a Silver Layer, and tracks pipeline metadata using PostgreSQL.

## Tech Stack

* Python
* Apache Kafka
* Apache Spark (PySpark Structured Streaming)
* AWS S3
* PostgreSQL
* Docker & Docker Compose
* AWS CLI
* Git/GitHub

---

# Architecture

```text
Transaction Producer
        ↓
      Kafka
        ↓
PySpark Structured Streaming
        ↓
 AWS S3 Bronze Layer

        ↓

PySpark Batch Processing
(Bronze → Silver)

        ↓

AWS S3 Silver Layer

        ↓

PostgreSQL
 ├── pipeline_metadata
 └── pipeline_run_history
```

---

# Current Features

### Streaming Ingestion

* Kafka-based transaction ingestion
* Real-time banking transaction generation
* PySpark Structured Streaming consumer
* S3 Bronze Layer storage

### Data Lake

* Bronze Layer
* Silver Layer
* Partitioned Parquet storage

### Data Quality

* Null checks
* Amount validation
* Record deduplication

### Incremental Processing

* Watermark-based processing
* Only new records are processed
* Metadata-driven ETL

### Monitoring & Audit

* Pipeline metadata tracking
* Run history logging
* Records read/written tracking
* Rejected records tracking

---

# Repository Structure

```text
streaming_ingestion/

├── docker/
│   └── docker-compose.yml
│
├── src/
│
│   ├── producer/
│   │   └── banking_tran_producer.py
│   │
│   ├── streaming/
│   │   └── banking_trans_streaming.py
│   │
│   ├── batch/
│   │   └── bronze_to_silver.py
│   |
|   ├── sql/
│       └── postgres_ddl.sql
│
├── docs/
│
├── requirements.txt
│
└── README.md
```

---

# AWS Setup

## Create S3 Bucket

Example:

```text
banking-data-ap-south-1
```

Recommended structure:

```text
banking-data-ap-south-1/

bronze/
silver/
gold/
checkpoints/
quarantine/
```

---

## Configure AWS CLI

Install:

```bash
brew install awscli
```

Configure:

```bash
aws configure
```

Verify:

```bash
aws s3 ls
```

---

# Docker Setup

## Start Services

```bash
docker compose up -d
```

Verify:

```bash
docker ps
```

Expected containers:

```text
kafka
zookeeper
postgres
pgadmin
```

---

## Stop Services

```bash
docker compose down
```

---

## Stop Services And Delete Volumes

WARNING: Deletes Kafka/Postgres persisted data.

```bash
docker compose down -v
```

---

# PostgreSQL Setup

## Create Metadata Table

```sql
CREATE TABLE pipeline_metadata (
    job_name VARCHAR(100) PRIMARY KEY,
    last_processed_timestamp TIMESTAMP,
    last_run_status VARCHAR(20),
    updated_at TIMESTAMP
);
```

---

## Insert Initial Watermark

```sql
INSERT INTO pipeline_metadata
VALUES (
    'bronze_to_silver',
    '1900-01-01',
    'SUCCESS',
    CURRENT_TIMESTAMP
);
```

---

## Create Run History Table

```sql
CREATE TABLE pipeline_run_history (
    run_id UUID PRIMARY KEY,
    job_name VARCHAR(100),
    records_read BIGINT,
    records_written BIGINT,
    records_rejected BIGINT,
    records_duplicates BIGINT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20),
    error_message TEXT
);
```

---

# Kafka Setup

## Create Topic

```bash
docker exec -it kafka bash
```

```bash
kafka-topics \
--create \
--topic banking_transactions \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1
```

---

## Verify Topic

```bash
kafka-topics \
--list \
--bootstrap-server localhost:9092
```

---

# Running The Producer

```bash
python src/producer/banking_trans_producer.py
```

Responsibilities:

* Generate banking transactions
* Publish transactions to Kafka

---

# Running Streaming Consumer

```bash
spark-submit \
--packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
src/streaming/banking_trans_streaming.py
```

Responsibilities:

* Consume Kafka stream
* Parse JSON
* Validate records
* Write Parquet files to Bronze Layer

---

# Running Bronze To Silver Job

```bash
spark-submit \
--packages \
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
src/batch/bronze_to_silver.py
```

Responsibilities:

* Read Bronze Layer
* Incremental processing using watermark
* Data quality validation
* Deduplication
* Write Silver Layer
* Update metadata tables

---

# S3 Data Layout

## Bronze Layer

```text
bronze/
└── transactions/
    └── year=YYYY/
        └── month=MM/
            └── day=DD/
```

---

## Silver Layer

```text
silver/
└── transactions/
    └── year=YYYY/
        └── month=MM/
            └── day=DD/
```

---

# Incremental Processing Logic

Watermark stored in:

```text
pipeline_metadata
```

Example:

```text
last_processed_timestamp
```

Workflow:

```text
Read Watermark
       ↓
Read Bronze
       ↓
Filter New Records
       ↓
Validate
       ↓
Deduplicate
       ↓
Write Silver
       ↓
Update Watermark
```

---

# Useful Commands

## View PostgreSQL Logs

```bash
docker logs postgres
```

---

## View Kafka Logs

```bash
docker logs kafka
```

---

## Connect To PostgreSQL

```bash
docker exec -it postgres psql -U admin -d banking_platform
```

---

## Verify Watermark

```sql
SELECT *
FROM pipeline_metadata;
```

---

## Verify Run History

```sql
SELECT *
FROM pipeline_run_history
ORDER BY start_time DESC;
```

---

# Future Enhancements

## Phase 3

Customer Master Stream

* customer_updates Kafka topic
* Customer Bronze Layer
* Customer Silver Layer

## Phase 4

SCD Type 2

* Historical customer tracking
* Customer dimension table

## Phase 5

Fraud Detection

* High-value transactions
* Velocity checks
* Risk-based alerts

## Phase 6

Apache Airflow

* Pipeline orchestration
* Scheduling
* Monitoring

## Phase 7

Analytics Layer

* PostgreSQL warehouse
* Tableau dashboards
