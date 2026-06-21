#!/bin/bash

echo "======================================"
echo "Starting Bootstrap"
echo "======================================"

# Activate venv
source .venv/bin/activate

echo "======================================"
echo "Installing Requirements"
echo "======================================"

pip install -r requirements.txt

echo "======================================"
echo "Starting Docker Services"
echo "======================================"

docker compose -f docker/docker-compose.yml up -d

echo "Waiting for containers..."
sleep 20

echo "======================================"
echo "Container Status"
echo "======================================"

docker ps

echo "======================================"
echo "Creating Kafka Topics"
echo "======================================"

docker exec kafka kafka-topics \
--create \
--if-not-exists \
--topic banking_transactions \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1

docker exec kafka kafka-topics \
--create \
--if-not-exists \
--topic customer_updates \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1

echo "======================================"
echo "Kafka Topics"
echo "======================================"

docker exec kafka kafka-topics \
--list \
--bootstrap-server localhost:9092

echo "======================================"
echo "Initializing PostgreSQL"
echo "======================================"

python scripts/init_postgres.py

echo "======================================"
echo "Bootstrap Complete"
echo "======================================"