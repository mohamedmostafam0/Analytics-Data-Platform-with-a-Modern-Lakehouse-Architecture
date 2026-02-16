#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "================================================================"
echo " Starting Lakehouse Preparation Pipeline"
echo "================================================================"

echo "[1/6] Creating Bronze layer schema..."
docker compose exec spark-iceberg /opt/spark/bin/spark-sql -f /home/iceberg/scripts/sql/bronze_schema.sql

echo "[2/6] Creating Silver layer schema..."
docker compose exec spark-iceberg /opt/spark/bin/spark-sql -f /home/iceberg/scripts/sql/silver_schema.sql

echo "[3/6] Creating Gold layer schema..."
docker compose exec spark-iceberg /opt/spark/bin/spark-sql -f /home/iceberg/scripts/sql/gold_schema.sql

echo "[4/6] Loading data from Postgres to Bronze..."
docker compose exec spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/scripts/postgres_loader.py

echo "[5/6] Loading data from MinIO to Bronze..."
docker compose exec spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/scripts/minio_loader.py

echo "[6/6] Transforming Bronze → Silver → Gold..."
docker compose exec spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/scripts/bronze_to_silver_transformer.py
docker compose exec spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/scripts/silver_to_gold_transformer.py

echo "================================================================"
echo " Lakehouse preparation pipeline completed successfully."
echo "================================================================"