#!/bin/bash
set -e

# Ensure SPARK_HOME is set
export SPARK_HOME=${SPARK_HOME:-"/opt/spark"}

# Create the event logging directory required by spark-defaults.conf
mkdir -p /home/iceberg/spark-events
mkdir -p $SPARK_HOME/logs

echo "Waiting for MinIO and REST Catalog..."
# Simple wait loop to ensure services are ready
sleep 10

echo "Running Iceberg table creation scripts..."
# We rely on spark-defaults.conf for catalog configuration
$SPARK_HOME/bin/spark-sql \
    --master local[*] \
    --name "Create Bronze Tables" \
    -f /home/iceberg/scripts/sql/bronze_schema.sql

$SPARK_HOME/bin/spark-sql \
    --master local[*] \
    --name "Create Silver Tables" \
    -f /home/iceberg/scripts/sql/silver_schema.sql

$SPARK_HOME/bin/spark-sql \
    --master local[*] \
    --name "Create Gold Tables" \
    -f /home/iceberg/scripts/sql/gold_schema.sql

echo "Iceberg tables created (or verified existing)."

# Check if arguments are available
echo "Received argument: '$1'"
if [ "$1" = "--etl" ]; then
    echo "Running Full ETL (MinIO + Postgres)..."
    $SPARK_HOME/bin/spark-submit --master local[*] --name "ETL MinIO" /home/iceberg/scripts/minio_loader.py
    $SPARK_HOME/bin/spark-submit --master local[*] --jars /opt/spark/jars/postgresql-42.7.2.jar --name "ETL Postgres" /home/iceberg/scripts/postgres_loader.py
    exit 0
fi

if [ "$1" = "--etl-minio" ]; then
    echo "Running MinIO ETL..."
    $SPARK_HOME/bin/spark-submit --master local[*] --name "ETL MinIO" /home/iceberg/scripts/minio_loader.py
    exit 0
fi

if [ "$1" = "--etl-postgres" ]; then
    echo "Running Postgres ETL..."
    $SPARK_HOME/bin/spark-submit --master local[*] --jars /opt/spark/jars/postgresql-42.7.2.jar --name "ETL Postgres" /home/iceberg/scripts/postgres_loader.py
    exit 0
fi

# Check if arguments are passed to the script (other than --etl)
if [ "$#" -gt 0 ]; then
    exec "$@"
else
    # Default to starting the Thrift server if no command is specified
    echo "Starting Spark Thrift Server..."
    $SPARK_HOME/sbin/start-thriftserver.sh \
        --master local[*] \
        --name "Spark Thrift Server"

    echo "Thrift Server started. Tailing logs..."
    
    # Wait for log file to be created
    sleep 5
    
    # Tail all logs in the directory
    tail -F $SPARK_HOME/logs/*
fi
