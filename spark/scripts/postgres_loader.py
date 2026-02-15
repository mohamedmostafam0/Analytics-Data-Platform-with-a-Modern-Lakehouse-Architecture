import os
import sys
from pyspark.sql.functions import current_timestamp, col
from etl_utils import get_spark_session, write_to_iceberg, retry_with_backoff

# ... imports ...

@retry_with_backoff(retries=3, backoff_in_seconds=5)
def get_jdbc_options(table_name):
    # ... existing implementation ...
    pg_user = Config.postgres_user
    pg_password = Config.postgres_password
    
    if not pg_user or not pg_password:
        raise EnvironmentError("Missing required Postgres credentials.")

    return {
        "url": f"jdbc:postgresql://{Config.postgres_host}:{Config.postgres_port}/{Config.postgres_db}",
        "dbtable": table_name,
        "user": pg_user,
        "password": pg_password,
        "driver": "org.postgresql.Driver"
    }

@retry_with_backoff(retries=3, backoff_in_seconds=5)
def get_spark_session_with_retry(app_name):
    return get_spark_session(app_name)

def run_postgres_loader(spark=None):
    created_spark = False
    try:
        if spark is None:
            spark = get_spark_session_with_retry("ETL Postgres Loader")
            created_spark = True
        
        tables_to_load = ["users", "items", "purchases"]
        
        for table in tables_to_load:
            logger.info(f"Processing {table} from Postgres...")
            try:
                # 1. Scalability: Get min/max ID for partitioning
                try:
                    min_max_df = spark.read.format("jdbc").options(**get_jdbc_options(f"(select min(id), max(id) from {table}) as subq")).load()
                    min_id, max_id = min_max_df.collect()[0]
                    # Handle empty table case
                    if min_id is None or max_id is None:
                         logger.info(f"Table {table} is empty. Skipping.")
                         continue
                except Exception as e:
                     logger.warning(f"Could not get min/max id for {table}, defaulting to non-partitioned read: {e}")
                     min_id, max_id = None, None

                # 2. Idempotency: Get max updated_at from target Iceberg table
                target_table = f"bronze.{table}"
                watermark = None
                try:
                    # Check if target table exists and get max updated_at
                    # We use a try-except block because table might not exist on first run
                    rows = spark.sql(f"SELECT max(updated_at) FROM {target_table}").collect()
                    if rows and rows[0][0]:
                        watermark = rows[0][0]
                        logger.info(f"Found watermark for {target_table}: {watermark}")
                except Exception:
                    logger.info(f"Target table {target_table} does not exist or is empty. Performing full load.")

                # 3. Configure Read Options
                read_options = get_jdbc_options(table)
                
                # Add partitioning if IDs are available
                if min_id is not None and max_id is not None:
                    read_options["partitionColumn"] = "id"
                    read_options["lowerBound"] = str(min_id)
                    read_options["upperBound"] = str(max_id)
                    read_options["numPartitions"] = "10" # Adjust based on executor count/data size

                # Apply Watermark Filter
                if watermark:
                    read_options["dbtable"] = f"(SELECT * FROM {table} WHERE updated_at > '{watermark}') as subq"

                # Define a helper function to read from JDBC with retry
                @retry_with_backoff(retries=3, backoff_in_seconds=5)
                def read_jdbc_with_retry(options):
                    return spark.read.format("jdbc").options(**options).load()

                # Read from Postgres
                df = read_jdbc_with_retry(read_options)
                
                # Add metadata
                df = df.withColumn("ingested_at", current_timestamp())
                
                # Write to Iceberg
                write_to_iceberg(df, target_table)
                
            except Exception as e:
                logger.error(f"Error processing table {table}: {e}")
    
    except Exception as e:
        logger.critical(f"Critical failure in Postgres loader: {e}")
    finally:
        if created_spark and spark:
            spark.stop()

if __name__ == "__main__":
    run_postgres_loader()
