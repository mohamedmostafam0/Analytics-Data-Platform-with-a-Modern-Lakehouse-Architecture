import config
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import current_timestamp, input_file_name, col
from etl_utils import get_spark_session, write_to_iceberg, retry_with_backoff
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MinIOLoader")

@retry_with_backoff(retries=3, backoff_in_seconds=5)
def get_spark_session_with_retry(app_name):
    return get_spark_session(app_name)

def run_minio_loader(spark=None):
    created_spark = False
    try:
        if spark is None:
            spark = get_spark_session_with_retry("ETL MinIO Loader")
            created_spark = True
        
        logger.info("Processing Pageviews from MinIO...")
        
        # Define Explicit Schema including corrupt record column
        schema = StructType([
            StructField("user_id", LongType(), True),
            StructField("url", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("received_at", LongType(), True),
            StructField("_corrupt_record", StringType(), True)
        ])
        
        try:
             # Read from MinIO using Structured Streaming
             # Use PERMISSIVE mode to handle malformed records
            df = spark.readStream \
                .schema(schema) \
                .option("maxFilesPerTrigger", 1000) \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .json(f"{config.Config.s3_endpoint}/pageviews/*" if "localhost" in config.Config.s3_endpoint else "s3a://pageviews/*")
            
            # Split into valid and error DFs
            # Valid records have no corrupt record
            valid_df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
            
            # Error records have corrupt record
            error_df = df.filter(col("_corrupt_record").isNotNull()) \
                .select(
                    current_timestamp().alias("ingestion_time"),
                    input_file_name().alias("source_file"),
                    col("_corrupt_record").alias("raw_record"),
                    col("user_id"), # Capture partial data if available
                    col("url")
                )

            # --- Process Valid Records ---
            # Cast received_at to timestamp
            valid_df = valid_df.withColumn("received_at", col("received_at").cast("timestamp"))
            
            # Add metadata
            valid_df = valid_df.withColumn("ingested_at", current_timestamp())
            valid_df = valid_df.withColumn("source_file", input_file_name())
            
            # Write Valid Records to Iceberg
            query_valid = (
                valid_df.writeStream
                .format("iceberg")
                .outputMode("append")
                .trigger(availableNow=True)
                .option("path", "bronze.pageviews")
                .option("checkpointLocation", "/tmp/checkpoints/bronze_pageviews")
                .start()
            )
            
            # --- Process Error Records (DLQ) ---
            # We use a separate checkpoint for the DLQ to ensure independent processing
            query_error = (
                error_df.writeStream
                .format("iceberg")
                .outputMode("append")
                .trigger(availableNow=True)
                .option("path", "bronze.pageviews_dlq")
                .option("checkpointLocation", "/tmp/checkpoints/bronze_pageviews_dlq")
                .start()
            )
            
            logger.info("Streaming queries started. Waiting for completion...")
            query_valid.awaitTermination()
            query_error.awaitTermination()
            logger.info("Streaming queries completed.")

        except Exception as e:
            logger.error(f"Error processing pageviews: {e}")

    except Exception as e:
        logger.critical(f"Critical failure in MinIO loader: {e}")
    finally:
        if created_spark and spark:
            spark.stop()

if __name__ == "__main__":
    run_minio_loader()