from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql.functions import current_timestamp, input_file_name, col
from etl_utils import get_spark_session, write_to_iceberg

def run_minio_loader():
    spark = get_spark_session("ETL MinIO Loader")
    
    print("Processing Pageviews from MinIO...")
    
    # Define Explicit Schema
    # received_at is Long (epoch) in JSON, but we'll cast to Timestamp later
    schema = StructType([
        StructField("user_id", LongType(), True),
        StructField("url", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("received_at", LongType(), True)
    ])
    
    # Read from MinIO
    # s3a path should match the bucket name configured in loadgen (pageviews)
    try:
        # We read all JSON files. In a real prod setup, we'd track processed files.
        pageviews_df = spark.read.schema(schema).json("s3a://pageviews/*")
        
        # Cast received_at to timestamp (Epoch Long -> Timestamp)
        pageviews_df = pageviews_df.withColumn("received_at", col("received_at").cast("timestamp"))
        
        # Add metadata
        pageviews_df = pageviews_df.withColumn("ingested_at", current_timestamp())
        pageviews_df = pageviews_df.withColumn("source_file", input_file_name())
        
        # Write to Iceberg
        write_to_iceberg(pageviews_df, "bronze.pageviews")
        
    except Exception as e:
        # This might happen if the bucket is empty or doesn't exist yet
        print(f"Warning: Could not read from s3a://pageviews/: {e}")
    
    spark.stop()

if __name__ == "__main__":
    run_minio_loader()
