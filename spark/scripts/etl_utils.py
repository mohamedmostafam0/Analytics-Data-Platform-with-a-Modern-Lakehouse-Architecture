import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def get_spark_session(app_name):
    """
    Creates and returns a SparkSession with Iceberg and S3A configurations.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "admin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "password")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def write_to_iceberg(df, table_name, mode="append"):
    """
    Writes a DataFrame to an Iceberg table.
    
    Args:
        df (DataFrame): The DataFrame to write.
        table_name (str): The target table name (e.g., 'bronze.users').
        mode (str): Write mode. Default is 'append'.
    """
    print(f"Writing to {table_name} with mode='{mode}'...")
    
    # Ensure database exists (simple check/creation)
    db_name = table_name.split(".")[0]
    spark = df.sparkSession
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

    try:
        # Using DataFrameWriterV2 API for Iceberg is often preferred for more control,
        # but standard write with format("iceberg") works too.
        # Here we use the highly compatible writeTo API if available or standard write.
        # For simplicity and broad compatibility in this setup:
        df.writeTo(table_name).append()
        print(f"Successfully wrote {df.count()} records to {table_name}")
    except Exception as e:
        print(f"Error writing to {table_name}: {e}")
        raise e
