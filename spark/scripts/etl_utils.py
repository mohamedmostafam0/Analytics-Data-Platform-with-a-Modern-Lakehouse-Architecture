import os
import sys
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ETL_Utils")

import config
import time
import functools

def retry_with_backoff(retries=3, backoff_in_seconds=1):
    """
    Decorator for retrying a function with exponential backoff.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            x = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if x == retries:
                        logger.error(f"Failed after {retries} retries: {e}")
                        raise e
                    else:
                        sleep = (backoff_in_seconds * 2 ** x)
                        logger.warning(f"Error: {e}. Retrying in {sleep} seconds...")
                        time.sleep(sleep)
                        x += 1
        return wrapper
    return decorator

def get_spark_session(app_name):
    """
    Creates and returns a SparkSession with Iceberg and S3A configurations.
    Enforces that AWS credentials are present in the environment via Config.authenticate().
    """
    # Validate credentials
    config.Config.validate()
    
    access_key = config.Config.aws_access_key
    secret_key = config.Config.aws_secret_key
    s3_endpoint = config.Config.s3_endpoint

    logger.info(f"Initializing SparkSession: {app_name}")

    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
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
        mode (str): Write mode. Supports 'append' and 'overwrite'.
    """
    logger.info(f"Writing to {table_name} with mode='{mode}'...")
    
    # Ensure database exists (simple check/creation)
    db_name = table_name.split(".")[0]
    spark = df.sparkSession
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

    try:
        # Use simple count for logging (note: this triggers an action)
        record_count = df.count()
        logger.info(f"Preparing to write {record_count} records to {table_name}")

        if mode == "overwrite":
            df.writeTo(table_name).overwritePartitions()
        else:
            df.writeTo(table_name).append()
            
        logger.info(f"Successfully wrote records to {table_name}")
    except Exception as e:
        logger.error(f"Error writing to {table_name}: {e}")
        raise e
