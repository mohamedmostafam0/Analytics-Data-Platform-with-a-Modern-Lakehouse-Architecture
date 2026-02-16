import pytest
from pyspark.sql.functions import col, current_timestamp
import sys
import os
import logging

from config import Config

# Import the main functions from our ETL scripts
# We need to add the parent directory to sys.path in conftest, which we did.
from minio_loader import run_minio_loader
from postgres_loader import run_postgres_loader
from bronze_to_silver_transformer import run_bronze_to_silver

@pytest.fixture(scope="function")
def seed_data(spark):
    """
    Seeds MinIO and Postgres with test data.
    """
    logger = logging.getLogger("TestDataSeeder")
    logger.info("Seeding test data...")
    
    # 1. Seed MinIO (Pageviews)
    
    # Bucket creation is handled by docker-compose (mc service)

    pageviews_data = [
        (1, "http://example.com/home", "organic", 1700000000),
        (2, "http://example.com/product", "ads", 1700000005),
    ]
    # Use full S3A path which includes the bucket
    spark.createDataFrame(pageviews_data, ["user_id", "url", "channel", "received_at"]) \
        .write.mode("overwrite").json("s3a://pageviews/test_batch_1.json")
        
    # 2. Seed Postgres (Users, Items, Purchases)
    
    jdbc_url = f"jdbc:postgresql://{Config.postgres_host}:{Config.postgres_port}/{Config.postgres_db}"
    jdbc_props = {
        "user": Config.postgres_user,
        "password": Config.postgres_password,
        "driver": "org.postgresql.Driver"
    }
    
    # Purchases (Write FIRST to drop the table and its FK constraints logic from Postgres side if relying on overwrite)
    # Note: Spark's overwrite drops the table and recreates it without FK constraints usually.
    # By writing purchases first, we remove the FK dependency on users/items, allowing them to be overwritten.
    purchases_data = [
        (1001, 1, 101, 2, 10.00, "2023-01-05 10:00:00"), 
        (1002, 2, 102, 1, 20.00, "2023-01-05 11:00:00")
    ]
    spark.createDataFrame(purchases_data, ["id", "user_id", "item_id", "quantity", "purchase_price", "created_at"]) \
        .withColumn("created_at", col("created_at").cast("timestamp")) \
        .withColumn("updated_at", col("created_at")) \
        .write.jdbc(jdbc_url, "purchases", mode="overwrite", properties=jdbc_props)

    # Users
    users_data = [
        (1, "John", "Doe", "user1@example.com", "2023-01-01"), 
        (2, "Jane", "Smith", "user2@example.com", "2023-01-02")
    ]
    spark.createDataFrame(users_data, ["id", "first_name", "last_name", "email", "created_at"]) \
        .withColumn("created_at", col("created_at").cast("timestamp")) \
        .withColumn("updated_at", col("created_at")) \
        .write.jdbc(jdbc_url, "users", mode="overwrite", properties=jdbc_props)
        
    # Items
    items_data = [
        (101, "Widget A", "Widgets", 10.00, 100), 
        (102, "Widget B", "Widgets", 20.00, 50)
    ]
    spark.createDataFrame(items_data, ["id", "name", "category", "price", "inventory"]) \
        .withColumn("created_at", current_timestamp()) \
        .withColumn("updated_at", current_timestamp()) \
        .write.jdbc(jdbc_url, "items", mode="overwrite", properties=jdbc_props)

    logger.info("Seeding complete.")

@pytest.mark.usefixtures("clean_bronze_tables", "clean_silver_tables", "seed_data")
def test_end_to_end_pipeline(spark):
    """
    Runs the full ETL pipeline:
    1.  Load Bronze (MinIO + Postgres)
    2.  Transform Bronze -> Silver
    3.  Verify Data in Silver Tables
    """
    
    # Configure logger for tests
    import logging
    logger = logging.getLogger("E2ETest")
    logging.basicConfig(level=logging.INFO)

    # 1. Run Bronze Loaders
    logger.info("--- Running MinIO Loader ---")
    run_minio_loader(spark)
    
    logger.info("--- Running Postgres Loader ---")
    run_postgres_loader(spark)
    
    # Verify Bronze Data
    bronze_pageviews_count = spark.table("bronze.pageviews").count()
    bronze_users_count = spark.table("bronze.users").count()
    
    logger.info(f"Bronze Pageviews Count: {bronze_pageviews_count}")
    logger.info(f"Bronze Users Count: {bronze_users_count}")
    
    assert bronze_pageviews_count > 0, "Bronze pageviews should not be empty"
    assert bronze_users_count > 0, "Bronze users should not be empty"
    
    # 2. Run Silver Transformation
    logger.info("--- Running Bronze to Silver Transformer ---")
    run_bronze_to_silver(spark)
    
    # 3. Verify Silver Data
    silver_users = spark.table("silver.users")
    silver_purchases = spark.table("silver.purchases_enriched")
    
    silver_users_count = silver_users.count()
    silver_purchases_count = silver_purchases.count()
    
    logger.info(f"Silver Users Count: {silver_users_count}")
    logger.info(f"Silver Purchases Count: {silver_purchases_count}")
    
    assert silver_users_count > 0, "Silver users should not be empty"
    assert silver_purchases_count > 0, "Silver purchases should not be empty"
    
    # Check for specific columns
    assert "valid_email" in silver_users.columns
    assert "purchase_date" in silver_purchases.columns
    assert "purchase_hour" in silver_purchases.columns
    
    # Verify Data Integrity (Sample Check)
    # Check that TOTAL_PRICE is correctly calculated
    sample_purchase = silver_purchases.limit(1).collect()[0]
    expected_total = sample_purchase['quantity'] * sample_purchase['purchase_price']
    
    # Use approximate equality for floating point/decimal
    assert abs(sample_purchase['total_price'] - expected_total) < 0.01, \
        f"Total price calculation error: {sample_purchase['total_price']} != {expected_total}"

    logger.info("--- E2E Test Completed Successfully ---")
