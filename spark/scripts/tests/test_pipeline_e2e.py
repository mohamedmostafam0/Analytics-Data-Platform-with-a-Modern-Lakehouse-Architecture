import pytest
from pyspark.sql.functions import col
import sys
import os

# Import the main functions from our ETL scripts
# We need to add the parent directory to sys.path in conftest, which we did.
from minio_loader import run_minio_loader
from postgres_loader import run_postgres_loader
from bronze_to_silver_transformer import run_bronze_to_silver

@pytest.mark.usefixtures("clean_bronze_tables", "clean_silver_tables")
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
