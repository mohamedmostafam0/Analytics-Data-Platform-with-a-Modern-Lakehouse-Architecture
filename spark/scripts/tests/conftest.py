import pytest
from pyspark.sql import SparkSession
import os
import sys

# Add scripts directory to sys.path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_utils import get_spark_session

@pytest.fixture(scope="session")
def spark():
    """
    Creates a SparkSession for testing.
    """
    # Ensure environment variables are set for testing
    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "password"
    os.environ["POSTGRES_USER"] = "postgresuser"
    os.environ["POSTGRES_PASSWORD"] = "postgrespw"
    os.environ["POSTGRES_DB"] = "oneshop"
    os.environ["POSTGRES_HOST"] = "postgres"
    
    # Reload Config to pick up these env vars
    from config import AppConfig
    import config
    config.Config = AppConfig.from_env()
    
    config.Config = AppConfig.from_env()
    
    spark = get_spark_session("ETL Tests")
    yield spark
    spark.stop()

@pytest.fixture(autouse=True)
def reset_config():
    """
    Resets the Config object to the environment variables before each test.
    This ensures that any monkeypatching in one test doesn't affect others.
    """
    import config
    from config import AppConfig
    # Reload from env (which is set in the session fixture)
    config.Config = AppConfig.from_env()
    yield

@pytest.fixture(scope="function")
def init_database(spark):
    """
    Initializes the database schema by running the DDL script.
    """
    # Read the SQL file
    # We assume the script is running from /home/iceberg/scripts/tests/
    # So the SQL file is at ../create_iceberg_tables.sql
    sql_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "create_iceberg_tables.sql")
    
    with open(sql_path, "r") as f:
        sql_content = f.read()
    
    # Split by semicolon and execute each command
    commands = sql_content.split(";")
    for cmd in commands:
        if cmd.strip():
            spark.sql(cmd)

@pytest.fixture(scope="function")
def clean_bronze_tables(spark, init_database):
    """
    Cleans up Bronze tables by truncating them (preserving schema) or recreating them.
    Since we have init_database, we can just drop and let init handle creation, 
    but for speed/simplicity let's use the init_database fixture to ensure they exist.
    """
    tables = ["bronze.users", "bronze.items", "bronze.purchases", "bronze.pageviews"]
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    
    # Re-run DDL to create fresh tables with correct partitioning
    # Note: This is a bit heavy for every test but correct. 
    # Calling init_database logic explicitly here for cleaner dependency chain or:
    # We can just rely on 'init_database' running first if we structure it right.
    # But let's just re-invoke the SQL execution logic to be safe.
    sql_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "create_iceberg_tables.sql")
    with open(sql_path, "r") as f:
        sql_content = f.read()
    for cmd in sql_content.split(";"):
        if cmd.strip():
            spark.sql(cmd)
            
    # Clean up checkpoints for streaming tests
    import shutil
    checkpoint_dir = "/tmp/checkpoints"
    if os.path.exists(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)
    
    yield

@pytest.fixture(scope="function")
def clean_silver_tables(spark):
    """
    Cleans up Silver tables.
    """
    tables = ["silver.users", "silver.items", "silver.purchases_enriched", "silver.pageviews_by_items"]
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    
    # Re-run DDL (it contains both Bronze and Silver, so it handles everything)
    sql_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "create_iceberg_tables.sql")
    with open(sql_path, "r") as f:
        sql_content = f.read()
    for cmd in sql_content.split(";"):
        if cmd.strip():
            spark.sql(cmd)
    
    yield
