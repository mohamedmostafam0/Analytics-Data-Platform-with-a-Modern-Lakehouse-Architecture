import pytest
import sys
from pathlib import Path
from pyspark.sql import SparkSession

# Add scripts directory to sys.path so we can import our modules
SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(SCRIPTS_DIR))

import config
from config import AppConfig
from etl_utils import get_spark_session

SQL_DIR = SCRIPTS_DIR / "sql"

# Test-specific configuration overrides
TEST_CONFIG = {
    "aws_access_key": "admin",
    "aws_secret_key": "password",
    "s3_endpoint": "http://minio:9000",
    "postgres_user": "postgresuser",
    "postgres_password": "postgrespw",
    "postgres_db": "lakehouse",
    "postgres_host": "postgres",
    "postgres_port": "5432",
}


def _run_sql_files(spark):
    """Execute all schema SQL files in order."""
    for sql_file in ["bronze_schema.sql", "silver_schema.sql", "gold_schema.sql"]:
        sql_content = (SQL_DIR / sql_file).read_text()
        for cmd in sql_content.split(";"):
            if cmd.strip():
                spark.sql(cmd)


def _load_test_config():
    """Create a Config object with test-specific values."""
    config.Config = AppConfig(**TEST_CONFIG)


@pytest.fixture(scope="session")
def spark():
    """Creates a SparkSession for testing with test-specific config."""
    _load_test_config()

    spark = get_spark_session("ETL Tests")
    yield spark
    spark.stop()


@pytest.fixture(autouse=True)
def reset_config():
    """
    Resets the Config object before each test.
    Ensures monkeypatching in one test doesn't leak into others.
    """
    _load_test_config()
    yield


@pytest.fixture(scope="function")
def init_database(spark):
    """Initializes the database schema by running the DDL scripts."""
    _run_sql_files(spark)


@pytest.fixture(scope="function")
def clean_bronze_tables(spark, init_database):
    """Drops and recreates Bronze tables for a clean test state."""
    tables = ["bronze.users", "bronze.items", "bronze.purchases", "bronze.pageviews", "bronze.pageviews_dlq"]
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {table}")

    _run_sql_files(spark)
    yield


@pytest.fixture(scope="function")
def clean_silver_tables(spark):
    """Drops and recreates Silver tables for a clean test state."""
    tables = ["silver.users", "silver.items", "silver.purchases_enriched", "silver.pageviews_by_items"]
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {table}")

    _run_sql_files(spark)
    yield
