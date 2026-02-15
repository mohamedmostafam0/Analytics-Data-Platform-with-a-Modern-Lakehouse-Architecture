import pytest
import os
from etl_utils import get_spark_session

def test_get_spark_session_config(spark):
    """
    Verifies that the Spark session has the correct S3A configurations.
    """
    conf = spark.conf
    assert conf.get("spark.hadoop.fs.s3a.endpoint") == "http://minio:9000"
    assert conf.get("spark.hadoop.fs.s3a.path.style.access") == "true"
    assert conf.get("spark.hadoop.fs.s3a.impl") == "org.apache.hadoop.fs.s3a.S3AFileSystem"

def test_get_spark_session_missing_credentials(monkeypatch):
    """
    Verifies that get_spark_session raises an EnvironmentError if AWS credentials are missing.
    """
    # Import the module where Config is instantiated
    import config
    
    # Create a dummy config with missing credentials
    from config import AppConfig
    bad_config = AppConfig(aws_access_key="", aws_secret_key="")
    
    # Patch the Config object in the config module
    monkeypatch.setattr(config, "Config", bad_config)
    
    with pytest.raises(EnvironmentError, match="Missing required AWS credentials"):
        get_spark_session("Fail Test")
