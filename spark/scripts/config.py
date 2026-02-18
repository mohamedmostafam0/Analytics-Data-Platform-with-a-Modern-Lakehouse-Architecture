import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class AppConfig:
    # AWS / MinIO
    aws_access_key: str
    aws_secret_key: str
    s3_endpoint: str = "http://minio:9000"
    
    # Postgres
    postgres_user: Optional[str] = None
    postgres_password: Optional[str] = None
    postgres_db: str = "oneshop"
    postgres_host: str = "postgres"
    postgres_port: str = "5432"
    
    # Iceberg
    warehouse_path: str = "s3a://warehouse/"
    
    @classmethod
    def from_env(cls) -> 'AppConfig':
        return cls(
            aws_access_key=os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("MINIO_ROOT_USER"),
            aws_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("MINIO_ROOT_PASSWORD"),
            s3_endpoint=os.getenv("CATALOG_S3_ENDPOINT") or os.getenv("S3_ENDPOINT", "http://minio:9000"),
            postgres_user=os.getenv("POSTGRES_USER"),
            postgres_password=os.getenv("POSTGRES_PASSWORD"),
            postgres_db=os.getenv("POSTGRES_DB", "oneshop"),
            postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
            postgres_port=os.getenv("POSTGRES_PORT", "5432"),
            warehouse_path=os.getenv("CATALOG_WAREHOUSE", "s3a://warehouse/"),
        )

    def validate(self):
        """
        Validates critical configuration.
        """
        if not self.aws_access_key or not self.aws_secret_key:
             raise EnvironmentError("Missing required AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY).")
        
# Global instance for easy import
Config = AppConfig.from_env()
