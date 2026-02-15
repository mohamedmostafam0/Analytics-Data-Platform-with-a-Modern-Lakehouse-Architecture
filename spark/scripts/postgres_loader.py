import os
import sys
from pyspark.sql.functions import current_timestamp, col
from etl_utils import get_spark_session, write_to_iceberg

def get_jdbc_options(table_name):
    return {
        "url": f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'oneshop')}",
        "dbtable": table_name,
        "user": os.getenv("POSTGRES_USER", "postgresuser"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgrespw"),
        "driver": "org.postgresql.Driver"
    }

def run_postgres_loader():
    spark = get_spark_session("ETL Postgres Loader")
    
    tables_to_load = ["users", "items", "purchases"]
    
    for table in tables_to_load:
        print(f"Processing {table} from Postgres...")
        try:
            # Read from Postgres
            df = spark.read.format("jdbc").options(**get_jdbc_options(table)).load()
            
            # Add metadata
            df = df.withColumn("ingested_at", current_timestamp())
            
            # Write to Iceberg
            write_to_iceberg(df, f"bronze.{table}")
            
        except Exception as e:
            print(f"Error processing table {table}: {e}")

    spark.stop()

if __name__ == "__main__":
    run_postgres_loader()
