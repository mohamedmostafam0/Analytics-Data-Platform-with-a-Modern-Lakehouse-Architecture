import config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, upper, regexp_extract, lit, when, hour, to_date
from etl_utils import get_spark_session, write_to_iceberg
import logging
import sys

# Configure logging for this script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BronzeToSilver")

def run_bronze_to_silver(spark=None):
    created_spark = False
    try:
        if spark is None:
            spark = get_spark_session("Bronze to Silver Transformer")
            created_spark = True
        
        failures = []
        # 1. Process Users
        try:
            logger.info("Processing Users...")
            bronze_users = spark.table("bronze.users")
            email_regex = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
            
            silver_users = (
                bronze_users
                .withColumn("valid_email", col("email").rlike(email_regex))
                .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
                .select("id", "first_name", "last_name", "email", "created_at", "updated_at", "valid_email", "full_name")
            )
            write_to_iceberg(silver_users, "silver.users", mode="overwrite")
            logger.info("Successfully processed Users.")
        except Exception as e:
            logger.error(f"Failed to process Users: {e}")
            failures.append(("Users", str(e)))

        # 2. Process Items
        try:
            logger.info("Processing Items...")
            bronze_items = spark.table("bronze.items")
            silver_items = (
                bronze_items
                .withColumn("price", when(col("price") < 0, lit(0)).otherwise(col("price")))
                .withColumn("category", upper(col("category")))
                .select("id", "name", "category", "price", "inventory", "created_at", "updated_at")
            )
            write_to_iceberg(silver_items, "silver.items", mode="overwrite")
            logger.info("Successfully processed Items.")
        except Exception as e:
            logger.error(f"Failed to process Items: {e}")
            failures.append(("Items", str(e)))

        # 3. Process Purchases Enriched
        try:
            logger.info("Processing Purchases Enriched...")
            bronze_purchases = spark.table("bronze.purchases")
            # Re-read tables if needed, or reuse DFs if caching strategies were used (omitted for simplicity)
            bronze_users = spark.table("bronze.users") 
            bronze_items = spark.table("bronze.items")

            silver_purchases = (
                bronze_purchases
                .join(bronze_users, bronze_purchases.user_id == bronze_users.id, "left")
                .join(bronze_items, bronze_purchases.item_id == bronze_items.id, "left")
                .select(
                    bronze_purchases.id,
                    bronze_purchases.user_id,
                    bronze_purchases.item_id,
                    bronze_purchases.quantity,
                    bronze_purchases.purchase_price,
                    (col("quantity") * col("purchase_price")).alias("total_price"),
                    bronze_users.email.alias("user_email"),
                    bronze_items.name.alias("item_name"),
                    bronze_items.category.alias("item_category"),
                    to_date(bronze_purchases.created_at).alias("purchase_date"),
                    hour(bronze_purchases.created_at).alias("purchase_hour"),
                    bronze_purchases.created_at,
                    bronze_purchases.updated_at
                )
            )
            write_to_iceberg(silver_purchases, "silver.purchases_enriched", mode="overwrite")
            logger.info("Successfully processed Purchases Enriched.")
        except Exception as e:
            logger.error(f"Failed to process Purchases: {e}")
            failures.append(("Purchases", str(e)))

        # 4. Process Pageviews by Items
        try:
            logger.info("Processing Pageviews by Items...")
            bronze_pageviews = spark.table("bronze.pageviews")
            bronze_items = spark.table("bronze.items")

            pageviews_with_item = bronze_pageviews.withColumn(
                "page", regexp_extract(col("url"), r"^/([^/]+)/\d+$", 1)
            ).withColumn(
                "item_id", regexp_extract(col("url"), r"/(\d+)$", 1).cast("bigint")
            ).filter(col("item_id").isNotNull())

            silver_pageviews_by_items = (
                pageviews_with_item
                .join(bronze_items, pageviews_with_item.item_id == bronze_items.id, "left")
                .select(
                    pageviews_with_item.user_id,
                    pageviews_with_item.item_id,
                    pageviews_with_item.page,
                    bronze_items.name.alias("item_name"),
                    bronze_items.category.alias("item_category"),
                    pageviews_with_item.channel,
                    pageviews_with_item.received_at
                )
            )
            write_to_iceberg(silver_pageviews_by_items, "silver.pageviews_by_items", mode="overwrite")
            logger.info("Successfully processed Pageviews by Items.")
        except Exception as e:
            logger.error(f"Failed to process Pageviews: {e}")
            failures.append(("Pageviews", str(e)))

        if failures:
            failed_steps = ", ".join(s for s, _ in failures)
            raise RuntimeError(f"Bronze-to-Silver failed for: {failed_steps}")

    except Exception as e:
        logger.critical(f"Critical failure in Bronze to Silver job: {e}")
        raise
    finally:
        if created_spark and spark:
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    run_bronze_to_silver()