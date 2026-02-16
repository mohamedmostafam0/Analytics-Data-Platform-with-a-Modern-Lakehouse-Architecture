#!/usr/bin/env python3
"""
Gold Layer ETL: Creates aggregated analytical tables
Uses same pattern as bronze_to_silver for consistency
"""

import logging
import config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, when
from etl_utils import get_spark_session, write_to_iceberg

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("GoldETL")

def create_top_selling_items(spark: SparkSession):
    """Create top selling items aggregation"""
    logger.info("Creating gold.top_selling_items...")
    
    df = spark.table("silver.purchases_enriched") \
        .groupBy("item_id", "item_name", "item_category") \
        .agg(_sum("total_price").alias("total_revenue"))
    
    write_to_iceberg(df, "gold.top_selling_items", mode="overwrite")
    logger.info(f"✓ gold.top_selling_items created: {df.count()} rows")

def create_sales_performance_24h(spark: SparkSession):
    """Create sales performance aggregated by hour"""
    logger.info("Creating gold.sales_performance_24h...")
    
    df = spark.table("silver.purchases_enriched") \
        .groupBy("purchase_hour") \
        .agg(_sum("total_price").alias("total_revenue"))
    
    write_to_iceberg(df, "gold.sales_performance_24h", mode="overwrite")
    logger.info(f"✓ gold.sales_performance_24h created: {df.count()} rows")

def create_pageviews_by_channel(spark: SparkSession):
    """Create pageviews aggregated by channel"""
    logger.info("Creating gold.pageviews_by_channel...")
    
    df = spark.table("silver.pageviews_by_items") \
        .groupBy("channel") \
        .agg(count("*").alias("total_pageviews"))
    
    write_to_iceberg(df, "gold.pageviews_by_channel", mode="overwrite")
    logger.info(f"✓ gold.pageviews_by_channel created: {df.count()} rows")

def create_top_converting_items(spark: SparkSession):
    """Create top converting items with funnel metrics"""
    logger.info("Creating gold.top_converting_items...")
    
    pageviews = spark.table("silver.pageviews_by_items").alias("pvi")
    purchases = spark.table("silver.purchases_enriched").alias("pe")
    
    df = pageviews.join(
        purchases,
        (col("pvi.item_id") == col("pe.item_id")) &
        (col("pvi.user_id") == col("pe.user_id")) &
        (col("pvi.received_at").cast("date") == col("pe.purchase_date")),
        "left"
    ).groupBy(
        col("pvi.item_id"),
        col("pvi.item_name"),
        col("pvi.item_category")
    ).agg(
        count(col("pvi.user_id").isNotNull()).alias("unique_pageview_users"),
        count(col("pe.user_id").isNotNull()).alias("unique_purchase_users"),
        count(col("pe.id")).alias("total_purchases"),
        count(col("pvi.user_id")).alias("total_pageviews")
    ).withColumn(
        "conversion_rate",
        when(col("total_pageviews") == 0, 0.0)
        .otherwise(col("total_purchases").cast("double") / col("total_pageviews"))
    )
    
    write_to_iceberg(df, "gold.top_converting_items", mode="overwrite")
    logger.info(f"✓ gold.top_converting_items created: {df.count()} rows")

def run_gold_etl(spark: SparkSession):
    """Main ETL orchestration"""
    logger.info("=== Starting Gold Layer ETL ===")
    
    try:
        create_top_selling_items(spark)
        create_sales_performance_24h(spark)
        create_pageviews_by_channel(spark)
        create_top_converting_items(spark)
        logger.info("=== Gold Layer ETL Completed Successfully ===")
    except Exception as e:
        logger.error(f"Gold ETL failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    spark = get_spark_session("Gold Layer ETL")
    try:
        run_gold_etl(spark)
    finally:
        spark.stop()
