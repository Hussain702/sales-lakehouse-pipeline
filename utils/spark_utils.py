# utils/spark_utils.py
# ============================================================
# Spark Session Helpers & Common Utilities
# ============================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
import sys
import os

# Allow running locally (outside Databricks) for testing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.config import SPARK_CONFIG, BATCH_ID
from utils.logger import get_logger

logger = get_logger(__name__)


# ------------------------------------------------------------------
# Spark Session Factory
# ------------------------------------------------------------------
def get_spark(app_name: str = "SalesDW") -> SparkSession:
   
    builder = SparkSession.builder.appName(app_name)
    for key, val in SPARK_CONFIG.items():
        builder = builder.config(key, val)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession active — app: {app_name}")
    return spark


# ------------------------------------------------------------------
# Delta Table Helpers
# ------------------------------------------------------------------
def table_exists(spark: SparkSession, full_table_name: str) -> bool:
   
    db, tbl = full_table_name.split(".")
    return spark.catalog.tableExists(tbl, db)


def read_delta(spark: SparkSession, table_name: str) -> DataFrame:
   
    logger.info(f"Reading Delta table: {table_name}")
    return spark.read.format("delta").table(table_name)


def write_delta_overwrite(df: DataFrame, table_name: str, path: str,
                          partition_by: list = None) -> None:
    
    logger.info(f"Writing (overwrite) → {table_name}")
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.option("path", path).saveAsTable(table_name)
    logger.info(f"Write complete → {table_name} | rows: {df.count()}")


def write_delta_append(df: DataFrame, table_name: str, path: str,
                       partition_by: list = None) -> None:
    """Append DataFrame to Delta table."""
    logger.info(f"Writing (append) → {table_name}")
    writer = df.write.format("delta").mode("append")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.option("path", path).saveAsTable(table_name)
    logger.info(f"Append complete → {table_name} | rows: {df.count()}")


# ------------------------------------------------------------------
# Audit Column Helpers
# ------------------------------------------------------------------
def add_audit_columns(df: DataFrame, source_system: str,
                      batch_id: str = BATCH_ID) -> DataFrame:
   
    
    return (
        df
        .withColumn("ingested_at",   F.current_timestamp())
        .withColumn("source_system", F.lit(source_system))
        .withColumn("batch_id",      F.lit(batch_id))
    )


def add_silver_audit_columns(df: DataFrame) -> DataFrame:
  
    return (
        df
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )


# ------------------------------------------------------------------
# Schema Validation
# ------------------------------------------------------------------
def validate_schema(df: DataFrame, expected_schema: StructType) -> bool:

    df_cols     = set(df.columns)
    expected_cols = set([f.name for f in expected_schema.fields])
    missing = expected_cols - df_cols
    if missing:
        raise ValueError(f"Schema validation failed. Missing columns: {missing}")
    logger.info("Schema validation passed.")
    return True


# ------------------------------------------------------------------
# Optimize Delta Table
# ------------------------------------------------------------------
def optimize_table(spark: SparkSession, table_name: str,
                   z_order_cols: list = None) -> None:
    """Run OPTIMIZE (and optional Z-ORDER) on a Delta table."""
    sql = f"OPTIMIZE {table_name}"
    if z_order_cols:
        cols = ", ".join(z_order_cols)
        sql += f" ZORDER BY ({cols})"
    logger.info(f"Optimizing table: {sql}")
    spark.sql(sql)
    logger.info(f"Optimization complete → {table_name}")
