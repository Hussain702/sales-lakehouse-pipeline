# notebooks/01_bronze_ingestion.py
# ============================================================
# NOTEBOOK 01 — Bronze Layer Ingestion
# ============================================================
# Purpose : Ingest raw data from 3 source systems into the
#           Bronze Delta tables with zero transformation.
#           Only metadata/audit columns are added.
#
# Sources:
#   1. CRM System         → bronze.crm_customers
#   2. ERP System         → bronze.erp_products
#   3. E-Commerce Platform → bronze.ecom_orders
#
# Design Principles (Bronze):
#   - Store raw data AS-IS (no business logic)
#   - Append-only pattern (full history preserved)
#   - Add ingestion metadata: ingested_at, source_system, batch_id
#   - Schema-on-read (all columns kept, even dirty ones)
# ============================================================

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import functions as F
from pyspark.sql.types import *

from utils.spark_utils import get_spark, add_audit_columns, write_delta_append
from utils.logger import get_logger
from config.config import (
    SOURCE_PATH, BRONZE_PATH,
    BRONZE_CUSTOMERS, BRONZE_PRODUCTS, BRONZE_ORDERS,
    SOURCE_CRM, SOURCE_ERP, SOURCE_ECOMMERCE
)

logger = get_logger("01_bronze_ingestion")
spark  = get_spark("SalesDW_Bronze")


# ================================================================
# SOURCE 1 — CRM System: Customers
# ================================================================
def ingest_crm_customers():
    logger.info("Ingesting CRM customers → Bronze...")

    # Define explicit schema for reliability
    schema = StructType([
        StructField("customer_id",        StringType(),  True),
        StructField("first_name",         StringType(),  True),
        StructField("last_name",          StringType(),  True),
        StructField("email",              StringType(),  True),
        StructField("phone",              StringType(),  True),
        StructField("city",               StringType(),  True),
        StructField("state",              StringType(),  True),
        StructField("country",            StringType(),  True),
        StructField("zip_code",           StringType(),  True),
        StructField("customer_segment",   StringType(),  True),
        StructField("registration_date",  StringType(),  True),
    ])

    # In Databricks: replace with spark.read.format("jdbc") or cloud path
    raw_df = (
        spark.read
             .option("header", "true")
             .schema(schema)
             .csv(f"{SOURCE_PATH}/crm_customers.csv")
    )

    bronze_df = add_audit_columns(raw_df, source_system=SOURCE_CRM)

    write_delta_append(
        df         = bronze_df,
        table_name = BRONZE_CUSTOMERS,
        path       = f"{BRONZE_PATH}/crm_customers"
    )
    logger.info(f"CRM customers ingested → {BRONZE_CUSTOMERS}")


# ================================================================
# SOURCE 2 — ERP System: Products
# ================================================================
def ingest_erp_products():
    logger.info("Ingesting ERP products → Bronze...")

    schema = StructType([
        StructField("product_id",    StringType(),  True),
        StructField("product_name",  StringType(),  True),
        StructField("category",      StringType(),  True),
        StructField("sub_category",  StringType(),  True),
        StructField("brand",         StringType(),  True),
        StructField("unit_cost",     StringType(),  True),  # keep as string raw
        StructField("unit_price",    StringType(),  True),
        StructField("sku",           StringType(),  True),
        StructField("supplier_id",   StringType(),  True),
        StructField("is_active",     StringType(),  True),
        StructField("created_date",  StringType(),  True),
        StructField("weight_kg",     StringType(),  True),
    ])

    raw_df = (
        spark.read
             .option("header", "true")
             .schema(schema)
             .csv(f"{SOURCE_PATH}/erp_products.csv")
    )

    bronze_df = add_audit_columns(raw_df, source_system=SOURCE_ERP)

    write_delta_append(
        df         = bronze_df,
        table_name = BRONZE_PRODUCTS,
        path       = f"{BRONZE_PATH}/erp_products"
    )
    logger.info(f"ERP products ingested → {BRONZE_PRODUCTS}")


# ================================================================
# SOURCE 3 — E-Commerce Platform: Orders
# ================================================================
def ingest_ecom_orders():
    logger.info("Ingesting E-Commerce orders → Bronze...")

    schema = StructType([
        StructField("order_id",        StringType(), True),
        StructField("order_line_id",   StringType(), True),
        StructField("customer_id",     StringType(), True),
        StructField("product_id",      StringType(), True),
        StructField("order_date",      StringType(), True),
        StructField("ship_date",       StringType(), True),
        StructField("quantity",        StringType(), True),
        StructField("unit_price",      StringType(), True),
        StructField("discount_pct",    StringType(), True),
        StructField("payment_method",  StringType(), True),
        StructField("order_channel",   StringType(), True),
        StructField("order_status",    StringType(), True),
    ])

    raw_df = (
        spark.read
             .option("header", "true")
             .schema(schema)
             .csv(f"{SOURCE_PATH}/ecom_orders.csv")
    )

    bronze_df = add_audit_columns(raw_df, source_system=SOURCE_ECOMMERCE)

    write_delta_append(
        df         = bronze_df,
        table_name = BRONZE_ORDERS,
        path       = f"{BRONZE_PATH}/ecom_orders",
        partition_by = ["order_date"]      # partition by date for query pruning
    )
    logger.info(f"E-Commerce orders ingested → {BRONZE_ORDERS}")


# ================================================================
# MAIN — Run all ingestions
# ================================================================
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("BRONZE LAYER INGESTION — START")
    logger.info("=" * 60)

    ingest_crm_customers()
    ingest_erp_products()
    ingest_ecom_orders()

    logger.info("=" * 60)
    logger.info("BRONZE LAYER INGESTION — COMPLETE")
    logger.info("=" * 60)

    # Quick verification
    for tbl in [BRONZE_CUSTOMERS, BRONZE_PRODUCTS, BRONZE_ORDERS]:
        count = spark.read.format("delta").table(tbl).count()
        logger.info(f"   {tbl}: {count} rows")
