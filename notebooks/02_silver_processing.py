# notebooks/02_silver_processing.py
# ============================================================
# NOTEBOOK 02 — Silver Layer Processing
# ============================================================
# Purpose : Read from Bronze, apply cleansing, standardisation,
#           deduplication and type casting, then write to Silver.
#
# Transformations per table:
#   silver.customers  ← bronze.crm_customers
#   silver.products   ← bronze.erp_products
#   silver.orders     ← bronze.ecom_orders
#
# Design Principles (Silver):
#   - Enforce correct data types
#   - Standardise string casing and formats
#   - Remove duplicates (keep latest by ingested_at)
#   - Reject / quarantine invalid records (null PKs, negative prices)
#   - Preserve all audit columns + add created_at / updated_at
# ============================================================

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import Window

from utils.spark_utils import (
    get_spark, add_silver_audit_columns,
    write_delta_overwrite, read_delta
)
from utils.dq_utils import run_standard_checks
from utils.logger import get_logger
from config.config import (
    BRONZE_CUSTOMERS, BRONZE_PRODUCTS, BRONZE_ORDERS,
    SILVER_CUSTOMERS, SILVER_PRODUCTS, SILVER_ORDERS,
    SILVER_PATH
)

logger = get_logger("02_silver_processing")
spark  = get_spark("SalesDW_Silver")


# ================================================================
# SILVER — Customers
# ================================================================
def process_customers():
    logger.info("Processing Bronze → Silver customers...")

    bronze = read_delta(spark, BRONZE_CUSTOMERS)

    # --- Deduplication: keep latest record per customer_id ---
    window = Window.partitionBy("customer_id").orderBy(F.desc("ingested_at"))
    deduped = (
        bronze
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # --- Filter invalid: drop rows with null customer_id or email ---
    valid = deduped.filter(
        F.col("customer_id").isNotNull() &
        F.col("email").isNotNull()
    )

    # --- Type casting & standardisation ---
    silver = (
        valid
        .withColumn("customer_id",       F.trim(F.upper(F.col("customer_id"))))
        .withColumn("first_name",         F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name",          F.initcap(F.trim(F.col("last_name"))))
        .withColumn("full_name",          F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
        .withColumn("email",              F.lower(F.trim(F.col("email"))))
        .withColumn("phone",              F.regexp_replace(F.col("phone"), r"[^\d\+\-]", ""))
        .withColumn("city",               F.initcap(F.trim(F.col("city"))))
        .withColumn("state",              F.upper(F.trim(F.col("state"))))
        .withColumn("country",            F.upper(F.trim(F.col("country"))))
        .withColumn("zip_code",           F.trim(F.col("zip_code")))
        .withColumn("customer_segment",   F.upper(F.trim(F.col("customer_segment"))))
        .withColumn("registration_date",  F.to_date(F.col("registration_date"), "yyyy-MM-dd"))
        .withColumn("created_at",         F.current_timestamp())
        .withColumn("updated_at",         F.current_timestamp())
    )

    # Select final columns (explicit column ordering)
    final_cols = [
        "customer_id", "first_name", "last_name", "full_name",
        "email", "phone", "city", "state", "country", "zip_code",
        "customer_segment", "registration_date",
        "source_system", "batch_id", "ingested_at", "created_at", "updated_at"
    ]
    silver = silver.select(*final_cols)

    # --- Data Quality Check ---
    run_standard_checks(silver, SILVER_CUSTOMERS,
                        not_null_cols=["customer_id", "email", "full_name"],
                        dedup_key_cols=["customer_id"])

    write_delta_overwrite(silver, SILVER_CUSTOMERS,
                          f"{SILVER_PATH}/customers")
    logger.info(f"✅ Silver customers written → {SILVER_CUSTOMERS}")


# ================================================================
# SILVER — Products
# ================================================================
def process_products():
    logger.info("Processing Bronze → Silver products...")

    bronze = read_delta(spark, BRONZE_PRODUCTS)

    window = Window.partitionBy("product_id").orderBy(F.desc("ingested_at"))
    deduped = (
        bronze
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    valid = deduped.filter(
        F.col("product_id").isNotNull() &
        F.col("product_name").isNotNull()
    )

    silver = (
        valid
        .withColumn("product_id",    F.trim(F.upper(F.col("product_id"))))
        .withColumn("product_name",  F.initcap(F.trim(F.col("product_name"))))
        .withColumn("category",      F.initcap(F.trim(F.col("category"))))
        .withColumn("sub_category",  F.initcap(F.trim(F.col("sub_category"))))
        .withColumn("brand",         F.trim(F.col("brand")))
        .withColumn("sku",           F.upper(F.trim(F.col("sku"))))
        .withColumn("supplier_id",   F.upper(F.trim(F.col("supplier_id"))))
        # Type casting
        .withColumn("unit_cost",     F.col("unit_cost").cast(DecimalType(10, 2)))
        .withColumn("unit_price",    F.col("unit_price").cast(DecimalType(10, 2)))
        .withColumn("weight_kg",     F.col("weight_kg").cast(DecimalType(8, 3)))
        .withColumn("is_active",     F.col("is_active").cast(BooleanType()))
        .withColumn("created_date",  F.to_date(F.col("created_date"), "yyyy-MM-dd"))
        # Derived column
        .withColumn("gross_margin_pct",
                    F.round((F.col("unit_price") - F.col("unit_cost")) / F.col("unit_price") * 100, 2))
        .withColumn("created_at",    F.current_timestamp())
        .withColumn("updated_at",    F.current_timestamp())
    )

    # Filter out bad pricing rows
    silver = silver.filter(
        (F.col("unit_cost") > 0) &
        (F.col("unit_price") > 0) &
        (F.col("unit_price") >= F.col("unit_cost"))
    )

    final_cols = [
        "product_id", "product_name", "category", "sub_category",
        "brand", "sku", "supplier_id", "unit_cost", "unit_price",
        "gross_margin_pct", "weight_kg", "is_active", "created_date",
        "source_system", "batch_id", "ingested_at", "created_at", "updated_at"
    ]
    silver = silver.select(*final_cols)

    run_standard_checks(silver, SILVER_PRODUCTS,
                        not_null_cols=["product_id", "product_name", "unit_price"],
                        dedup_key_cols=["product_id"])

    write_delta_overwrite(silver, SILVER_PRODUCTS,
                          f"{SILVER_PATH}/products")
    logger.info(f"✅ Silver products written → {SILVER_PRODUCTS}")


# ================================================================
# SILVER — Orders
# ================================================================
def process_orders():
    logger.info("Processing Bronze → Silver orders...")

    bronze = read_delta(spark, BRONZE_ORDERS)

    # Dedup on composite key: order_line_id
    window = Window.partitionBy("order_line_id").orderBy(F.desc("ingested_at"))
    deduped = (
        bronze
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    valid = deduped.filter(
        F.col("order_id").isNotNull() &
        F.col("order_line_id").isNotNull() &
        F.col("customer_id").isNotNull() &
        F.col("product_id").isNotNull()
    )

    silver = (
        valid
        .withColumn("order_id",       F.upper(F.trim(F.col("order_id"))))
        .withColumn("order_line_id",  F.upper(F.trim(F.col("order_line_id"))))
        .withColumn("customer_id",    F.upper(F.trim(F.col("customer_id"))))
        .withColumn("product_id",     F.upper(F.trim(F.col("product_id"))))
        .withColumn("order_date",     F.to_date(F.col("order_date"),  "yyyy-MM-dd"))
        .withColumn("ship_date",      F.to_date(F.col("ship_date"),   "yyyy-MM-dd"))
        .withColumn("quantity",       F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price",     F.col("unit_price").cast(DecimalType(10, 2)))
        .withColumn("discount_pct",   F.col("discount_pct").cast(DecimalType(5, 4)))
        .withColumn("payment_method", F.initcap(F.trim(F.col("payment_method"))))
        .withColumn("order_channel",  F.initcap(F.trim(F.col("order_channel"))))
        .withColumn("order_status",   F.initcap(F.trim(F.col("order_status"))))
        # Derived metrics
        .withColumn("discount_amount",
                    F.round(F.col("unit_price") * F.col("quantity") * F.col("discount_pct"), 2))
        .withColumn("gross_revenue",
                    F.round(F.col("unit_price") * F.col("quantity"), 2))
        .withColumn("net_revenue",
                    F.round(F.col("gross_revenue") - F.col("discount_amount"), 2))
        .withColumn("days_to_ship",
                    F.datediff(F.col("ship_date"), F.col("order_date")))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    # Filter invalid quantities / prices
    silver = silver.filter(
        (F.col("quantity") > 0) &
        (F.col("unit_price") > 0)
    )

    final_cols = [
        "order_id", "order_line_id", "customer_id", "product_id",
        "order_date", "ship_date", "quantity", "unit_price",
        "discount_pct", "discount_amount", "gross_revenue", "net_revenue",
        "days_to_ship", "payment_method", "order_channel", "order_status",
        "source_system", "batch_id", "ingested_at", "created_at", "updated_at"
    ]
    silver = silver.select(*final_cols)

    run_standard_checks(silver, SILVER_ORDERS,
                        not_null_cols=["order_id", "customer_id", "product_id", "order_date"],
                        dedup_key_cols=["order_line_id"])

    write_delta_overwrite(
        silver, SILVER_ORDERS,
        f"{SILVER_PATH}/orders",
        partition_by=["order_date"]
    )
    logger.info(f"✅ Silver orders written → {SILVER_ORDERS}")


# ================================================================
# MAIN
# ================================================================
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("SILVER LAYER PROCESSING — START")
    logger.info("=" * 60)

    process_customers()
    process_products()
    process_orders()

    logger.info("=" * 60)
    logger.info("✅ SILVER LAYER PROCESSING — COMPLETE")
    logger.info("=" * 60)

    for tbl in [SILVER_CUSTOMERS, SILVER_PRODUCTS, SILVER_ORDERS]:
        count = spark.read.format("delta").table(tbl).count()
        logger.info(f"   {tbl}: {count} rows")
