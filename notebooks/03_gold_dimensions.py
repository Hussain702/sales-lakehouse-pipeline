# notebooks/03_gold_dimensions.py
# ============================================================
# NOTEBOOK 03 — Gold Layer: Dimension Tables (SCD Type 1)
# ============================================================
# Purpose : Build and maintain Star Schema dimension tables in
#           the Gold layer using SCD Type 1 MERGE logic.
#
# Tables:
#   gold.dim_customer   ← silver.customers   (SCD Type 1)
#   gold.dim_product    ← silver.products    (SCD Type 1)
#   gold.dim_date       ← generated          (static)
#   gold.dim_geography  ← silver.customers   (SCD Type 1)
#
# SCD Type 1 Behaviour:
#   MATCHED     → overwrite changed attributes (no history kept)
#   NOT MATCHED → insert new row with surrogate key
# ============================================================

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

from utils.spark_utils import (
    get_spark, read_delta, write_delta_overwrite,
    table_exists, optimize_table
)
from utils.scd_utils import scd1_merge, generate_surrogate_key
from utils.logger import get_logger
from config.config import (
    SILVER_CUSTOMERS, SILVER_PRODUCTS,
    DIM_CUSTOMER, DIM_PRODUCT, DIM_DATE, DIM_GEOGRAPHY,
    GOLD_PATH, DIM_DATE_START, DIM_DATE_END
)

logger = get_logger("03_gold_dimensions")
spark  = get_spark("SalesDW_GoldDimensions")


# ================================================================
# DIMENSION: dim_customer  (SCD Type 1)
# ================================================================
def build_dim_customer():
    logger.info("Building dim_customer (SCD Type 1)...")

    silver = read_delta(spark, SILVER_CUSTOMERS)

    staging = (
        silver
        .select(
            F.col("customer_id").alias("customer_bk"),   # business key
            "first_name", "last_name", "full_name", "email",
            "phone", "city", "state", "country", "zip_code",
            "customer_segment", "registration_date"
        )
    )

    # Generate stable surrogate key
    staging = generate_surrogate_key(staging, "customer_bk", "customer_sk")

    # Add audit columns
    staging = (
        staging
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    update_cols = [
        "first_name", "last_name", "full_name", "email", "phone",
        "city", "state", "country", "zip_code",
        "customer_segment", "registration_date"
    ]

    if table_exists(spark, DIM_CUSTOMER):
        # SCD Type 1: MERGE (upsert)
        scd1_merge(
            spark         = spark,
            source_df     = staging,
            target_table  = DIM_CUSTOMER,
            merge_key     = "customer_bk",
            update_columns= update_cols
        )
    else:
        # First load: create the table
        write_delta_overwrite(
            staging, DIM_CUSTOMER,
            f"{GOLD_PATH}/dim_customer"
        )

    optimize_table(spark, DIM_CUSTOMER, z_order_cols=["customer_bk"])
    logger.info(f"✅ dim_customer complete → {DIM_CUSTOMER}")


# ================================================================
# DIMENSION: dim_product  (SCD Type 1)
# ================================================================
def build_dim_product():
    logger.info("Building dim_product (SCD Type 1)...")

    silver = read_delta(spark, SILVER_PRODUCTS)

    staging = (
        silver
        .select(
            F.col("product_id").alias("product_bk"),
            "product_name", "category", "sub_category",
            "brand", "sku", "supplier_id",
            "unit_cost", "unit_price", "gross_margin_pct",
            "weight_kg", "is_active", "created_date"
        )
    )

    staging = generate_surrogate_key(staging, "product_bk", "product_sk")

    staging = (
        staging
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    update_cols = [
        "product_name", "category", "sub_category", "brand",
        "sku", "supplier_id", "unit_cost", "unit_price",
        "gross_margin_pct", "weight_kg", "is_active"
    ]

    if table_exists(spark, DIM_PRODUCT):
        scd1_merge(
            spark         = spark,
            source_df     = staging,
            target_table  = DIM_PRODUCT,
            merge_key     = "product_bk",
            update_columns= update_cols
        )
    else:
        write_delta_overwrite(
            staging, DIM_PRODUCT,
            f"{GOLD_PATH}/dim_product"
        )

    optimize_table(spark, DIM_PRODUCT, z_order_cols=["product_bk", "category"])
    logger.info(f"✅ dim_product complete → {DIM_PRODUCT}")


# ================================================================
# DIMENSION: dim_date  (Static, pre-generated)
# ================================================================
def build_dim_date():
    logger.info("Building dim_date...")

    # Generate date range as a DataFrame
    date_df = spark.sql(f"""
        SELECT sequence(
            to_date('{DIM_DATE_START}'),
            to_date('{DIM_DATE_END}'),
            interval 1 day
        ) AS date_array
    """).withColumn("full_date", F.explode(F.col("date_array"))).select("full_date")

    dim_date = (
        date_df
        .withColumn("date_key",         F.date_format(F.col("full_date"), "yyyyMMdd").cast(IntegerType()))
        .withColumn("year",              F.year(F.col("full_date")))
        .withColumn("quarter",           F.quarter(F.col("full_date")))
        .withColumn("quarter_name",      F.concat(F.lit("Q"), F.quarter(F.col("full_date"))))
        .withColumn("month",             F.month(F.col("full_date")))
        .withColumn("month_name",        F.date_format(F.col("full_date"), "MMMM"))
        .withColumn("month_abbrev",      F.date_format(F.col("full_date"), "MMM"))
        .withColumn("week_of_year",      F.weekofyear(F.col("full_date")))
        .withColumn("day_of_month",      F.dayofmonth(F.col("full_date")))
        .withColumn("day_of_week",       F.dayofweek(F.col("full_date")))
        .withColumn("day_name",          F.date_format(F.col("full_date"), "EEEE"))
        .withColumn("day_abbrev",        F.date_format(F.col("full_date"), "EEE"))
        .withColumn("is_weekend",        (F.dayofweek(F.col("full_date")).isin(1, 7)).cast(BooleanType()))
        .withColumn("is_weekday",        (~F.dayofweek(F.col("full_date")).isin(1, 7)).cast(BooleanType()))
        .withColumn("year_month",        F.date_format(F.col("full_date"), "yyyy-MM"))
        .withColumn("year_quarter",
                    F.concat(F.year(F.col("full_date")), F.lit("-Q"), F.quarter(F.col("full_date"))))
        .withColumn("fiscal_year",
                    F.when(F.month(F.col("full_date")) >= 7,
                           F.year(F.col("full_date")) + 1)
                     .otherwise(F.year(F.col("full_date"))))
        .withColumn("fiscal_quarter",
                    F.when(F.month(F.col("full_date")).isin(7, 8, 9), F.lit("FQ1"))
                     .when(F.month(F.col("full_date")).isin(10, 11, 12), F.lit("FQ2"))
                     .when(F.month(F.col("full_date")).isin(1, 2, 3), F.lit("FQ3"))
                     .otherwise(F.lit("FQ4")))
    )

    final_cols = [
        "date_key", "full_date", "year", "quarter", "quarter_name",
        "month", "month_name", "month_abbrev", "week_of_year",
        "day_of_month", "day_of_week", "day_name", "day_abbrev",
        "is_weekend", "is_weekday", "year_month", "year_quarter",
        "fiscal_year", "fiscal_quarter"
    ]

    write_delta_overwrite(
        dim_date.select(*final_cols),
        DIM_DATE,
        f"{GOLD_PATH}/dim_date"
    )
    optimize_table(spark, DIM_DATE, z_order_cols=["date_key"])
    logger.info(f"✅ dim_date complete → {DIM_DATE}")


# ================================================================
# DIMENSION: dim_geography  (SCD Type 1)
# ================================================================
def build_dim_geography():
    logger.info("Building dim_geography (SCD Type 1)...")

    silver = read_delta(spark, SILVER_CUSTOMERS)

    staging = (
        silver
        .select("city", "state", "country", "zip_code")
        .distinct()
        .withColumn("geography_bk",
                    F.concat_ws("_", F.col("country"), F.col("state"), F.col("city")))
    )

    staging = generate_surrogate_key(staging, "geography_bk", "geography_sk")
    staging = (
        staging
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    update_cols = ["city", "state", "country", "zip_code"]

    if table_exists(spark, DIM_GEOGRAPHY):
        scd1_merge(
            spark         = spark,
            source_df     = staging,
            target_table  = DIM_GEOGRAPHY,
            merge_key     = "geography_bk",
            update_columns= update_cols
        )
    else:
        write_delta_overwrite(
            staging, DIM_GEOGRAPHY,
            f"{GOLD_PATH}/dim_geography"
        )

    optimize_table(spark, DIM_GEOGRAPHY, z_order_cols=["country", "state"])
    logger.info(f"✅ dim_geography complete → {DIM_GEOGRAPHY}")


# ================================================================
# MAIN
# ================================================================
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("GOLD LAYER — DIMENSION TABLES — START")
    logger.info("=" * 60)

    build_dim_date()         # No dependencies
    build_dim_geography()    # Depends on silver.customers
    build_dim_customer()     # Depends on silver.customers
    build_dim_product()      # Depends on silver.products

    logger.info("=" * 60)
    logger.info("✅ GOLD DIMENSIONS — COMPLETE")
    logger.info("=" * 60)

    for tbl in [DIM_CUSTOMER, DIM_PRODUCT, DIM_DATE, DIM_GEOGRAPHY]:
        count = spark.read.format("delta").table(tbl).count()
        logger.info(f"   {tbl}: {count} rows")
