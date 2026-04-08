# notebooks/04_gold_fact.py
# ============================================================
# NOTEBOOK 04 — Gold Layer: Fact Table (fact_sales)
# ============================================================
# Purpose : Build the central fact table of the Star Schema
#           by joining Silver orders with Gold dimension SKs.
#
# Grain  : One row per order line item
# Source : silver.orders + all gold dimension tables
#
# Key Metrics:
#   - quantity           : Units sold
#   - unit_price         : Selling price per unit
#   - unit_cost          : Cost price per unit (from dim_product)
#   - discount_amount    : Total discount applied
#   - gross_revenue      : quantity × unit_price
#   - net_revenue        : gross_revenue − discount_amount
#   - cogs               : quantity × unit_cost
#   - gross_profit       : net_revenue − cogs
#   - gross_profit_pct   : gross_profit / net_revenue × 100
# ============================================================

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import functions as F
from pyspark.sql.types import *

from utils.spark_utils import (
    get_spark, read_delta, write_delta_overwrite,
    table_exists, optimize_table
)
from utils.scd_utils import scd1_merge_multi_key
from utils.dq_utils import run_standard_checks
from utils.logger import get_logger
from config.config import (
    SILVER_ORDERS,
    DIM_CUSTOMER, DIM_PRODUCT, DIM_DATE, DIM_GEOGRAPHY,
    FACT_SALES, GOLD_PATH
)

logger = get_logger("04_gold_fact")
spark  = get_spark("SalesDW_GoldFact")


def build_fact_sales():
    logger.info("Building fact_sales...")

    # ---------------------------------------------------------------
    # 1. Load Silver orders
    # ---------------------------------------------------------------
    orders = read_delta(spark, SILVER_ORDERS)

    # ---------------------------------------------------------------
    # 2. Load dimension tables (select only required columns)
    # ---------------------------------------------------------------
    dim_customer = read_delta(spark, DIM_CUSTOMER).select(
        F.col("customer_bk"), F.col("customer_sk")
    )

    dim_product = read_delta(spark, DIM_PRODUCT).select(
        F.col("product_bk"), F.col("product_sk"),
        F.col("unit_cost").alias("product_unit_cost")
    )

    dim_date_order = read_delta(spark, DIM_DATE).select(
        F.col("full_date").alias("order_date_join"),
        F.col("date_key").alias("order_date_sk")
    )

    dim_date_ship = read_delta(spark, DIM_DATE).select(
        F.col("full_date").alias("ship_date_join"),
        F.col("date_key").alias("ship_date_sk")
    )

    # Load geography via customer — re-join on city/state/country
    dim_customer_full = read_delta(spark, DIM_CUSTOMER).select(
        F.col("customer_bk"), F.col("city"), F.col("state"), F.col("country")
    )
    dim_geography = read_delta(spark, DIM_GEOGRAPHY).select(
        F.col("geography_sk"), F.col("city"), F.col("state"), F.col("country")
    )

    # ---------------------------------------------------------------
    # 3. Resolve surrogate keys via lookups
    # ---------------------------------------------------------------
    fact = (
        orders

        # Join → customer_sk
        .join(dim_customer,
              orders["customer_id"] == dim_customer["customer_bk"], "left")

        # Join → product_sk + unit_cost from dim
        .join(dim_product,
              orders["product_id"] == dim_product["product_bk"], "left")

        # Join → order_date_sk
        .join(dim_date_order,
              orders["order_date"] == dim_date_order["order_date_join"], "left")

        # Join → ship_date_sk
        .join(dim_date_ship,
              orders["ship_date"] == dim_date_ship["ship_date_join"], "left")

        # Join → geography_sk  (via customer → city/state/country)
        .join(dim_customer_full,
              orders["customer_id"] == dim_customer_full["customer_bk"], "left")
        .join(dim_geography,
              (dim_customer_full["city"]    == dim_geography["city"])   &
              (dim_customer_full["state"]   == dim_geography["state"])  &
              (dim_customer_full["country"] == dim_geography["country"]), "left")
    )

    # ---------------------------------------------------------------
    # 4. Compute financial metrics
    # ---------------------------------------------------------------
    fact = (
        fact
        .withColumn("cogs",
                    F.round(F.col("quantity") * F.col("product_unit_cost"), 2))
        .withColumn("gross_profit",
                    F.round(F.col("net_revenue") - F.col("cogs"), 2))
        .withColumn("gross_profit_pct",
                    F.when(F.col("net_revenue") != 0,
                           F.round(F.col("gross_profit") / F.col("net_revenue") * 100, 2))
                     .otherwise(F.lit(0.0)))
    )

    # ---------------------------------------------------------------
    # 5. Select final fact columns
    # ---------------------------------------------------------------
    final_cols = [
        # Surrogate Keys (FKs to dimensions)
        F.col("customer_sk"),
        F.col("product_sk"),
        F.col("order_date_sk"),
        F.col("ship_date_sk"),
        F.col("geography_sk"),

        # Degenerate Dimensions (stored directly on fact)
        F.col("order_id"),
        F.col("order_line_id"),
        F.col("payment_method"),
        F.col("order_channel"),
        F.col("order_status"),

        # Measures
        F.col("quantity"),
        F.col("unit_price"),
        F.col("product_unit_cost").alias("unit_cost"),
        F.col("discount_pct"),
        F.col("discount_amount"),
        F.col("gross_revenue"),
        F.col("net_revenue"),
        F.col("cogs"),
        F.col("gross_profit"),
        F.col("gross_profit_pct"),
        F.col("days_to_ship"),

        # Audit
        F.col("order_date"),         # actual date (for partitioning)
        F.col("source_system"),
        F.col("batch_id"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    ]

    fact_final = fact.select(*final_cols)

    # ---------------------------------------------------------------
    # 6. Data Quality on Fact
    # ---------------------------------------------------------------
    run_standard_checks(
        fact_final, FACT_SALES,
        not_null_cols=["order_line_id", "customer_sk", "product_sk", "order_date_sk"],
        dedup_key_cols=["order_line_id"]
    )

    # ---------------------------------------------------------------
    # 7. Write to Gold — partitioned by order_date
    # ---------------------------------------------------------------
    if table_exists(spark, FACT_SALES):
        # Incremental merge on composite key
        scd1_merge_multi_key(
            spark         = spark,
            source_df     = fact_final,
            target_table  = FACT_SALES,
            merge_keys    = ["order_line_id"],
            update_columns= [
                "customer_sk", "product_sk", "order_date_sk", "ship_date_sk",
                "geography_sk", "payment_method", "order_channel", "order_status",
                "quantity", "unit_price", "unit_cost", "discount_pct",
                "discount_amount", "gross_revenue", "net_revenue",
                "cogs", "gross_profit", "gross_profit_pct", "days_to_ship"
            ]
        )
    else:
        write_delta_overwrite(
            fact_final, FACT_SALES,
            f"{GOLD_PATH}/fact_sales",
            partition_by=["order_date"]
        )

    optimize_table(spark, FACT_SALES,
                   z_order_cols=["customer_sk", "product_sk", "order_date_sk"])
    logger.info(f"✅ fact_sales complete → {FACT_SALES}")


# ================================================================
# MAIN
# ================================================================
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("GOLD LAYER — FACT TABLE — START")
    logger.info("=" * 60)

    build_fact_sales()

    count = spark.read.format("delta").table(FACT_SALES).count()
    logger.info(f"   {FACT_SALES}: {count} rows")

    logger.info("=" * 60)
    logger.info("✅ GOLD FACT TABLE — COMPLETE")
    logger.info("=" * 60)

    # ---------------------------------------------------------------
    # Quick analytical sanity check
    # ---------------------------------------------------------------
    logger.info("\n--- Sales Summary by Order Status ---")
    spark.read.format("delta").table(FACT_SALES) \
        .groupBy("order_status") \
        .agg(
            F.count("*").alias("orders"),
            F.round(F.sum("net_revenue"), 2).alias("total_revenue"),
            F.round(F.sum("gross_profit"), 2).alias("total_gross_profit")
        ) \
        .orderBy(F.desc("total_revenue")) \
        .show()
