# notebooks/05_data_quality.py
# ============================================================
# NOTEBOOK 05 — Data Quality & Audit Reports
# ============================================================
# Purpose : Run comprehensive DQ checks across all layers and
#           produce audit reports for each Gold table.
#
# Includes:
#   - Row count checks per layer
#   - Null checks on critical columns
#   - Duplicate checks on business/natural keys
#   - Referential integrity: fact → dimensions
#   - Value range checks on financial metrics
#   - Aggregated analytical summary queries
# ============================================================

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import functions as F

from utils.spark_utils import get_spark, read_delta
from utils.dq_utils import (
    DQResult, check_row_count, check_nulls, check_duplicates,
    check_referential_integrity, check_value_range
)
from utils.logger import get_logger
from config.config import (
    BRONZE_CUSTOMERS, BRONZE_PRODUCTS, BRONZE_ORDERS,
    SILVER_CUSTOMERS, SILVER_PRODUCTS, SILVER_ORDERS,
    DIM_CUSTOMER, DIM_PRODUCT, DIM_DATE, DIM_GEOGRAPHY, FACT_SALES
)

logger = get_logger("05_data_quality")
spark  = get_spark("SalesDW_DataQuality")


# ================================================================
# ROW COUNTS — All Layers
# ================================================================
def report_row_counts():
    logger.info("\n" + "=" * 60)
    logger.info("ROW COUNT SUMMARY")
    logger.info("=" * 60)
    tables = [
        ("BRONZE", BRONZE_CUSTOMERS),
        ("BRONZE", BRONZE_PRODUCTS),
        ("BRONZE", BRONZE_ORDERS),
        ("SILVER", SILVER_CUSTOMERS),
        ("SILVER", SILVER_PRODUCTS),
        ("SILVER", SILVER_ORDERS),
        ("GOLD",   DIM_CUSTOMER),
        ("GOLD",   DIM_PRODUCT),
        ("GOLD",   DIM_DATE),
        ("GOLD",   DIM_GEOGRAPHY),
        ("GOLD",   FACT_SALES),
    ]
    for layer, tbl in tables:
        try:
            count = spark.read.format("delta").table(tbl).count()
            logger.info(f"  [{layer:6}] {tbl}: {count:,} rows")
        except Exception as e:
            logger.warning(f"  [{layer:6}] {tbl}: ERROR — {e}")


# ================================================================
# DQ CHECKS — Silver Layer
# ================================================================
def dq_silver_customers():
    df = read_delta(spark, SILVER_CUSTOMERS)
    result = DQResult(SILVER_CUSTOMERS)
    check_row_count(df, result, min_count=1)
    check_nulls(df, result, columns=["customer_id", "email", "full_name", "country"])
    check_duplicates(df, result, key_columns=["customer_id"])
    print(result.summary())
    return result


def dq_silver_products():
    df = read_delta(spark, SILVER_PRODUCTS)
    result = DQResult(SILVER_PRODUCTS)
    check_row_count(df, result, min_count=1)
    check_nulls(df, result, columns=["product_id", "product_name", "unit_price"])
    check_duplicates(df, result, key_columns=["product_id"])
    check_value_range(df, result, "unit_price", min_val=0)
    check_value_range(df, result, "unit_cost",  min_val=0)
    print(result.summary())
    return result


def dq_silver_orders():
    df = read_delta(spark, SILVER_ORDERS)
    result = DQResult(SILVER_ORDERS)
    check_row_count(df, result, min_count=1)
    check_nulls(df, result, columns=["order_id", "order_line_id", "customer_id", "product_id"])
    check_duplicates(df, result, key_columns=["order_line_id"])
    check_value_range(df, result, "quantity",    min_val=1)
    check_value_range(df, result, "unit_price",  min_val=0)
    check_value_range(df, result, "net_revenue", min_val=0)
    print(result.summary())
    return result


# ================================================================
# DQ CHECKS — Gold Layer
# ================================================================
def dq_gold_dimensions():
    for tbl, key in [
        (DIM_CUSTOMER,  "customer_bk"),
        (DIM_PRODUCT,   "product_bk"),
        (DIM_GEOGRAPHY, "geography_bk"),
    ]:
        df = read_delta(spark, tbl)
        result = DQResult(tbl)
        check_row_count(df, result, min_count=1)
        check_duplicates(df, result, key_columns=[key])
        print(result.summary())


def dq_fact_referential_integrity():
    logger.info("Checking referential integrity on fact_sales...")

    for dim_table, dim_key, fact_key in [
        (DIM_CUSTOMER,  "customer_sk",   "customer_sk"),
        (DIM_PRODUCT,   "product_sk",    "product_sk"),
        (DIM_DATE,      "date_key",      "order_date_sk"),
    ]:
        result = DQResult(FACT_SALES)
        check_referential_integrity(
            spark, result,
            fact_table=FACT_SALES, dim_table=dim_table,
            fact_key=fact_key, dim_key=dim_key
        )
        print(result.summary())


def dq_fact_metrics():
    df = read_delta(spark, FACT_SALES)
    result = DQResult(FACT_SALES)
    check_row_count(df, result, min_count=1)
    check_nulls(df, result, columns=["order_line_id", "customer_sk", "product_sk"])
    check_duplicates(df, result, key_columns=["order_line_id"])
    check_value_range(df, result, "quantity",     min_val=1)
    check_value_range(df, result, "net_revenue",  min_val=0)
    check_value_range(df, result, "gross_profit", min_val=None)  # can be negative
    print(result.summary())


# ================================================================
# ANALYTICAL SUMMARY REPORTS
# ================================================================
def analytical_reports():
    logger.info("\n" + "=" * 60)
    logger.info("ANALYTICAL SUMMARY REPORTS")
    logger.info("=" * 60)

    fact = read_delta(spark, FACT_SALES)
    dim_customer = read_delta(spark, DIM_CUSTOMER)
    dim_product  = read_delta(spark, DIM_PRODUCT)
    dim_date     = read_delta(spark, DIM_DATE)

    # --- Report 1: Revenue by Product Category ---
    logger.info("\n[Report 1] Revenue by Product Category")
    (
        fact
        .join(dim_product, "product_sk", "left")
        .groupBy("category")
        .agg(
            F.count("order_line_id").alias("order_lines"),
            F.sum("quantity").alias("total_units"),
            F.round(F.sum("net_revenue"), 2).alias("total_revenue"),
            F.round(F.sum("gross_profit"), 2).alias("total_profit"),
            F.round(F.avg("gross_profit_pct"), 2).alias("avg_margin_pct")
        )
        .orderBy(F.desc("total_revenue"))
        .show(truncate=False)
    )

    # --- Report 2: Revenue by Customer Segment ---
    logger.info("\n[Report 2] Revenue by Customer Segment")
    (
        fact
        .join(dim_customer, "customer_sk", "left")
        .groupBy("customer_segment")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.sum("net_revenue"), 2).alias("total_revenue"),
            F.round(F.avg("net_revenue"), 2).alias("avg_order_value")
        )
        .orderBy(F.desc("total_revenue"))
        .show(truncate=False)
    )

    # --- Report 3: Monthly Revenue Trend ---
    logger.info("\n[Report 3] Monthly Revenue Trend")
    (
        fact
        .join(dim_date.select("date_key", "year_month"),
              fact["order_date_sk"] == dim_date["date_key"], "left")
        .groupBy("year_month")
        .agg(
            F.count("order_line_id").alias("transactions"),
            F.round(F.sum("net_revenue"), 2).alias("monthly_revenue"),
            F.round(F.sum("gross_profit"), 2).alias("monthly_profit")
        )
        .orderBy("year_month")
        .show(20, truncate=False)
    )

    # --- Report 4: Top 5 Products by Revenue ---
    logger.info("\n[Report 4] Top 5 Products by Revenue")
    (
        fact
        .join(dim_product, "product_sk", "left")
        .groupBy("product_name", "category")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("net_revenue"), 2).alias("total_revenue"),
            F.round(F.avg("gross_profit_pct"), 2).alias("avg_margin_pct")
        )
        .orderBy(F.desc("total_revenue"))
        .limit(5)
        .show(truncate=False)
    )

    # --- Report 5: Sales by Order Channel ---
    logger.info("\n[Report 5] Sales by Order Channel")
    (
        fact
        .groupBy("order_channel", "payment_method")
        .agg(
            F.count("order_id").alias("orders"),
            F.round(F.sum("net_revenue"), 2).alias("revenue")
        )
        .orderBy(F.desc("revenue"))
        .show(truncate=False)
    )


# ================================================================
# MAIN
# ================================================================
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("DATA QUALITY & AUDIT REPORT — START")
    logger.info("=" * 60)

    report_row_counts()

    logger.info("\n--- Silver Layer DQ ---")
    dq_silver_customers()
    dq_silver_products()
    dq_silver_orders()

    logger.info("\n--- Gold Dimension DQ ---")
    dq_gold_dimensions()

    logger.info("\n--- Gold Fact DQ ---")
    dq_fact_metrics()
    dq_fact_referential_integrity()

    logger.info("\n--- Analytical Reports ---")
    analytical_reports()

    logger.info("=" * 60)
    logger.info("✅ DATA QUALITY & AUDIT — COMPLETE")
    logger.info("=" * 60)
