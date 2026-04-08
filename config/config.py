# config/config.py
# ============================================================
# Central Configuration — Sales Data Warehouse
# ============================================================

from datetime import datetime


# ------------------------------------------------------------------
# Environment
# ------------------------------------------------------------------
ENV = "dev"          # dev | staging | prod
BATCH_ID = datetime.now().strftime("%Y%m%d%H%M%S")

# ------------------------------------------------------------------
# Databricks / DBFS Paths
# ------------------------------------------------------------------
BASE_PATH = "/mnt/sales_dw"                         # Mount point (ADLS / S3)
SOURCE_PATH = f"{BASE_PATH}/source"
BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"
GOLD_PATH   = f"{BASE_PATH}/gold"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints"
LOG_PATH    = f"{BASE_PATH}/logs"

# ------------------------------------------------------------------
# Database / Schema Names
# ------------------------------------------------------------------
BRONZE_DB = "bronze"
SILVER_DB = "silver"
GOLD_DB   = "gold"

# ------------------------------------------------------------------
# Source System Identifiers
# ------------------------------------------------------------------
SOURCE_CRM      = "CRM_SYSTEM"
SOURCE_ERP      = "ERP_SYSTEM"
SOURCE_ECOMMERCE = "ECOMMERCE_PLATFORM"

# ------------------------------------------------------------------
# Bronze Table Names
# ------------------------------------------------------------------
BRONZE_CUSTOMERS = f"{BRONZE_DB}.crm_customers"
BRONZE_PRODUCTS  = f"{BRONZE_DB}.erp_products"
BRONZE_ORDERS    = f"{BRONZE_DB}.ecom_orders"

# ------------------------------------------------------------------
# Silver Table Names
# ------------------------------------------------------------------
SILVER_CUSTOMERS = f"{SILVER_DB}.customers"
SILVER_PRODUCTS  = f"{SILVER_DB}.products"
SILVER_ORDERS    = f"{SILVER_DB}.orders"

# ------------------------------------------------------------------
# Gold Table Names
# ------------------------------------------------------------------
DIM_CUSTOMER    = f"{GOLD_DB}.dim_customer"
DIM_PRODUCT     = f"{GOLD_DB}.dim_product"
DIM_DATE        = f"{GOLD_DB}.dim_date"
DIM_GEOGRAPHY   = f"{GOLD_DB}.dim_geography"
FACT_SALES      = f"{GOLD_DB}.fact_sales"

# ------------------------------------------------------------------
# Date Dimension Config
# ------------------------------------------------------------------
DIM_DATE_START = "2020-01-01"
DIM_DATE_END   = "2030-12-31"

# ------------------------------------------------------------------
# Spark / Delta Config
# ------------------------------------------------------------------
SPARK_CONFIG = {
    "spark.sql.extensions":                  "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog":       "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.shuffle.partitions":          "200",
    "spark.sql.adaptive.enabled":            "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.databricks.delta.optimizeWrite.enabled":  "true",
    "spark.databricks.delta.autoCompact.enabled":    "true",
}

# ------------------------------------------------------------------
# Data Quality Thresholds
# ------------------------------------------------------------------
DQ_NULL_THRESHOLD       = 0.05   # Max 5% nulls allowed
DQ_DUPLICATE_THRESHOLD  = 0.01   # Max 1% duplicates allowed
DQ_MIN_ROW_COUNT        = 1      # Minimum rows expected
