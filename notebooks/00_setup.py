# notebooks/00_setup.py
# ============================================================
# NOTEBOOK 00 — Environment Setup
# ============================================================
# Purpose : Create databases (schemas), configure Spark, and
#           prepare the Databricks environment for the pipeline.
# Run Once: Execute this notebook before any other notebooks.
# ============================================================

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.spark_utils import get_spark
from utils.logger import get_logger
from config.config import (
    BRONZE_DB, SILVER_DB, GOLD_DB,
    BRONZE_PATH, SILVER_PATH, GOLD_PATH,
    SOURCE_PATH, CHECKPOINT_PATH, LOG_PATH
)

logger = get_logger("00_setup")

# ---------------------------------------------------------------
# 1. Initialise Spark Session
# ---------------------------------------------------------------
spark = get_spark("SalesDW_Setup")

# ---------------------------------------------------------------
# 2. Create Layer Databases
# ---------------------------------------------------------------
logger.info("Creating Bronze / Silver / Gold databases...")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB} COMMENT 'Raw ingestion layer'")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB} COMMENT 'Cleansed and conformed layer'")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}   COMMENT 'Star schema data warehouse'")

logger.info("Databases created successfully.")

# ---------------------------------------------------------------
# 3. Set Default Database Locations (Databricks Unity Catalog
#    or DBFS paths — comment/uncomment as needed)
# ---------------------------------------------------------------
# On Unity Catalog you would instead use:
#   CREATE SCHEMA IF NOT EXISTS catalog.bronze MANAGED LOCATION '...'

logger.info("Configuring Delta Lake settings...")

# Enable automatic schema evolution for Delta
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# ---------------------------------------------------------------
# 4. Verify Setup
# ---------------------------------------------------------------
databases = [row.namespace for row in spark.sql("SHOW DATABASES").collect()]
logger.info(f"Available databases: {databases}")

assert BRONZE_DB in databases, f"Database {BRONZE_DB} not found!"
assert SILVER_DB in databases, f"Database {SILVER_DB} not found!"
assert GOLD_DB   in databases, f"Database {GOLD_DB} not found!"

logger.info("=" * 60)
logger.info("✅ Setup complete — environment is ready.")
logger.info(f"   Bronze DB : {BRONZE_DB}")
logger.info(f"   Silver DB : {SILVER_DB}")
logger.info(f"   Gold DB   : {GOLD_DB}")
logger.info("=" * 60)
