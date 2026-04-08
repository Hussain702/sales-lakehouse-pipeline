# utils/scd_utils.py
# ============================================================
# SCD Type 1 — Merge (Upsert) Logic using Delta Lake MERGE
# ============================================================
#
# SCD Type 1 Strategy:
#   - No historical tracking.
#   - When a record changes in the source, the target row is
#     OVERWRITTEN with the latest values.
#   - New records are INSERTED.
#   - Deleted source records are NOT handled (soft-delete pattern
#     can be added via an is_active flag if required).
# ============================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from utils.logger import get_logger

logger = get_logger(__name__)


def scd1_merge(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    merge_key: str,
    update_columns: list,
) -> None:
    """
    Performs an SCD Type 1 MERGE (upsert) on a Delta table.

    Parameters
    ----------
    spark          : Active SparkSession
    source_df      : Incoming / staging DataFrame
    target_table   : Fully qualified Delta table name  e.g. 'gold.dim_customer'
    merge_key      : Business key column used to match source ↔ target
                     e.g. 'customer_id'
    update_columns : List of columns to update when a match is found.
                     Exclude surrogate key and created_at from this list.

    Behaviour
    ---------
    MATCH    → UPDATE listed columns + set updated_at = current_timestamp()
    NO MATCH → INSERT entire source row
    """
    logger.info(f"SCD1 MERGE → {target_table} | key: {merge_key}")

    delta_target = DeltaTable.forName(spark, target_table)

    # Build SET clause for updates
    update_expr = {col: F.col(f"src.{col}") for col in update_columns}
    update_expr["updated_at"] = F.current_timestamp()

    # Build INSERT clause (all columns from source)
    insert_expr = {col: F.col(f"src.{col}") for col in source_df.columns}
    insert_expr["updated_at"] = F.current_timestamp()

    (
        delta_target.alias("tgt")
        .merge(
            source_df.alias("src"),
            f"tgt.{merge_key} = src.{merge_key}"
        )
        .whenMatchedUpdate(set=update_expr)
        .whenNotMatchedInsert(values=insert_expr)
        .execute()
    )

    logger.info(f"SCD1 MERGE complete → {target_table}")


def scd1_merge_multi_key(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    merge_keys: list,
    update_columns: list,
) -> None:
    """
    SCD Type 1 MERGE with a composite business key.

    Parameters
    ----------
    merge_keys : List of column names forming the composite key
                 e.g. ['order_id', 'line_item_id']
    """
    logger.info(f"SCD1 MERGE (composite key) → {target_table} | keys: {merge_keys}")

    delta_target = DeltaTable.forName(spark, target_table)

    # Compose match condition
    match_condition = " AND ".join(
        [f"tgt.{k} = src.{k}" for k in merge_keys]
    )

    update_expr = {col: F.col(f"src.{col}") for col in update_columns}
    update_expr["updated_at"] = F.current_timestamp()

    insert_expr = {col: F.col(f"src.{col}") for col in source_df.columns}
    insert_expr["updated_at"] = F.current_timestamp()

    (
        delta_target.alias("tgt")
        .merge(source_df.alias("src"), match_condition)
        .whenMatchedUpdate(set=update_expr)
        .whenNotMatchedInsert(values=insert_expr)
        .execute()
    )

    logger.info(f"SCD1 MERGE (composite) complete → {target_table}")


def generate_surrogate_key(df: DataFrame, business_key_col: str,
                           sk_col_name: str = "surrogate_key") -> DataFrame:
    """
    Generates a stable surrogate key using SHA-256 hash of the business key.
    This ensures idempotency — same business key always produces same SK.

    Parameters
    ----------
    df               : Input DataFrame
    business_key_col : Column containing the business/natural key
    sk_col_name      : Name for the new surrogate key column
    """
    return df.withColumn(
        sk_col_name,
        F.conv(F.substring(F.sha2(F.col(business_key_col).cast("string"), 256), 1, 15), 16, 10)
         .cast("long")
    )
