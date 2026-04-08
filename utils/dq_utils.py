# utils/dq_utils.py
# ============================================================
# Data Quality Checks — Bronze, Silver, Gold
# ============================================================

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import List, Dict, Any
from utils.logger import get_logger

logger = get_logger(__name__)


# ------------------------------------------------------------------
# Result accumulator
# ------------------------------------------------------------------
class DQResult:
    def __init__(self, table: str):
        self.table  = table
        self.checks: List[Dict[str, Any]] = []
        self.passed = True

    def add(self, check_name: str, passed: bool, details: str = ""):
        status = "PASS" if passed else "FAIL"
        self.checks.append({"check": check_name, "status": status, "details": details})
        if not passed:
            self.passed = False
        logger.info(f"[DQ] {self.table} | {check_name}: {status} | {details}")

    def summary(self) -> str:
        lines = [f"\n{'='*60}", f"DQ Report — {self.table}", f"{'='*60}"]
        for c in self.checks:
            lines.append(f"  [{c['status']:4}] {c['check']}: {c['details']}")
        overall = "✅ ALL PASSED" if self.passed else "❌ FAILURES DETECTED"
        lines.append(f"\nOverall: {overall}\n{'='*60}\n")
        return "\n".join(lines)


# ------------------------------------------------------------------
# Core DQ Functions
# ------------------------------------------------------------------
def check_row_count(df: DataFrame, result: DQResult,
                    min_count: int = 1) -> DQResult:
    """Ensure table has at least min_count rows."""
    count = df.count()
    passed = count >= min_count
    result.add("row_count_check", passed, f"rows={count}, min={min_count}")
    return result


def check_nulls(df: DataFrame, result: DQResult,
                columns: List[str], threshold: float = 0.05) -> DQResult:
    """Null rate must be below threshold for each column."""
    total = df.count()
    if total == 0:
        result.add("null_check", False, "No rows to check")
        return result
    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_rate  = null_count / total
        passed     = null_rate <= threshold
        result.add(
            f"null_check_{col}", passed,
            f"nulls={null_count}, rate={null_rate:.2%}, threshold={threshold:.0%}"
        )
    return result


def check_duplicates(df: DataFrame, result: DQResult,
                     key_columns: List[str], threshold: float = 0.01) -> DQResult:
    """Duplicate rate on key_columns must be below threshold."""
    total    = df.count()
    distinct = df.dropDuplicates(key_columns).count()
    dup_count = total - distinct
    dup_rate  = dup_count / total if total > 0 else 0
    passed    = dup_rate <= threshold
    result.add(
        "duplicate_check", passed,
        f"total={total}, distinct={distinct}, dup_rate={dup_rate:.2%}"
    )
    return result


def check_referential_integrity(spark: SparkSession, result: DQResult,
                                fact_table: str, dim_table: str,
                                fact_key: str, dim_key: str) -> DQResult:
    """Every FK in fact_table must exist in dim_table."""
    fact_df = spark.read.format("delta").table(fact_table)
    dim_df  = spark.read.format("delta").table(dim_table)

    orphans = (
        fact_df.select(fact_key)
               .distinct()
               .join(dim_df.select(dim_key), fact_df[fact_key] == dim_df[dim_key], "left_anti")
               .count()
    )
    passed = orphans == 0
    result.add(
        f"ref_integrity_{fact_table}_{fact_key}→{dim_table}_{dim_key}",
        passed,
        f"orphan_keys={orphans}"
    )
    return result


def check_value_range(df: DataFrame, result: DQResult,
                      column: str, min_val=None, max_val=None) -> DQResult:
    """Values in column must fall within [min_val, max_val]."""
    condition = F.lit(True)
    if min_val is not None:
        condition = condition & (F.col(column) >= min_val)
    if max_val is not None:
        condition = condition & (F.col(column) <= max_val)
    out_of_range = df.filter(~condition).count()
    passed = out_of_range == 0
    result.add(
        f"range_check_{column}", passed,
        f"out_of_range={out_of_range}, min={min_val}, max={max_val}"
    )
    return result


def check_regex_pattern(df: DataFrame, result: DQResult,
                        column: str, pattern: str) -> DQResult:
    """Values in column must match the given regex pattern."""
    invalid = df.filter(~F.col(column).rlike(pattern)).count()
    passed  = invalid == 0
    result.add(
        f"regex_check_{column}", passed,
        f"invalid_count={invalid}, pattern={pattern}"
    )
    return result


# ------------------------------------------------------------------
# Convenience wrapper — run a standard set of checks
# ------------------------------------------------------------------
def run_standard_checks(df: DataFrame, table_name: str,
                        not_null_cols: List[str],
                        dedup_key_cols: List[str]) -> DQResult:
    result = DQResult(table_name)
    check_row_count(df, result)
    check_nulls(df, result, not_null_cols)
    check_duplicates(df, result, dedup_key_cols)
    print(result.summary())
    return result
