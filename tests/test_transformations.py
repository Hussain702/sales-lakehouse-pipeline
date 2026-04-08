# tests/test_transformations.py
# ============================================================
# Unit Tests — Sales Data Warehouse Transformations
# ============================================================

import pytest
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from utils.scd_utils import generate_surrogate_key
from utils.dq_utils import DQResult, check_nulls, check_duplicates, check_row_count


# ================================================================
# Fixtures
# ================================================================
@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("SalesDW_Tests")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


# ================================================================
# Test: Surrogate Key Generation
# ================================================================
class TestSurrogateKey:
    def test_surrogate_key_created(self, spark):
        df = spark.createDataFrame([("C001",), ("C002",)], ["customer_id"])
        result = generate_surrogate_key(df, "customer_id", "customer_sk")
        assert "customer_sk" in result.columns

    def test_surrogate_key_unique(self, spark):
        df = spark.createDataFrame([
            ("C001",), ("C002",), ("C003",)
        ], ["customer_id"])
        result = generate_surrogate_key(df, "customer_id", "customer_sk")
        total  = result.count()
        unique = result.select("customer_sk").distinct().count()
        assert total == unique, "Surrogate keys must be unique"

    def test_surrogate_key_stable(self, spark):
        """Same business key must produce the same SK across runs."""
        df1 = spark.createDataFrame([("C001",)], ["customer_id"])
        df2 = spark.createDataFrame([("C001",)], ["customer_id"])
        sk1 = generate_surrogate_key(df1, "customer_id", "sk").collect()[0]["sk"]
        sk2 = generate_surrogate_key(df2, "customer_id", "sk").collect()[0]["sk"]
        assert sk1 == sk2, "Surrogate key must be stable / idempotent"


# ================================================================
# Test: Data Quality Checks
# ================================================================
class TestDQChecks:
    def test_row_count_pass(self, spark):
        df = spark.createDataFrame([("A",), ("B",)], ["col"])
        result = DQResult("test_table")
        check_row_count(df, result, min_count=1)
        assert result.passed

    def test_row_count_fail(self, spark):
        df = spark.createDataFrame([], StructType([StructField("col", StringType())]))
        result = DQResult("test_table")
        check_row_count(df, result, min_count=1)
        assert not result.passed

    def test_null_check_pass(self, spark):
        df = spark.createDataFrame([("A", "X"), ("B", "Y")], ["id", "val"])
        result = DQResult("test_table")
        check_nulls(df, result, columns=["id"], threshold=0.05)
        assert result.passed

    def test_null_check_fail(self, spark):
        df = spark.createDataFrame([("A", None), ("B", None)], ["id", "val"])
        result = DQResult("test_table")
        check_nulls(df, result, columns=["val"], threshold=0.05)
        assert not result.passed

    def test_duplicate_check_pass(self, spark):
        df = spark.createDataFrame([("A",), ("B",), ("C",)], ["id"])
        result = DQResult("test_table")
        check_duplicates(df, result, key_columns=["id"])
        assert result.passed

    def test_duplicate_check_fail(self, spark):
        df = spark.createDataFrame([("A",), ("A",), ("B",)], ["id"])
        result = DQResult("test_table")
        check_duplicates(df, result, key_columns=["id"], threshold=0.0)
        assert not result.passed


# ================================================================
# Test: Silver Transformations (sample logic)
# ================================================================
class TestSilverTransformations:
    def test_email_lowercased(self, spark):
        df = spark.createDataFrame([("Alice.TEST@Email.COM",)], ["email"])
        result = df.withColumn("email", F.lower(F.trim(F.col("email"))))
        assert result.collect()[0]["email"] == "alice.test@email.com"

    def test_customer_id_uppercased(self, spark):
        df = spark.createDataFrame([("c001",)], ["customer_id"])
        result = df.withColumn("customer_id", F.upper(F.trim(F.col("customer_id"))))
        assert result.collect()[0]["customer_id"] == "C001"

    def test_net_revenue_calculation(self, spark):
        schema = StructType([
            StructField("quantity",     IntegerType(), True),
            StructField("unit_price",   DoubleType(),  True),
            StructField("discount_pct", DoubleType(),  True),
        ])
        df = spark.createDataFrame([(2, 100.0, 0.10)], schema)
        result = (
            df
            .withColumn("gross_revenue", F.col("quantity") * F.col("unit_price"))
            .withColumn("discount_amount", F.col("gross_revenue") * F.col("discount_pct"))
            .withColumn("net_revenue", F.col("gross_revenue") - F.col("discount_amount"))
        )
        row = result.collect()[0]
        assert row["gross_revenue"]  == 200.0
        assert row["discount_amount"] == 20.0
        assert row["net_revenue"]     == 180.0

    def test_negative_quantity_filtered(self, spark):
        schema = StructType([StructField("quantity", IntegerType(), True)])
        df = spark.createDataFrame([(5,), (-1,), (0,)], schema)
        filtered = df.filter(F.col("quantity") > 0)
        assert filtered.count() == 1

    def test_days_to_ship_calculation(self, spark):
        from pyspark.sql.types import DateType
        schema = StructType([
            StructField("order_date", DateType(), True),
            StructField("ship_date",  DateType(), True),
        ])
        from datetime import date
        df = spark.createDataFrame([(date(2024, 1, 5), date(2024, 1, 8))], schema)
        result = df.withColumn("days_to_ship", F.datediff(F.col("ship_date"), F.col("order_date")))
        assert result.collect()[0]["days_to_ship"] == 3


# ================================================================
# Test: Gold Transformations
# ================================================================
class TestGoldTransformations:
    def test_gross_profit_calculation(self, spark):
        schema = StructType([
            StructField("quantity",   IntegerType(), True),
            StructField("unit_price", DoubleType(),  True),
            StructField("unit_cost",  DoubleType(),  True),
            StructField("net_revenue", DoubleType(), True),
        ])
        df = spark.createDataFrame([(3, 100.0, 60.0, 300.0)], schema)
        result = (
            df
            .withColumn("cogs", F.col("quantity") * F.col("unit_cost"))
            .withColumn("gross_profit", F.col("net_revenue") - F.col("cogs"))
            .withColumn("gross_profit_pct",
                        F.round(F.col("gross_profit") / F.col("net_revenue") * 100, 2))
        )
        row = result.collect()[0]
        assert row["cogs"]             == 180.0
        assert row["gross_profit"]     == 120.0
        assert row["gross_profit_pct"] == 40.0
