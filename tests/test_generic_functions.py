
import pytest
from pyspark.sql import Row
from common_utils.generic_functions import rename_columns


@pytest.fixture(scope="module")
def spark():
    """Reuse the existing Databricks Spark session."""
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def test_rename_existing_column(spark):
    df = spark.createDataFrame([
        Row(cust_id=1, name="Alice"),
        Row(cust_id=2, name="Bob")
    ])

    result = rename_columns(df, {"cust_id": "customer_id"})

    assert "customer_id" in result.columns
    assert "cust_id" not in result.columns
    assert result.count() == 2


def test_missing_column_is_ignored(spark):
    df = spark.createDataFrame([
        Row(cust_id=1, name="Alice")
    ])

    result = rename_columns(df, {"missing_col": "new_col"})

    assert result.columns == ["cust_id", "name"]


def test_multiple_columns_are_renamed(spark):
    df = spark.createDataFrame([
        Row(cust_id=1, name="Alice")
    ])

    result = rename_columns(
        df,
        {"cust_id": "customer_id", "name": "customer_name"}
    )

    assert result.columns == ["customer_id", "customer_name"]


def test_empty_mapping_changes_nothing(spark):
    df = spark.createDataFrame([
        Row(cust_id=1, name="Alice")
    ])

    result = rename_columns(df, {})

    assert result.columns == ["cust_id", "name"]
    assert result.collect() == df.collect()
