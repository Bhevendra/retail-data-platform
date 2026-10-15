
from pyspark.sql import SparkSession


def test_gold_customer_table_is_available():
    spark = SparkSession.builder.getOrCreate()

    # Critical table that downstream users depend on
    table_name = "retaildataplatform.gold.dim_customer"

    # 1. Check that the table exists
    assert spark.catalog.tableExists(table_name), (
        f"Smoke test failed: {table_name} does not exist"
    )

    # 2. Check that the table can be queried
    df = spark.table(table_name)

    # 3. Check that it has columns
    assert len(df.columns) > 0, (
        f"Smoke test failed: {table_name} has no columns"
    )

    # 4. Check that the query executes successfully
    df.limit(1).collect()
