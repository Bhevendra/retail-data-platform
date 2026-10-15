
import uuid

from pyspark.sql import SparkSession
from common_utils.generic_functions import rename_columns


def test_bronze_transformation_and_delta_write_read():
    # Reuse the active Databricks Spark session.
    # Do not set .master() or stop() the shared session.
    spark = SparkSession.builder.getOrCreate()

    # Use a unique table name so the test does not overwrite another table.
    table_name = (
        f"retaildataplatform.bronze.integration_test_{uuid.uuid4().hex}"
    )

    try:
        # 1. Create sample source data.
        source_df = spark.createDataFrame(
            [
                (101, "Alice"),
                (102, "Bob"),
            ],
            ["cust_id", "name"],
        )

        # 2. Apply the real project transformation.
        transformed_df = rename_columns(
            source_df,
            {
                "cust_id": "customer_id",
                "name": "customer_name",
            },
        )

        # 3. Write the transformed data to a Delta table.
        transformed_df.write.format("delta").mode("error").saveAsTable(
            table_name
        )

        # 4. Read the data back from Delta.
        result_df = spark.table(table_name)

        # 5. Validate the integration result.
        assert result_df.columns == ["customer_id", "customer_name"]
        assert result_df.count() == 2

        actual_rows = {
            (row["customer_id"], row["customer_name"])
            for row in result_df.collect()
        }

        expected_rows = {
            (101, "Alice"),
            (102, "Bob"),
        }

        assert actual_rows == expected_rows

    finally:
        # Clean up the test table, even if an assertion fails.
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
