
from pyspark.sql import SparkSession


def test_silver_customer_transformation_regression():
    spark = SparkSession.builder.getOrCreate()

    table_name = "retaildataplatform.silver.customers"
    df = spark.table(table_name)

    # 1. Regression check: required columns must remain available
    expected_columns = [
        "customer_id",
        "first_name",
        "last_name",
        "tax_id",
        "country",
        "city",
        "postcode",
        "units_purchased",
        "loyalty_segment",
        "last_update_ts",
    ]

    for column in expected_columns:
        assert column in df.columns, (
            f"Regression detected: missing column '{column}'"
        )

    # 2. Regression check: important data types must remain stable
    schema = {
        field.name: field.dataType.simpleString()
        for field in df.schema.fields
    }

    assert schema["customer_id"] == "int"
    assert schema["first_name"] == "string"
    assert schema["last_name"] == "string"
    assert schema["units_purchased"] == "int"
    assert schema["last_update_ts"] == "timestamp"

    # 3. Basic data sanity check
    assert df.count() > 0, (
        "Regression detected: Silver customers table is empty"
    )
