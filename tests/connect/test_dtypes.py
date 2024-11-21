from __future__ import annotations

from pyspark.sql.functions import col


def test_dtypes(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Add a column that will have repeated values for grouping
    df = df.withColumn("group", col("id") % 3)

    # Check dtypes of the DataFrame
    expected_dtypes = [
        ("id", "bigint"),
        ("group", "bigint")
    ]

    # Get actual dtypes
    actual_dtypes = df.dtypes

    # Verify the dtypes match expected
    assert actual_dtypes == expected_dtypes

    # Also check individual column types
    assert df.schema["id"].dataType.simpleString() == "bigint"
    assert df.schema["group"].dataType.simpleString() == "bigint"
