from __future__ import annotations

from pyspark.sql.types import LongType, StructField, StructType


def test_schema(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Define the expected schema
    # in reality should be nullable=False, but daft has all our structs as nullable=True
    expected_schema = StructType([StructField("id", LongType(), nullable=True)])

    # Verify the schema is as expected
    assert df.schema == expected_schema, "Schema should match the expected schema"
