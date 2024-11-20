from __future__ import annotations

from pyspark.sql.functions import col


def test_alias(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Simply rename the 'id' column to 'my_number'
    df_renamed = df.select(col("id").alias("my_number"))

    # Verify the alias was set correctly
    assert df_renamed.schema != df.schema, "Schema should be changed after alias"
    assert df_renamed.count() == df.count(), "Row count should be unchanged after alias"

    # Verify the data is unchanged but column name is different
    df_rows = df.collect()
    df_renamed_rows = df_renamed.collect()
    assert [row.id for row in df_rows] == [
        row.my_number for row in df_renamed_rows
    ], "Data should be unchanged after alias"
