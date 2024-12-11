from __future__ import annotations

from pyspark.sql.functions import col


def test_filter(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Filter for values less than 5
    df_filtered = df.filter(col("id") < 5)

    # Verify the schema is unchanged after filter
    assert df_filtered.schema == df.schema, "Schema should be unchanged after filter"

    # Verify the filtered data is correct
    df_filtered_pandas = df_filtered.toPandas()
    assert len(df_filtered_pandas) == 5, "Should have 5 rows after filtering < 5"
    assert all(df_filtered_pandas["id"] < 5), "All values should be less than 5"
