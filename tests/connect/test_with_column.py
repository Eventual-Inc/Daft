from __future__ import annotations

from pyspark.sql.functions import col


def test_with_column(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Add a new column that's a boolean indicating if id > 2
    df_with_col = df.withColumn("double_id", col("id") > 2)

    # Verify the schema has both columns
    assert "id" in df_with_col.schema.names, "Original column should still exist"
    assert "double_id" in df_with_col.schema.names, "New column should be added"

    # Verify the data is correct
    df_pandas = df_with_col.toPandas()
    assert (df_pandas["double_id"] == (df_pandas["id"] > 2)).all(), "New column should be greater than 2 comparison"
