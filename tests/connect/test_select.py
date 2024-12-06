from __future__ import annotations

from pyspark.sql.functions import col


def test_select(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Select just the 'id' column
    df_selected = df.select(col("id"))

    # Verify the schema is unchanged since we selected same column
    assert df_selected.schema == df.schema, "Schema should be unchanged after selecting same column"
    assert len(df_selected.collect()) == 10, "Row count should be unchanged after select"

    # Verify the data is unchanged
    df_data = [row["id"] for row in df.collect()]
    df_selected_data = [row["id"] for row in df_selected.collect()]
    assert df_data == df_selected_data, "Data should be unchanged after select"
