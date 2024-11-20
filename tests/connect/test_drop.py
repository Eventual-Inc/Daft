from __future__ import annotations


def test_drop(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Drop the 'id' column
    df_dropped = df.drop("id")

    # Verify the drop was successful
    assert "id" not in df_dropped.columns, "Column 'id' should be dropped"
    assert len(df_dropped.columns) == len(df.columns) - 1, "Should have one less column after drop"

    # Verify the DataFrame has no columns after dropping all columns"
    assert len(df_dropped.toPandas().columns) == 0, "DataFrame should have no columns after dropping 'id'"
    assert df_dropped.count() == df.count(), "Row count should be unchanged after drop"
