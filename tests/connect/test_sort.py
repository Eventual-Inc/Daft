from __future__ import annotations

from pyspark.sql.functions import col


def test_sort(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Sort the DataFrame by 'id' column in descending order
    df_sorted = df.sort(col("id").desc())

    # Verify the DataFrame is sorted correctly
    df_pandas = df.toPandas()
    df_sorted_pandas = df_sorted.toPandas()
    assert df_sorted_pandas["id"].equals(df_pandas["id"].sort_values(ascending=False).reset_index(drop=True)), "Data should be sorted in descending order"
