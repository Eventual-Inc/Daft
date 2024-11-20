from __future__ import annotations

from pyspark.sql.functions import col


def test_where(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Filter the DataFrame where 'id' is greater than 5
    df_filtered = df.where(col("id") > 5)

    # Verify the filter was applied correctly by checking the expected data
    df_filtered_pandas = df_filtered.toPandas()
    expected_data = [6, 7, 8, 9]
    assert df_filtered_pandas["id"].tolist() == expected_data, "Filtered data does not match expected data"
