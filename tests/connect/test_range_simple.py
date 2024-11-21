from __future__ import annotations


def test_range_operation(spark_session):
    # Create a range using Spark
    # For example, creating a range from 0 to 9
    spark_range = spark_session.range(10)  # Creates DataFrame with numbers 0 to 9

    # Convert to Pandas DataFrame
    pandas_df = spark_range.toPandas()

    # Verify the DataFrame has expected values
    assert len(pandas_df) == 10, "DataFrame should have 10 rows"
    assert list(pandas_df["id"]) == list(range(10)), "DataFrame should contain values 0-9"
