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


def test_range_first(spark_session):
    spark_range = spark_session.range(10)
    first_row = spark_range.first()
    assert first_row["id"] == 0, "First row should have id=0"


def test_range_limit(spark_session):
    spark_range = spark_session.range(10)
    limited_df = spark_range.limit(5).toPandas()
    assert len(limited_df) == 5, "Limited DataFrame should have 5 rows"
    assert list(limited_df["id"]) == list(range(5)), "Limited DataFrame should contain values 0-4"
