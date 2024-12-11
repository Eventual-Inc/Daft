from __future__ import annotations


def test_count(spark_session):
    # Create a range using Spark
    # For example, creating a range from 0 to 9
    spark_range = spark_session.range(10)  # Creates DataFrame with numbers 0 to 9

    # Convert to Pandas DataFrame
    count = spark_range.count()

    # Verify the DataFrame has expected values
    assert count == 10, "DataFrame should have 10 rows"
