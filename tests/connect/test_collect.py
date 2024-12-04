from __future__ import annotations


def test_range_collect(spark_session):
    # Create a range using Spark
    # For example, creating a range from 0 to 9
    spark_range = spark_session.range(10)  # Creates DataFrame with numbers 0 to 9

    # Collect the data
    collected_rows = spark_range.collect()

    # Verify the collected data has expected values
    assert len(collected_rows) == 10, "Should have 10 rows"
    assert [row["id"] for row in collected_rows] == list(range(10)), "Should contain values 0-9"
