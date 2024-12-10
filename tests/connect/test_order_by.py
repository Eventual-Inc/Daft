from __future__ import annotations


def test_order_by(spark_session):
    # Create a range using Spark and order it
    spark_range = spark_session.range(10)  # Creates DataFrame with numbers 0 to 9
    ordered_df = spark_range.orderBy("id", ascending=False)  # Order by id descending

    # Collect results
    collected_rows = ordered_df.collect()

    # Verify the DataFrame has expected values and ordering
    assert len(collected_rows) == 10
    assert [row.id for row in collected_rows] == [9, 8, 7, 6, 5, 4, 3, 2, 1, 0]
