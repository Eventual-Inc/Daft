from __future__ import annotations


def test_distinct(spark_session):
    # Create ranges using Spark - with overlap
    range1 = spark_session.range(7)  # Creates DataFrame with numbers 0 to 6
    range2 = spark_session.range(3, 10)  # Creates DataFrame with numbers 3 to 9

    # Union the two ranges and get distinct values
    unioned = range1.union(range2).distinct()

    # Collect results
    results = unioned.collect()

    # Verify the DataFrame has expected values
    # Distinct removes duplicates, so length should be 10 (0-9)
    assert len(results) == 10, "DataFrame should have 10 unique rows"
    
    # Check that all expected values are present, with no duplicates
    values = [row.id for row in results]
    assert sorted(values) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], "Values should match expected sequence without duplicates"
