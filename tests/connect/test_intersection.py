from __future__ import annotations


def test_intersection(spark_session):
    # Create ranges using Spark - with overlap
    range1 = spark_session.range(7)  # Creates DataFrame with numbers 0 to 6
    range2 = spark_session.range(3, 10)  # Creates DataFrame with numbers 3 to 9

    # Intersect the two ranges
    intersected = range1.intersect(range2)

    # Collect results
    results = intersected.collect()

    # Verify the DataFrame has expected values
    # Intersection should only include overlapping values once
    assert len(results) == 4, "DataFrame should have 4 rows (overlapping values 3,4,5,6)"

    # Check that all expected values are present
    values = [row.id for row in results]
    assert sorted(values) == [3, 4, 5, 6], "Values should match expected overlapping sequence"
