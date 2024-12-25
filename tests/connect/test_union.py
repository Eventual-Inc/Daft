from __future__ import annotations


def test_union(spark_session):
    # Create ranges using Spark - with overlap
    range1 = spark_session.range(7)  # Creates DataFrame with numbers 0 to 6
    range2 = spark_session.range(3, 10)  # Creates DataFrame with numbers 3 to 9

    # Union the two ranges
    unioned = range1.union(range2)

    # Collect results
    results = unioned.collect()

    # Verify the DataFrame has expected values
    # Union includes duplicates, so length should be sum of both ranges
    assert len(results) == 14, "DataFrame should have 14 rows (7 + 7)"

    # Check that all expected values are present, including duplicates
    values = [row.id for row in results]
    assert sorted(values) == [
        0,
        1,
        2,
        3,
        3,
        4,
        4,
        5,
        5,
        6,
        6,
        7,
        8,
        9,
    ], "Values should match expected sequence with duplicates"
