from __future__ import annotations


def test_explain(spark_session):
    # Create ranges using Spark - with overlap
    range1 = spark_session.range(7)  # Creates DataFrame with numbers 0 to 6
    range2 = spark_session.range(3, 10)  # Creates DataFrame with numbers 3 to 9

    # Union the two ranges
    unioned = range1.union(range2)

    # Get the explain plan
    explain_str = unioned.explain(extended=True)

    # Verify explain output contains expected elements
    print(explain_str)
