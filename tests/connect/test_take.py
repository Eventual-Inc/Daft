from __future__ import annotations


def test_take(spark_session):
    # Create DataFrame with 10 rows
    df = spark_session.range(10)

    # Take first 5 rows and collect
    result = df.take(5)

    # Verify the expected values
    expected = df.limit(5).collect()

    assert result == expected

    # Test take with more rows than exist
    result_large = df.take(20)
    expected_large = df.collect()
    assert result_large == expected_large  # Should return all existing rows
