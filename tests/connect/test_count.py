from __future__ import annotations


def test_count(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Verify count is correct
    assert df.count() == 10, "DataFrame should have 10 rows"
