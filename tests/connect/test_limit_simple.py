from __future__ import annotations


def test_range_first(spark_session):
    spark_range = spark_session.range(10)
    first_row = spark_range.first()
    assert first_row["id"] == 0, "First row should have id=0"


def test_range_limit(spark_session):
    spark_range = spark_session.range(10)
    limited_df = spark_range.limit(5).toPandas()
    assert len(limited_df) == 5, "Limited DataFrame should have 5 rows"
    assert list(limited_df["id"]) == list(range(5)), "Limited DataFrame should contain values 0-4"
