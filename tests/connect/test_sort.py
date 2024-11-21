from __future__ import annotations

from pyspark.sql.functions import col


def test_sort_multiple_columns(spark_session):
    # Create DataFrame with two columns using range
    df = spark_session.range(4).select(col("id").alias("num"), col("id").alias("letter"))

    # Sort by multiple columns
    df_sorted = df.sort(col("num").asc(), col("letter").desc())

    # Verify the DataFrame is sorted correctly
    actual = df_sorted.collect()
    expected = [(0, 0), (1, 1), (2, 2), (3, 3)]
    assert [(row.num, row.letter) for row in actual] == expected


def test_sort_mixed_order(spark_session):
    # Create DataFrame with two columns using range
    df = spark_session.range(4).select(col("id").alias("num"), col("id").alias("letter"))

    # Sort with mixed ascending/descending order
    df_sorted = df.sort(col("num").desc(), col("letter").asc())

    # Verify the DataFrame is sorted correctly
    actual = df_sorted.collect()
    expected = [(3, 3), (2, 2), (1, 1), (0, 0)]
    assert [(row.num, row.letter) for row in actual] == expected
