from __future__ import annotations

import pytest
from pyspark.sql.functions import col


@pytest.mark.skip(
    reason="nulls_first not yet implemented in Daft - see https://github.com/Eventual-Inc/Daft/issues/3151"
)
def test_sort_single_column_asc(spark_session):
    # Create DataFrame with two columns using range
    df = spark_session.range(4).select(col("id").alias("num"), col("id").alias("letter"))

    # Sort by single column ascending
    df_sorted = df.sort(col("num").asc())

    # Verify the DataFrame is sorted correctly
    actual = df_sorted.collect()
    expected = [(0, 0), (1, 1), (2, 2), (3, 3)]
    assert [(row.num, row.letter) for row in actual] == expected


@pytest.mark.skip(
    reason="nulls_first not yet implemented in Daft - see https://github.com/Eventual-Inc/Daft/issues/3151"
)
def test_sort_single_column_desc(spark_session):
    # Create DataFrame with two columns using range
    df = spark_session.range(4).select(col("id").alias("num"), col("id").alias("letter"))

    # Sort by single column descending
    df_sorted = df.sort(col("num").desc())

    # Verify the DataFrame is sorted correctly
    actual = df_sorted.collect()
    expected = [(3, 3), (2, 2), (1, 1), (0, 0)]
    assert [(row.num, row.letter) for row in actual] == expected


@pytest.mark.skip(
    reason="nulls_first not yet implemented in Daft - see https://github.com/Eventual-Inc/Daft/issues/3151"
)
def test_sort_multiple_columns_asc_asc(spark_session):
    # Create DataFrame with two columns using range
    df = spark_session.range(4).select(col("id").alias("num"), col("id").alias("letter"))

    # Sort by multiple columns ascending
    df_sorted = df.sort(col("num").asc(), col("letter").asc())

    # Verify the DataFrame is sorted correctly
    actual = df_sorted.collect()
    expected = [(0, 0), (1, 1), (2, 2), (3, 3)]
    assert [(row.num, row.letter) for row in actual] == expected


@pytest.mark.skip(
    reason="nulls_first not yet implemented in Daft - see https://github.com/Eventual-Inc/Daft/issues/3151"
)
def test_sort_multiple_columns_desc_desc(spark_session):
    # Create DataFrame with two columns using range
    df = spark_session.range(4).select(col("id").alias("num"), col("id").alias("letter"))

    # Sort by multiple columns descending
    df_sorted = df.sort(col("num").desc(), col("letter").desc())

    # Verify the DataFrame is sorted correctly
    actual = df_sorted.collect()
    expected = [(3, 3), (2, 2), (1, 1), (0, 0)]
    assert [(row.num, row.letter) for row in actual] == expected


@pytest.mark.skip(
    reason="nulls_first not yet implemented in Daft - see https://github.com/Eventual-Inc/Daft/issues/3151"
)
def test_sort_multiple_columns_asc_desc(spark_session):
    # Create DataFrame with two columns using range
    df = spark_session.range(4).select(col("id").alias("num"), col("id").alias("letter"))

    # Sort by num ascending, letter descending
    df_sorted = df.sort(col("num").asc(), col("letter").desc())

    # Verify the DataFrame is sorted correctly
    actual = df_sorted.collect()
    expected = [(0, 0), (1, 1), (2, 2), (3, 3)]
    assert [(row.num, row.letter) for row in actual] == expected


@pytest.mark.skip(
    reason="nulls_first not yet implemented in Daft - see https://github.com/Eventual-Inc/Daft/issues/3151"
)
def test_sort_multiple_columns_desc_asc(spark_session):
    # Create DataFrame with two columns using range
    df = spark_session.range(4).select(col("id").alias("num"), col("id").alias("letter"))

    # Sort by num descending, letter ascending
    df_sorted = df.sort(col("num").desc(), col("letter").asc())

    # Verify the DataFrame is sorted correctly
    actual = df_sorted.collect()
    expected = [(3, 3), (2, 2), (1, 1), (0, 0)]
    assert [(row.num, row.letter) for row in actual] == expected
