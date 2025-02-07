from __future__ import annotations

import pandas as pd
import polars as pl
from pyspark.sql import SparkSession
from pyspark.sql.functions import col as spark_col
from pyspark.sql.functions import substring

# Test strings that cover various UTF-8 scenarios
TEST_STRINGS = [
    "Hello☃World",  # UTF-8 in middle
    "😉test",  # UTF-8 at start
    "test🌈",  # UTF-8 at end
    "☃😉🌈",  # Multiple UTF-8 characters
    "",  # Empty string
]


def test_negative_start_index() -> None:
    """Test behavior when start index is negative."""
    print("\n" + "=" * 80)
    print("TEST: Negative Start Index")
    print("=" * 80)
    test_data = TEST_STRINGS
    start = -1  # Take last character
    length = 1  # Take one character

    # Pandas - allows negative indices (counts from end)
    pd_result = pd.Series(test_data).str.slice(start, length)

    # Polars - allows negative indices (counts from end)
    pl_result = pl.Series("col", test_data).str.slice(start, length)

    # Spark - doesn't support negative indices
    spark = SparkSession.builder.appName("SubstrTest").getOrCreate()
    spark_df = spark.createDataFrame([(x,) for x in test_data], ["col"])
    try:
        spark_result = spark_df.select(
            substring(spark_col("col"), start + 1, length)  # Spark uses 1-based indexing
        ).collect()
        spark_list = [row[0] for row in spark_result]
    except Exception as e:
        spark_list = [f"Error: {e!s}"] * len(test_data)
    finally:
        spark.stop()

    print("\nTest negative start index (start=-1, length=1):")
    print("Input:", test_data)
    print("Pandas:", pd_result.tolist())
    print("Polars:", pl_result.to_list())
    print("Spark:", spark_list)
    # Expected behavior:
    # - Pandas: Returns last character for each string
    # - Polars: Returns last character for each string
    # - Spark: Errors on negative indices


def test_start_at_string_boundaries() -> None:
    """Test behavior when start index is at string boundaries (0, len, len+1)."""
    print("\n" + "=" * 80)
    print("TEST: String Boundaries (Start at 0, len, len+1)")
    print("=" * 80)
    test_data = TEST_STRINGS
    substr_length = 3  # Fixed length to see what happens at boundaries

    # Test start = 0 (beginning of string)
    pd_start_0 = pd.Series(test_data).str.slice(0, substr_length)
    pl_start_0 = pl.Series("col", test_data).str.slice(0, substr_length)

    # Test start = len(string) (end of string)
    pd_df = pd.DataFrame({"col": test_data})
    pd_start_len = pd_df.apply(
        lambda row: row["col"][len(str(row["col"])) : len(str(row["col"])) + substr_length]
        if pd.notna(row["col"])
        else None,
        axis=1,
    )

    pl_df = pl.DataFrame({"col": test_data})
    pl_start_len = pl_df.select(pl.col("col").str.slice(pl.col("col").str.len_chars(), substr_length)).to_series()

    # Test start > len(string) (beyond string)
    pd_start_beyond = pd_df.apply(
        lambda row: row["col"][len(str(row["col"])) + 1 : len(str(row["col"])) + 1 + substr_length]
        if pd.notna(row["col"])
        else None,
        axis=1,
    )

    pl_start_beyond = pl_df.select(
        pl.col("col").str.slice(pl.col("col").str.len_chars() + 1, substr_length)
    ).to_series()

    # Spark implementation
    spark = SparkSession.builder.appName("SubstrTest").getOrCreate()
    spark_df = spark.createDataFrame([(x,) for x in test_data], ["col"])

    spark_start_0 = spark_df.select(
        substring(spark_col("col"), 1, substr_length)  # Spark uses 1-based indexing
    ).collect()

    # Using SQL expression for column arithmetic
    spark_start_len = spark_df.selectExpr(f"substring(col, length(col) + 1, {substr_length}) as result").collect()

    spark_start_beyond = spark_df.selectExpr(f"substring(col, length(col) + 2, {substr_length}) as result").collect()

    spark.stop()

    print("\nTest start index at boundaries (length=3):")
    print("Input:", test_data)
    print("\nStart at beginning (0):")
    print("Pandas:", pd_start_0.tolist())
    print("Polars:", pl_start_0.to_list())
    print("Spark:", [row[0] for row in spark_start_0])

    print("\nStart at end (len):")
    print("Pandas:", pd_start_len.tolist())
    print("Polars:", pl_start_len.to_list())
    print("Spark:", [row[0] for row in spark_start_len])

    print("\nStart beyond end (len+1):")
    print("Pandas:", pd_start_beyond.tolist())
    print("Polars:", pl_start_beyond.to_list())
    print("Spark:", [row[0] for row in spark_start_beyond])
    # Expected behavior:
    # - Start at 0: Returns up to length characters from start
    # - Start at len: Returns empty string or null
    # - Start beyond len: Returns empty string or null


def test_length_not_specified() -> None:
    """Test behavior when length parameter is not specified."""
    print("\n" + "=" * 80)
    print("TEST: Unspecified Length Parameter")
    print("=" * 80)
    test_data = TEST_STRINGS
    start = 2  # Start from third character

    # Pandas - should go to end of string
    pd_result = pd.Series(test_data).str.slice(start)

    # Polars - should go to end of string
    pl_result = pl.Series("col", test_data).str.slice(start)

    # Spark - requires length parameter, use max possible length
    spark = SparkSession.builder.appName("SubstrTest").getOrCreate()
    spark_df = spark.createDataFrame([(x,) for x in test_data], ["col"])
    max_length = 1000  # Use a large number to effectively get rest of string
    spark_result = spark_df.select(substring(spark_col("col"), start + 1, max_length)).collect()
    spark_list = [row[0] for row in spark_result]
    spark.stop()

    print("\nTest length not specified (start=2):")
    print("Input:", test_data)
    print("Pandas:", pd_result.tolist())
    print("Polars:", pl_result.to_list())
    print("Spark (using max_length=1000):", spark_list)


def test_length_none() -> None:
    """Test behavior when length parameter is explicitly set to None."""
    print("\n" + "=" * 80)
    print("TEST: None Length Parameter")
    print("=" * 80)
    test_data = TEST_STRINGS
    start = 2
    length = None

    # Pandas - None length
    pd_result = pd.Series(test_data).str.slice(start, length)

    # Polars - None length
    pl_result = pl.Series("col", test_data).str.slice(start, length)

    # Spark - None length (not typically supported)
    spark = SparkSession.builder.appName("SubstrTest").getOrCreate()
    spark_df = spark.createDataFrame([(x,) for x in test_data], ["col"])
    try:
        spark_result = spark_df.select(substring(spark_col("col"), start + 1, None)).collect()
        spark_list = [row[0] for row in spark_result]
    except Exception as e:
        spark_list = [f"Error: {e!s}"] * len(test_data)
    finally:
        spark.stop()

    print("\nTest explicit None length (start=2, length=None):")
    print("Input:", test_data)
    print("Pandas:", pd_result.tolist())
    print("Polars:", pl_result.to_list())
    print("Spark:", spark_list)
    # Expected behavior: May differ between libraries, but should handle None gracefully


def test_zero_and_negative_length() -> None:
    """Test behavior with zero and negative length values."""
    print("\n" + "=" * 80)
    print("TEST: Zero and Negative Length")
    print("=" * 80)
    test_data = TEST_STRINGS
    start = 2

    # Zero length
    pd_zero = pd.Series(test_data).str.slice(start, 0)
    pl_zero = pl.Series("col", test_data).str.slice(start, 0)

    # Negative length - Polars doesn't support this, only test with Pandas
    pd_neg = pd.Series(test_data).str.slice(start, -2)
    try:
        pl_neg = pl.Series("col", test_data).str.slice(start, -2)
    except Exception as e:
        pl_neg = [f"Error: {e!s}"] * len(test_data)

    # Spark implementation
    spark = SparkSession.builder.appName("SubstrTest").getOrCreate()
    spark_df = spark.createDataFrame([(x,) for x in test_data], ["col"])

    # Zero length
    try:
        spark_zero = spark_df.select(substring(spark_col("col"), start + 1, 0)).collect()
        spark_zero_list = [row[0] for row in spark_zero]
    except Exception as e:
        spark_zero_list = [f"Error: {e!s}"] * len(test_data)

    # Negative length
    try:
        spark_neg = spark_df.select(substring(spark_col("col"), start + 1, -2)).collect()
        spark_neg_list = [row[0] for row in spark_neg]
    except Exception as e:
        spark_neg_list = [f"Error: {e!s}"] * len(test_data)

    spark.stop()

    print("\nTest zero length (start=2, length=0):")
    print("Input:", test_data)
    print("Pandas:", pd_zero.tolist())
    print("Polars:", pl_zero.to_list())
    print("Spark:", spark_zero_list)

    print("\nTest negative length (start=2, length=-2):")
    print("Pandas:", pd_neg.tolist())
    print("Polars:", pl_neg)
    print("Spark:", spark_neg_list)


def test_length_overflow() -> None:
    """Test behavior when length extends beyond string end or is very large."""
    print("\n" + "=" * 80)
    print("TEST: Length Overflow")
    print("=" * 80)
    test_data = TEST_STRINGS
    start = 2

    # Length > remaining string
    remaining_length = 100  # Much larger than any test string
    pd_overflow = pd.Series(test_data).str.slice(start, remaining_length)
    pl_overflow = pl.Series("col", test_data).str.slice(start, remaining_length)

    # Very large length (near system limits)
    large_length = 999999999
    pd_large = pd.Series(test_data).str.slice(start, large_length)
    pl_large = pl.Series("col", test_data).str.slice(start, large_length)

    # Spark implementation
    spark = SparkSession.builder.appName("SubstrTest").getOrCreate()
    spark_df = spark.createDataFrame([(x,) for x in test_data], ["col"])

    # Length > remaining string
    spark_overflow = spark_df.select(substring(spark_col("col"), start + 1, remaining_length)).collect()

    # Very large length
    try:
        spark_large = spark_df.select(substring(spark_col("col"), start + 1, large_length)).collect()
        spark_large_list = [row[0] for row in spark_large]
    except Exception as e:
        spark_large_list = [f"Error: {e!s}"] * len(test_data)

    spark.stop()

    print("\nTest length > remaining string (start=2, length=100):")
    print("Input:", test_data)
    print("Pandas:", pd_overflow.tolist())
    print("Polars:", pl_overflow.to_list())
    print("Spark:", [row[0] for row in spark_overflow])

    print("\nTest very large length (start=2, length=999999999):")
    print("Pandas:", pd_large.tolist())
    print("Polars:", pl_large.to_list())
    print("Spark:", spark_large_list)
    # Expected behavior:
    # Overflow: Should return remaining string from start
    # Large length: Should handle gracefully, same as overflow


def test_utf8_character_boundaries() -> None:
    """Test behavior when substring boundaries intersect UTF-8 characters."""
    print("\n" + "=" * 80)
    print("TEST: UTF-8 Character Boundaries")
    print("=" * 80)
    # Test strings where start/length would split UTF-8 characters
    test_data = [
        "Hello☃World",  # 3-byte UTF-8 char
        "Hi😉Smile",  # 4-byte UTF-8 char
        "🌈Rainbow",  # Start with 4-byte
        "Star⭐End",  # 3-byte in middle
    ]

    # Test with start in middle of UTF-8 char
    pd_mid_char = pd.Series(test_data).str.slice(5, 3)
    pl_mid_char = pl.Series("col", test_data).str.slice(5, 3)

    # Test with length that would split UTF-8 char
    pd_split_char = pd.Series(test_data).str.slice(4, 2)
    pl_split_char = pl.Series("col", test_data).str.slice(4, 2)

    # Spark implementation
    spark = SparkSession.builder.appName("SubstrTest").getOrCreate()
    spark_df = spark.createDataFrame([(x,) for x in test_data], ["col"])

    spark_mid_char = spark_df.select(
        substring(spark_col("col"), 6, 3)  # Spark uses 1-based indexing
    ).collect()

    spark_split_char = spark_df.select(substring(spark_col("col"), 5, 2)).collect()

    spark.stop()

    print("\nTest UTF-8 character boundary handling:")
    print("Input:", test_data)
    print("\nStart in middle of UTF-8 char (start=5, length=3):")
    print("Pandas:", pd_mid_char.tolist())
    print("Polars:", pl_mid_char.to_list())
    print("Spark:", [row[0] for row in spark_mid_char])

    print("\nLength splitting UTF-8 char (start=4, length=2):")
    print("Pandas:", pd_split_char.tolist())
    print("Polars:", pl_split_char.to_list())
    print("Spark:", [row[0] for row in spark_split_char])


def test_mixed_ascii_utf8() -> None:
    """Test behavior with strings containing both ASCII and UTF-8 characters."""
    print("\n" + "=" * 80)
    print("TEST: Mixed ASCII and UTF-8")
    print("=" * 80)
    test_data = [
        "ABC☃DEF",  # ASCII-UTF8-ASCII
        "12😉34",  # Numbers-UTF8-Numbers
        "Hi🌈Bye⭐",  # Multiple UTF-8 mixed
        "Test⭐",  # ASCII ending with UTF-8
        "☃Start",  # UTF-8 starting with ASCII
    ]

    # Test normal substring
    pd_result = pd.Series(test_data).str.slice(2, 3)
    pl_result = pl.Series("col", test_data).str.slice(2, 3)

    # Spark implementation
    spark = SparkSession.builder.appName("SubstrTest").getOrCreate()
    spark_df = spark.createDataFrame([(x,) for x in test_data], ["col"])

    spark_result = spark_df.select(
        substring(spark_col("col"), 3, 3)  # Spark uses 1-based indexing
    ).collect()

    spark.stop()

    print("\nTest mixed ASCII and UTF-8 handling (start=2, length=3):")
    print("Input:", test_data)
    print("Pandas:", pd_result.tolist())
    print("Polars:", pl_result.to_list())
    print("Spark:", [row[0] for row in spark_result])


def test_null_values() -> None:
    """Test behavior with null/None values in the input."""
    print("\n" + "=" * 80)
    print("TEST: Null Values")
    print("=" * 80)
    test_data = [None, "Hello", None, "World", None]

    # Test with different start/length combinations
    pd_result1 = pd.Series(test_data).str.slice(0, 3)
    pd_result2 = pd.Series(test_data).str.slice(2, None)

    pl_result1 = pl.Series("col", test_data).str.slice(0, 3)
    pl_result2 = pl.Series("col", test_data).str.slice(2, None)

    # Spark implementation
    spark = SparkSession.builder.appName("SubstrTest").getOrCreate()
    spark_df = spark.createDataFrame([(x,) for x in test_data], ["col"])

    spark_result1 = spark_df.select(substring(spark_col("col"), 1, 3)).collect()

    # Fixed: providing length parameter for Spark
    max_length = 1000  # Use a large number to effectively get rest of string
    spark_result2 = spark_df.select(substring(spark_col("col"), 3, max_length)).collect()

    spark.stop()

    print("\nTest null value handling:")
    print("Input:", test_data)
    print("\nFirst test (start=0, length=3):")
    print("Pandas:", pd_result1.tolist())
    print("Polars:", pl_result1.to_list())
    print("Spark:", [row[0] for row in spark_result1])

    print("\nSecond test (start=2, length=None):")
    print("Pandas:", pd_result2.tolist())
    print("Polars:", pl_result2.to_list())
    print("Spark (using max_length=1000):", [row[0] for row in spark_result2])
