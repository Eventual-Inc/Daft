import pytest
from pyspark.sql import functions as F


def test_numeric_equals(spark_session):
    """Test numeric equality comparison with NULL handling."""
    data = [(1, 10), (2, None)]
    df = spark_session.createDataFrame(data, ["id", "value"])

    result = df.withColumn("equals_20", F.col("value") == F.lit(20)).collect()

    assert result[0].equals_20 is False  # 10 == 20
    assert result[1].equals_20 is None  # NULL == 20


def test_string_equals(spark_session):
    """Test string equality comparison with NULL handling."""
    data = [(1, "apple"), (2, None)]
    df = spark_session.createDataFrame(data, ["id", "text"])

    result = df.withColumn("equals_banana", F.col("text") == F.lit("banana")).collect()

    assert result[0].equals_banana is False  # apple == banana
    assert result[1].equals_banana is None  # NULL == banana


@pytest.mark.skip(reason="We believe null-safe equals are not yet implemented")
def test_null_safe_equals(spark_session):
    """Test null-safe equality comparison."""
    data = [(1, 10), (2, None)]
    df = spark_session.createDataFrame(data, ["id", "value"])

    result = df.withColumn("null_safe_equals", F.col("value").eqNullSafe(F.lit(10))).collect()

    assert result[0].null_safe_equals is True  # 10 <=> 10
    assert result[1].null_safe_equals is False  # NULL <=> 10


def test_not(spark_session):
    """Test logical NOT operation with NULL handling."""
    data = [(True,), (False,), (None,)]
    df = spark_session.createDataFrame(data, ["value"])

    result = df.withColumn("not_value", ~F.col("value")).collect()

    assert result[0].not_value is False  # NOT True
    assert result[1].not_value is True  # NOT False
    assert result[2].not_value is None  # NOT NULL
