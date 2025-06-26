from __future__ import annotations


def test_create_simple_df(spark_session):
    # Create simple DataFrame with single column
    data = [(1,), (2,), (3,)]
    df = spark_session.createDataFrame(data, ["id"])

    # Convert to pandas and verify
    df_pandas = df.toPandas()
    assert len(df_pandas) == 3, "DataFrame should have 3 rows"
    assert list(df_pandas["id"]) == [1, 2, 3], "DataFrame should contain expected values"


def test_create_float_df(spark_session):
    # Create DataFrame with float column
    float_data = [(1.1,), (2.2,), (3.3,)]
    df_float = spark_session.createDataFrame(float_data, ["value"])
    df_float_pandas = df_float.toPandas()
    assert len(df_float_pandas) == 3, "Float DataFrame should have 3 rows"
    assert list(df_float_pandas["value"]) == [1.1, 2.2, 3.3], "Float DataFrame should contain expected values"


def test_create_two_column_df(spark_session):
    # Create DataFrame with two numeric columns
    two_col_data = [(1, 10), (2, 20), (3, 30)]
    df_two = spark_session.createDataFrame(two_col_data, ["num1", "num2"])
    df_two_pandas = df_two.toPandas()
    assert len(df_two_pandas) == 3, "Two-column DataFrame should have 3 rows"
    assert list(df_two_pandas["num1"]) == [1, 2, 3], "First number column should contain expected values"
    assert list(df_two_pandas["num2"]) == [10, 20, 30], "Second number column should contain expected values"


def test_create_boolean_df(spark_session):
    boolean_data = [(True,), (False,), (True,)]
    df_boolean = spark_session.createDataFrame(boolean_data, ["value"])
    df_boolean_pandas = df_boolean.toPandas()
    assert len(df_boolean_pandas) == 3, "Boolean DataFrame should have 3 rows"
    assert list(df_boolean_pandas["value"]) == [True, False, True], "Boolean DataFrame should contain expected values"


def test_create_string_df(spark_session):
    # Create DataFrame with string column
    string_data = [("hello",), ("world",), ("test",)]
    df_string = spark_session.createDataFrame(string_data, ["text"])
    df_string_pandas = df_string.toPandas()
    assert len(df_string_pandas) == 3, "String DataFrame should have 3 rows"
    assert list(df_string_pandas["text"]) == [
        "hello",
        "world",
        "test",
    ], "String DataFrame should contain expected values"


def test_create_mixed_type_df(spark_session):
    # Create DataFrame with mixed types
    mixed_data = [(1, "one", True), (2, "two", False), (3, "three", True)]
    df_mixed = spark_session.createDataFrame(mixed_data, ["id", "name", "active"])
    df_mixed_pandas = df_mixed.toPandas()
    assert len(df_mixed_pandas) == 3, "Mixed-type DataFrame should have 3 rows"
    assert list(df_mixed_pandas["id"]) == [1, 2, 3], "ID column should contain expected values"
    assert list(df_mixed_pandas["name"]) == ["one", "two", "three"], "Name column should contain expected values"
    assert list(df_mixed_pandas["active"]) == [True, False, True], "Active column should contain expected values"


def test_create_null_df(spark_session):
    # Create DataFrame with null values
    null_data = [(1, None), (None, "test"), (3, "data")]
    df_null = spark_session.createDataFrame(null_data, ["id", "value"])
    df_null_pandas = df_null.toPandas()
    assert len(df_null_pandas) == 3, "Null-containing DataFrame should have 3 rows"
    assert df_null_pandas["id"].isna().sum() == 1, "ID column should have one null value"
    assert df_null_pandas["value"].isna().sum() == 1, "Value column should have one null value"


def test_create_decimal_df(spark_session):
    # Create DataFrame with decimal/double precision numbers
    decimal_data = [(1.23456789,), (9.87654321,), (5.55555555,)]
    df_decimal = spark_session.createDataFrame(decimal_data, ["amount"])
    df_decimal_pandas = df_decimal.toPandas()
    assert len(df_decimal_pandas) == 3, "Decimal DataFrame should have 3 rows"
    assert abs(df_decimal_pandas["amount"][0] - 1.23456789) < 1e-8, "Decimal values should maintain precision"


def test_create_special_chars_df(spark_session):
    # Create DataFrame with empty strings and special characters
    special_data = [("",), (" ",), ("!@#$%^&*",)]
    df_special = spark_session.createDataFrame(special_data, ["special"])
    df_special_pandas = df_special.toPandas()
    assert len(df_special_pandas) == 3, "Special character DataFrame should have 3 rows"
    assert list(df_special_pandas["special"]) == [
        "",
        " ",
        "!@#$%^&*",
    ], "Special character DataFrame should contain expected values"
