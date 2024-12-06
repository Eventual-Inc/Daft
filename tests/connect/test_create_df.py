from __future__ import annotations


def test_create_df(spark_session):
    # Create simple DataFrame with single column
    data = [(1,), (2,), (3,)]
    df = spark_session.createDataFrame(data, ["id"])

    # Convert to pandas and verify
    df_pandas = df.toPandas()
    assert len(df_pandas) == 3, "DataFrame should have 3 rows"
    assert list(df_pandas["id"]) == [1, 2, 3], "DataFrame should contain expected values"

    # Create DataFrame with float column
    float_data = [(1.1,), (2.2,), (3.3,)]
    df_float = spark_session.createDataFrame(float_data, ["value"])
    df_float_pandas = df_float.toPandas()
    assert len(df_float_pandas) == 3, "Float DataFrame should have 3 rows"
    assert list(df_float_pandas["value"]) == [1.1, 2.2, 3.3], "Float DataFrame should contain expected values"

    # Create DataFrame with two numeric columns
    two_col_data = [(1, 10), (2, 20), (3, 30)]
    df_two = spark_session.createDataFrame(two_col_data, ["num1", "num2"])
    df_two_pandas = df_two.toPandas()
    assert len(df_two_pandas) == 3, "Two-column DataFrame should have 3 rows"
    assert list(df_two_pandas["num1"]) == [1, 2, 3], "First number column should contain expected values"
    assert list(df_two_pandas["num2"]) == [10, 20, 30], "Second number column should contain expected values"

    # now do boolean
    print("now testing boolean")
    boolean_data = [(True,), (False,), (True,)]
    df_boolean = spark_session.createDataFrame(boolean_data, ["value"])
    df_boolean_pandas = df_boolean.toPandas()
    assert len(df_boolean_pandas) == 3, "Boolean DataFrame should have 3 rows"
    assert list(df_boolean_pandas["value"]) == [True, False, True], "Boolean DataFrame should contain expected values"
