from __future__ import annotations


def test_create_df(spark_session):
    # Create a DataFrame with duplicate values
    data = [(1,), (2,), (2,), (3,), (3,), (3,)]
    df = spark_session.createDataFrame(data, ["value"])

    # Collect and verify results
    result = df.collect()

    # Verify the DataFrame has the expected number of rows and values
    assert sorted([row.value for row in result]) == [1, 2, 2, 3, 3, 3]
