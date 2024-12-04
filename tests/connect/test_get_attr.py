from __future__ import annotations


def test_get_attr(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Get column using df[...]
    # df.get_attr("id") is equivalent to df["id"]
    df_col = df["id"]

    # Check that column values match expected range
    values = df.select(df_col).collect()  # Changed to select column first
    assert len(values) == 10
    assert [row[0] for row in values] == list(range(10))  # Need to extract values from Row objects
