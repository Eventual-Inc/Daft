import daft


def test_sql():
    df = daft.sql("SELECT * FROM my_table")
    print(df)
    assert df is not None
