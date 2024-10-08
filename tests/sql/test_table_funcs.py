import daft


def test_sql_read_parquet():
    df = daft.sql("SELECT * FROM read_parquet('tests/assets/parquet-data/mvp.parquet')").collect()
    expected = daft.read_parquet("tests/assets/parquet-data/mvp.parquet").collect()
    assert df.to_pydict() == expected.to_pydict()
