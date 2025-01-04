import pytest
import daft

@pytest.mark.integration()
def test_write_df_to_hdfs():
    df1 = daft.from_pydict({"a": [1, 2, 3]})
    df1.write_parquet("hdfs://127.0.0.1:9000/data.parquet")
    df2 = daft.read_parquet("hdfs://127.0.0.1:9000/data.parquet")
    assert df2.to_pydict() == {"a": [1, 2, 3]}
