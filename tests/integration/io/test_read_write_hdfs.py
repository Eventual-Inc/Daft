import pytest
import daft

@pytest.mark.integration()
def write_df_to_hdfs():
    df = daft.DataFrame({"a": [1, 2, 3]})
    df.write_parquet("hdfs://127.0.0.1:9000/data.parquet")

@pytest.mark.integration()
def read_df_from_hdfs():
    df = daft.read_parquet("hdfs://127.0.0.1:9000/data.parquet")
    assert df.to_pydict() == {"a": [1, 2, 3]}
