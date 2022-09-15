from daft.dataframe import DataFrame
from tests.dataframe_cookbook.conftest import SERVICE_REQUESTS_CSV


def test_write():
    df = DataFrame.from_csv(SERVICE_REQUESTS_CSV)
    import ipdb

    ipdb.set_trace()
    df.write_parquet("./tmp", partition_cols=["Borough"]).collect()
