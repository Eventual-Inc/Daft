from daft.dataframe import DataFrame
from tests.conftest import assert_df_equals
from tests.dataframe_cookbook.conftest import SERVICE_REQUESTS_CSV


def test_parquet_write(tmp_path):
    df = DataFrame.from_csv(SERVICE_REQUESTS_CSV)

    pd_df = df.write_parquet(tmp_path).to_pandas()
    assert len(pd_df) == 1
    read_back_pd_df = DataFrame.from_parquet(tmp_path.as_posix() + "/*.parquet").to_pandas()

    assert_df_equals(df.to_pandas(), read_back_pd_df)


def test_parquet_write_with_partitioning(tmp_path):
    df = DataFrame.from_csv(SERVICE_REQUESTS_CSV)

    pd_df = df.write_parquet(tmp_path, partition_cols=["Borough"]).to_pandas()
    assert len(pd_df) == 5

    read_back_pd_df = DataFrame.from_parquet(tmp_path.as_posix() + "/**/*.parquet").to_pandas()

    assert_df_equals(df.exclude("Borough").to_pandas(), read_back_pd_df)
