import pytest

pyiceberg = pytest.importorskip("pyiceberg")

import daft
from tests.conftest import assert_df_equals


@pytest.mark.integration
def test_daft_iceberg_cloud_table_load(cloud_iceberg_table):
    df = daft.read_iceberg(cloud_iceberg_table)
    daft_pandas = df.to_pandas()
    iceberg_pandas = cloud_iceberg_table.scan().to_arrow().to_pandas()
    assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])
