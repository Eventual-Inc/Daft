from __future__ import annotations

import pytest

from daft.io import IOConfig, S3Config
from daft.table import Table


@pytest.mark.integration()
def test_non_retryable_error_404_table_read(nginx_config):
    endpoint_url, _ = nginx_config
    io_config = IOConfig(s3=S3Config(endpoint_url=endpoint_url, key_id="foo", access_key="bar"))

    # This path is hardcoded in our nginx conf to return a 404
    data_path = "s3://s3-bucket-name/non-retryable-404-failure.parquet"

    # TODO: Error code for this could be made nicer -- currently throws a 404 on HEAD request but
    # error message is not very user-friendly
    with pytest.raises(ValueError):
        Table.read_parquet(data_path, io_config=io_config)
