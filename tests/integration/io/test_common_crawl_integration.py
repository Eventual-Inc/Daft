from __future__ import annotations

import os

import pytest
from botocore import session

import daft
from daft.datatype import DataType, TimeUnit
from daft.schema import Schema
from tests.assets import ASSET_FOLDER


@pytest.mark.integration()
def test_read_common_crawl(pytestconfig):
    if pytestconfig.getoption("--credentials") is not True:
        pytest.skip("Test can only run in a credentialled environment, and when run with the `--credentials` flag")

    sess = session.Session()
    creds = sess.get_credentials()

    io_config = daft.IOConfig(
        s3=daft.io.S3Config(
            key_id=creds.access_key, access_key=creds.secret_key, session_token=creds.token, region_name="us-west-2"
        )
    )

    df = daft.datasets.common_crawl("CC-MAIN-2024-51", num_files=1, io_config=io_config).limit(10)
    df = df.collect()

    expected_schema = Schema.from_pydict(
        {
            "WARC-Record-ID": DataType.string(),
            "WARC-Target-URI": DataType.string(),
            "WARC-Type": DataType.string(),
            "WARC-Date": DataType.timestamp(TimeUnit.ns(), timezone="Etc/UTC"),
            "Content-Length": DataType.int64(),
            "WARC-Identified-Payload-Type": DataType.string(),
            "warc_content": DataType.binary(),
            "warc_headers": DataType.string(),
        }
    )

    assert df.schema() == expected_schema, "Schema mismatch between actual and expected data"
    assert len(df) > 0, "Dataset should not be empty"
    warc_types = set(df.select("WARC-Type").to_pydict()["WARC-Type"])
    expected_warc_types = {
        "warcinfo",
        "response",
        "resource",
        "request",
        "metadata",
        "revisit",
        "conversion",
        "continuation",
    }
    for warc_type in warc_types:
        assert warc_type in expected_warc_types, f"WARC-Type {warc_type} is not in expected WARC types"

    record_ids = df.select("WARC-Record-ID").to_pydict()["WARC-Record-ID"]
    for record_id in record_ids:
        assert isinstance(record_id, str) and len(record_id) > 0, "WARC-Record-ID should be non-empty strings"

    # Check values against a pre-downloaded sample of Common Crawl. We only use the warcinfo entry to avoid any
    # potential NSFW data.
    sample_path = os.path.join(ASSET_FOLDER, "common-crawl-sample.parquet")
    sample_df = daft.read_parquet(sample_path)
    assert sample_df.to_pydict() == df.where(daft.col("WARC-Type") == "warcinfo").to_pydict()
