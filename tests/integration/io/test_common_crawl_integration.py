from __future__ import annotations

import pytest

import daft
from daft.datatype import DataType, TimeUnit
from daft.schema import Schema


@pytest.mark.integration()
def test_read_common_crawl():
    # Results may be non-deterministic when using the Common Crawl dataset. Instead of testing for exact equality,
    # we test the expected properties of the data.
    df = daft.datasets.common_crawl("CC-MAIN-2024-51", num_files=1).limit(10)
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
