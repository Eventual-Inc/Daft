from __future__ import annotations

import daft


def test_warc():
    path = "tests/assets/example.warc"
    df = daft.read_warc(path)
    assert df.count_rows() == 30
    assert df.filter(df["WARC-Type"] == "response").count_rows() == 11
    # Test that we can correctly extract metadata from the warc_headers json column.
    num_rows = (
        df.with_column("WARC-Identified-Payload-Type", daft.col("warc_headers").json.query('."Content-Type"'))
        .where(
            (daft.col("WARC-Type") == "response")
            & (daft.col("Content-Length") < 10 * 1024 * 1024)
            & (daft.col("WARC-Identified-Payload-Type").is_in(['"application/http; msgtype=response"']))
        )
        .count_rows()
    )
    assert num_rows == 11


def test_warc_gz():
    # Test that we can read a gzipped warc file.
    path = "tests/assets/example.warc.gz"
    df = daft.read_warc(path)
    assert df.count_rows() == 30
    assert df.filter(df["WARC-Type"] == "response").count_rows() == 11
    # Test that we can correctly extract metadata from the warc_headers json column.
    num_rows = (
        df.with_column("WARC-Identified-Payload-Type", daft.col("warc_headers").json.query('."Content-Type"'))
        .where(
            (daft.col("WARC-Type") == "response")
            & (daft.col("Content-Length") < 10 * 1024 * 1024)
            & (daft.col("WARC-Identified-Payload-Type").is_in(['"application/http; msgtype=response"']))
        )
        .count_rows()
    )
    assert num_rows == 11
