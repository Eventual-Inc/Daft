from __future__ import annotations

import daft


def test_warc():
    path = "tests/assets/example.warc"
    df = daft.read_warc(path)
    assert df.count_rows() == 30
    assert df.filter(df["WARC-Type"] == "response").count_rows() == 11
    # Test that we can correctly extract metadata from the warc_headers json column.
    num_rows = (
        df.with_column("WARC-Identified-Payload-Type", daft.col("warc_headers").jq('."Content-Type"'))
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
        df.with_column("WARC-Identified-Payload-Type", daft.col("warc_headers").jq('."Content-Type"'))
        .where(
            (daft.col("WARC-Type") == "response")
            & (daft.col("Content-Length") < 10 * 1024 * 1024)
            & (daft.col("WARC-Identified-Payload-Type").is_in(['"application/http; msgtype=response"']))
        )
        .count_rows()
    )
    assert num_rows == 11


# From the WARC spec:
# > All ‘response’, ‘resource’, ‘request’, ‘revisit’, ‘conversion’ and ‘continuation’ records shall have
# > a WARC- Target-URI field. A ‘metadata’ record may have a WARC-Target-URI field. A ‘warcinfo’ record
# > shall not have a WARC-Target-URI field.
#
# For more details, see: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-target-uri
def test_warc_target_uri_column():
    path = "tests/assets/example.warc"
    df = daft.read_warc(path)

    schema_fields = {field.name: field.dtype for field in df.schema()}
    assert "WARC-Target-URI" in schema_fields
    assert str(schema_fields["WARC-Target-URI"]) == "String"

    # Test that warcinfo records have null WARC-Target-URI.
    warcinfo_records = df.filter(df["WARC-Type"] == "warcinfo")
    assert warcinfo_records.count_rows() == 1
    warcinfo_with_null_uri = warcinfo_records.filter(df["WARC-Target-URI"].is_null())
    assert warcinfo_with_null_uri.count_rows() == 1

    # Test that request records have non-null WARC-Target-URI.
    request_records = df.filter(df["WARC-Type"] == "request")
    request_count = request_records.count_rows()
    assert request_count == 17
    request_with_uri = request_records.filter(df["WARC-Target-URI"].not_null())
    assert request_with_uri.count_rows() == request_count

    # Test that response records have non-null WARC-Target-URI.
    response_records = df.filter(df["WARC-Type"] == "response")
    response_count = response_records.count_rows()
    assert response_count == 11
    response_with_uri = response_records.filter(df["WARC-Target-URI"].not_null())
    assert response_with_uri.count_rows() == response_count

    # Test that metadata records optionally have non-null WARC-Target-URI.
    metadata_records = df.filter(df["WARC-Type"] == "metadata")
    metadata_count = metadata_records.count_rows()
    assert metadata_count == 1
    metadata_with_uri = metadata_records.filter(df["WARC-Target-URI"].not_null())
    assert metadata_with_uri.count_rows() <= metadata_count

    # Test that records contain valid target URI domain names.
    eventualcomputing_count = df.filter(
        daft.functions.contains(df["WARC-Target-URI"], "eventualcomputing.com")
    ).count_rows()
    assert eventualcomputing_count > 0
