from __future__ import annotations

import pandas as pd
import pyarrow as pa
import pytest
from pyarrow import parquet as pq

import daft
from daft.filesystem import get_filesystem_from_path, get_protocol_from_path
from daft.io import IOConfig, S3Config
from daft.table import Table

# Taken from our spreadsheet of files that Daft should be able to handle
DAFT_CAN_READ_FILES = [
    (
        "parquet-testing/data/alltypes_dictionary.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/alltypes_dictionary.parquet",
    ),
    (
        "parquet-testing/data/alltypes_plain.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/alltypes_plain.parquet",
    ),
    (
        "parquet-testing/data/alltypes_plain.snappy.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/alltypes_plain.snappy.parquet",
    ),
    (
        "parquet-testing/data/alltypes_tiny_pages.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/alltypes_tiny_pages.parquet",
    ),
    (
        "parquet-testing/data/alltypes_tiny_pages_plain.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/alltypes_tiny_pages_plain.parquet",
    ),
    (
        "parquet-testing/data/binary.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/binary.parquet",
    ),
    # Needs Decimals decoding from byte arrays
    (
        "parquet-testing/data/byte_array_decimal.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/byte_array_decimal.parquet",
    ),
    (
        "parquet-testing/data/data_index_bloom_encoding_stats.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/data_index_bloom_encoding_stats.parquet",
    ),
    (
        "parquet-testing/data/datapage_v1-snappy-compressed-checksum.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/datapage_v1-snappy-compressed-checksum.parquet",
    ),
    (
        "parquet-testing/data/datapage_v1-uncompressed-checksum.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/datapage_v1-uncompressed-checksum.parquet",
    ),
    # Thrift Error?
    # (
    #     "parquet-testing/data/dict-page-offset-zero.parquet",
    #     "https://raw.githubusercontent.com/apache/parquet-testing/master/data/dict-page-offset-zero.parquet",
    # ),
    # Need Fixed Length Binary in Daft or convert to Variable Sized Binary
    # (
    #     "parquet-testing/data/fixed_length_byte_array.parquet",
    #     "https://raw.githubusercontent.com/apache/parquet-testing/master/data/fixed_length_byte_array.parquet",
    # ),
    (
        "parquet-testing/data/fixed_length_decimal.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/fixed_length_decimal.parquet",
    ),
    (
        "parquet-testing/data/fixed_length_decimal_legacy.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/fixed_length_decimal_legacy.parquet",
    ),
    (
        "parquet-testing/data/int32_decimal.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/int32_decimal.parquet",
    ),
    (
        "parquet-testing/data/int32_with_null_pages.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/int32_with_null_pages.parquet",
    ),
    (
        "parquet-testing/data/int64_decimal.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/int64_decimal.parquet",
    ),
    (
        "parquet-testing/data/list_columns.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/list_columns.parquet",
    ),
    (
        "parquet-testing/data/nan_in_stats.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/nan_in_stats.parquet",
    ),
    # Page Header Wrong Size?
    # (
    #     "parquet-testing/data/nation.dict-malformed.parquet",
    #     "https://raw.githubusercontent.com/apache/parquet-testing/master/data/nation.dict-malformed.parquet",
    # ),
    (
        "parquet-testing/data/nested_lists.snappy.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/nested_lists.snappy.parquet",
    ),
    # We have problems decoding struct objects in our arrow2 decoder
    # (
    #     "parquet-testing/data/nested_structs.rust.parquet",
    #     "https://raw.githubusercontent.com/apache/parquet-testing/master/data/nested_structs.rust.parquet",
    # ),
    # We currently don't support Map Dtypes
    # (
    #     "parquet-testing/data/nonnullable.impala.parquet",
    #     "https://raw.githubusercontent.com/apache/parquet-testing/master/data/nonnullable.impala.parquet",
    # ),
    (
        "parquet-testing/data/null_list.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/null_list.parquet",
    ),
    # We currently don't support Map Dtypes
    # (
    #     "parquet-testing/data/nullable.impala.parquet",
    #     "https://raw.githubusercontent.com/apache/parquet-testing/master/data/nullable.impala.parquet",
    # ),
    (
        "parquet-testing/data/nulls.snappy.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/nulls.snappy.parquet",
    ),
    (
        "parquet-testing/data/overflow_i16_page_cnt.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/overflow_i16_page_cnt.parquet",
    ),
    (
        "parquet-testing/data/plain-dict-uncompressed-checksum.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/plain-dict-uncompressed-checksum.parquet",
    ),
    # We have problems decoding struct objects in our arrow2 decoder
    # (
    #     "parquet-testing/data/repeated_no_annotation.parquet",
    #     "https://raw.githubusercontent.com/apache/parquet-testing/master/data/repeated_no_annotation.parquet",
    # ),
    (
        "parquet-testing/data/rle-dict-snappy-checksum.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/rle-dict-snappy-checksum.parquet",
    ),
    # We currently don't support RLE Boolean encodings
    # (
    #     "parquet-testing/data/rle_boolean_encoding.parquet",
    #     "https://raw.githubusercontent.com/apache/parquet-testing/master/data/rle_boolean_encoding.parquet",
    # ),
    (
        "parquet-testing/data/single_nan.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/single_nan.parquet",
    ),
    # This is currently is in a private s3 bucket
    # (
    #     "daft-tpch/100g_32part",
    #     "s3://eventual-dev-benchmarking-fixtures/uncompressed/tpch-dbgen/100_0/32/parquet/lineitem/108417bd-5bee-43d9-bf9a-d6faec6afb2d-0.parquet",
    # ),
    (
        "parquet-benchmarking/mvp",
        "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet",
    ),
]


@pytest.fixture(scope="session", params=DAFT_CAN_READ_FILES, ids=[name for name, _ in DAFT_CAN_READ_FILES])
def parquet_file(request) -> tuple[str, str]:
    """Returns a tuple of (`name`, `url`) of files that Daft should be able to handle. URLs may be HTTPs or S3."""
    return request.param


def read_parquet_with_pyarrow(path) -> pa.Table:

    kwargs = {}
    if get_protocol_from_path(path) == "s3":
        kwargs["anon"] = True
    fs = get_filesystem_from_path(path, **kwargs)
    table = pq.read_table(path, filesystem=fs)
    return table


@pytest.mark.integration()
def test_parquet_read_table(parquet_file):
    _, url = parquet_file
    daft_native_read = Table.read_parquet(url, io_config=IOConfig(s3=S3Config(anonymous=True)))
    pa_read = Table.from_arrow(read_parquet_with_pyarrow(url))
    assert daft_native_read.schema() == pa_read.schema()
    pd.testing.assert_frame_equal(daft_native_read.to_pandas(), pa_read.to_pandas())


@pytest.mark.integration()
def test_parquet_read_table_bulk(parquet_file):
    _, url = parquet_file
    daft_native_reads = Table.read_parquet_bulk([url] * 2, io_config=IOConfig(s3=S3Config(anonymous=True)))
    pa_read = Table.from_arrow(read_parquet_with_pyarrow(url))

    for daft_native_read in daft_native_reads:
        assert daft_native_read.schema() == pa_read.schema()
        pd.testing.assert_frame_equal(daft_native_read.to_pandas(), pa_read.to_pandas())


@pytest.mark.integration()
def test_parquet_read_df(parquet_file):
    _, url = parquet_file
    daft_native_read = daft.read_parquet(
        url, io_config=IOConfig(s3=S3Config(anonymous=True)), use_native_downloader=True
    )
    pa_read = Table.from_arrow(read_parquet_with_pyarrow(url))
    assert daft_native_read.schema() == pa_read.schema()
    pd.testing.assert_frame_equal(daft_native_read.to_pandas(), pa_read.to_pandas())
