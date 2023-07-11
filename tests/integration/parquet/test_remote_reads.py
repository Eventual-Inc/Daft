from __future__ import annotations

import pytest

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
    (
        "parquet-testing/data/dict-page-offset-zero.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/dict-page-offset-zero.parquet",
    ),
    (
        "parquet-testing/data/fixed_length_byte_array.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/fixed_length_byte_array.parquet",
    ),
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
    (
        "parquet-testing/data/nation.dict-malformed.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/nation.dict-malformed.parquet",
    ),
    (
        "parquet-testing/data/nested_lists.snappy.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/nested_lists.snappy.parquet",
    ),
    (
        "parquet-testing/data/nested_structs.rust.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/nested_structs.rust.parquet",
    ),
    (
        "parquet-testing/data/nonnullable.impala.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/nonnullable.impala.parquet",
    ),
    (
        "parquet-testing/data/null_list.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/null_list.parquet",
    ),
    (
        "parquet-testing/data/nullable.impala.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/nullable.impala.parquet",
    ),
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
    (
        "parquet-testing/data/repeated_no_annotation.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/repeated_no_annotation.parquet",
    ),
    (
        "parquet-testing/data/rle-dict-snappy-checksum.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/rle-dict-snappy-checksum.parquet",
    ),
    (
        "parquet-testing/data/rle_boolean_encoding.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/rle_boolean_encoding.parquet",
    ),
    (
        "parquet-testing/data/single_nan.parquet",
        "https://raw.githubusercontent.com/apache/parquet-testing/master/data/single_nan.parquet",
    ),
    (
        "daft-tpch/100g_32part",
        "s3://eventual-dev-benchmarking-fixtures/uncompressed/tpch-dbgen/100_0/32/parquet/lineitem/108417bd-5bee-43d9-bf9a-d6faec6afb2d-0.parquet",
    ),
    (
        "parquet-benchmarking/mvp",
        "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet",
    ),
]


@pytest.fixture(scope="session", params=DAFT_CAN_READ_FILES)
def parquet_file(request) -> tuple[str, str]:
    """Returns a tuple of (`name`, `url`) of files that Daft should be able to handle. URLs may be HTTPs or S3."""
    return request.param


# @pytest.mark.integration()
def test_parquet_read(parquet_file):
    name, url = parquet_file
    print(name)
    Table.read_parquet(url)
    # Test Daft reads
    # df = daft.read_parquet(url)
    # df.collect()
