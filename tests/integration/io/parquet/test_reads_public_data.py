from __future__ import annotations

import os
import tempfile

import pandas as pd
import pyarrow as pa
import pytest
import requests
from pyarrow import parquet as pq

import daft
from daft.filesystem import get_filesystem_from_path, get_protocol_from_path
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
    # For some reason the program segfaults with this file unless we make the chunk size > 2024
    # (
    #     "parquet-testing/data/overflow_i16_page_cnt.parquet",
    #     "https://raw.githubusercontent.com/apache/parquet-testing/master/data/overflow_i16_page_cnt.parquet",
    # ),
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
    (
        "azure/mvp",
        "az://public-anonymous/mvp.parquet",
    ),
    (
        "gcs/mvp",
        "gs://daft-public-data-gs/mvp.parquet",
    ),
    (
        "daft/schema_with_metadata",
        "tests/assets/parquet-data/parquet-with-schema-metadata.parquet",
    ),
]


@pytest.fixture(scope="session")
def public_storage_io_config() -> daft.io.IOConfig:
    return daft.io.IOConfig(
        azure=daft.io.AzureConfig(storage_account="dafttestdata", anonymous=True),
        s3=daft.io.S3Config(region_name="us-west-2", anonymous=True),
        gcs=daft.io.GCSConfig(anonymous=True),
    )


@pytest.fixture(scope="session", params=DAFT_CAN_READ_FILES, ids=[name for name, _ in DAFT_CAN_READ_FILES])
def parquet_file(request) -> tuple[str, str]:
    """Returns a tuple of (`name`, `url`) of files that Daft should be able to handle. URLs may be HTTPs or S3."""
    return request.param


HTTP_CAN_READ_FILES = [param for param in DAFT_CAN_READ_FILES if param[1].startswith("http")]


@pytest.fixture(scope="session", params=HTTP_CAN_READ_FILES, ids=[name for name, _ in HTTP_CAN_READ_FILES])
def local_parquet_file(request) -> tuple[str, str]:
    """Returns a tuple of (`name`, `url`) of files that Daft should be able to handle. URLs input are remote but will return local paths."""
    name, url = request.param
    with tempfile.TemporaryDirectory() as dir:
        file = os.path.join(dir, "tempfile")
        response = requests.get(url)
        with open(file, "wb") as f:
            f.write(response.content)

        yield (name, file)


def read_parquet_with_pyarrow(path) -> pa.Table:
    kwargs = {}
    if get_protocol_from_path(path) == "s3":
        kwargs["anon"] = True
    if get_protocol_from_path(path) == "az":
        kwargs["account_name"] = "dafttestdata"
        kwargs["anon"] = True

    fs = get_filesystem_from_path(path, **kwargs)
    table = pq.read_table(path, filesystem=fs)
    return table


@pytest.mark.integration()
@pytest.mark.parametrize(
    "multithreaded_io",
    [False, True],
)
def test_parquet_read_table(parquet_file, public_storage_io_config, multithreaded_io):
    _, url = parquet_file
    daft_native_read = Table.read_parquet(url, io_config=public_storage_io_config, multithreaded_io=multithreaded_io)
    pa_read = Table.from_arrow(read_parquet_with_pyarrow(url))
    assert daft_native_read.schema() == pa_read.schema()
    pd.testing.assert_frame_equal(daft_native_read.to_pandas(), pa_read.to_pandas())


@pytest.mark.integration()
@pytest.mark.parametrize(
    "multithreaded_io",
    [False, True],
)
def test_parquet_read_table_local(local_parquet_file, public_storage_io_config, multithreaded_io):
    _, url = local_parquet_file
    daft_native_read = Table.read_parquet(url, io_config=public_storage_io_config, multithreaded_io=multithreaded_io)
    pa_read = Table.from_arrow(read_parquet_with_pyarrow(url))
    assert daft_native_read.schema() == pa_read.schema()
    pd.testing.assert_frame_equal(daft_native_read.to_pandas(), pa_read.to_pandas())


@pytest.mark.integration()
@pytest.mark.parametrize(
    "multithreaded_io",
    [False, True],
)
def test_parquet_read_table_into_pyarrow(parquet_file, public_storage_io_config, multithreaded_io):
    _, url = parquet_file
    daft_native_read = daft.table.read_parquet_into_pyarrow(
        url, io_config=public_storage_io_config, multithreaded_io=multithreaded_io
    )
    pa_read = read_parquet_with_pyarrow(url)
    assert daft_native_read.schema == pa_read.schema
    assert pa_read.schema.metadata is None or daft_native_read.schema.metadata == pa_read.schema.metadata
    pd.testing.assert_frame_equal(daft_native_read.to_pandas(), pa_read.to_pandas())


@pytest.mark.integration()
@pytest.mark.skipif(
    daft.context.get_context().use_rust_planner, reason="Custom fsspec filesystems not supported in new query planner"
)
@pytest.mark.parametrize(
    "multithreaded_io",
    [False, True],
)
def test_parquet_read_table_bulk(parquet_file, public_storage_io_config, multithreaded_io):
    _, url = parquet_file
    daft_native_reads = Table.read_parquet_bulk(
        [url] * 2, io_config=public_storage_io_config, multithreaded_io=multithreaded_io
    )
    pa_read = Table.from_arrow(read_parquet_with_pyarrow(url))

    for daft_native_read in daft_native_reads:
        assert daft_native_read.schema() == pa_read.schema()
        pd.testing.assert_frame_equal(daft_native_read.to_pandas(), pa_read.to_pandas())


@pytest.mark.integration()
@pytest.mark.skipif(
    daft.context.get_context().use_rust_planner, reason="Custom fsspec filesystems not supported in new query planner"
)
@pytest.mark.parametrize(
    "multithreaded_io",
    [False, True],
)
def test_parquet_into_pyarrow_bulk(parquet_file, public_storage_io_config, multithreaded_io):
    _, url = parquet_file
    daft_native_reads = daft.table.read_parquet_into_pyarrow_bulk(
        [url] * 2, io_config=public_storage_io_config, multithreaded_io=multithreaded_io
    )
    pa_read = read_parquet_with_pyarrow(url)

    for daft_native_read in daft_native_reads:
        assert daft_native_read.schema == pa_read.schema
        pd.testing.assert_frame_equal(daft_native_read.to_pandas(), pa_read.to_pandas())


@pytest.mark.integration()
def test_parquet_read_df(parquet_file, public_storage_io_config):
    _, url = parquet_file
    # This is a hack until we remove `fsspec.info`, `fsspec.glob` and `fsspec.glob` from  `daft.read_parquet`.
    # We rely on the native downloaders impl for that
    if url.startswith("az"):
        import adlfs

        fs = adlfs.AzureBlobFileSystem(account_name="dafttestdata", anon=True)
    else:
        fs = None
    daft_native_read = daft.read_parquet(url, io_config=public_storage_io_config, use_native_downloader=True, fs=fs)
    pa_read = Table.from_arrow(read_parquet_with_pyarrow(url))
    assert daft_native_read.schema() == pa_read.schema()
    pd.testing.assert_frame_equal(daft_native_read.to_pandas(), pa_read.to_pandas())


@pytest.mark.integration()
@pytest.mark.parametrize(
    "multithreaded_io",
    [False, True],
)
def test_row_groups_selection(public_storage_io_config, multithreaded_io):
    url = "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet"
    all_rows = Table.read_parquet(url, io_config=public_storage_io_config, multithreaded_io=multithreaded_io)
    assert len(all_rows) == 100
    first = Table.read_parquet(
        url, io_config=public_storage_io_config, multithreaded_io=multithreaded_io, row_groups=[0]
    )
    assert len(first) == 10
    assert all_rows.to_arrow()[:10] == first.to_arrow()

    fifth = Table.read_parquet(
        url, io_config=public_storage_io_config, multithreaded_io=multithreaded_io, row_groups=[5]
    )
    assert len(fifth) == 10
    assert all_rows.to_arrow()[50:60] == fifth.to_arrow()

    repeated = Table.read_parquet(
        url, io_config=public_storage_io_config, multithreaded_io=multithreaded_io, row_groups=[1, 1, 1]
    )
    assert len(repeated) == 30
    assert all_rows.to_arrow()[10:20] == repeated.to_arrow()[:10]
    assert all_rows.to_arrow()[10:20] == repeated.to_arrow()[10:20]
    assert all_rows.to_arrow()[10:20] == repeated.to_arrow()[20:]

    out_of_order = Table.read_parquet(
        url, io_config=public_storage_io_config, multithreaded_io=multithreaded_io, row_groups=[1, 0]
    )
    assert len(out_of_order) == 20
    assert all_rows.to_arrow()[10:20] == out_of_order.to_arrow()[:10]
    assert all_rows.to_arrow()[0:10] == out_of_order.to_arrow()[10:20]


@pytest.mark.integration()
@pytest.mark.parametrize(
    "multithreaded_io",
    [False, True],
)
def test_row_groups_selection_local(public_storage_io_config, multithreaded_io):
    url = "tests/assets/parquet-data/mvp.parquet"
    all_rows = Table.read_parquet(url, io_config=public_storage_io_config, multithreaded_io=multithreaded_io)
    assert len(all_rows) == 100
    first = Table.read_parquet(
        url, io_config=public_storage_io_config, multithreaded_io=multithreaded_io, row_groups=[0]
    )
    assert len(first) == 10
    assert all_rows.to_arrow()[:10] == first.to_arrow()

    fifth = Table.read_parquet(
        url, io_config=public_storage_io_config, multithreaded_io=multithreaded_io, row_groups=[5]
    )
    assert len(fifth) == 10
    assert all_rows.to_arrow()[50:60] == fifth.to_arrow()

    repeated = Table.read_parquet(
        url, io_config=public_storage_io_config, multithreaded_io=multithreaded_io, row_groups=[1, 1, 1]
    )
    assert len(repeated) == 30
    assert all_rows.to_arrow()[10:20] == repeated.to_arrow()[:10]
    assert all_rows.to_arrow()[10:20] == repeated.to_arrow()[10:20]
    assert all_rows.to_arrow()[10:20] == repeated.to_arrow()[20:]

    out_of_order = Table.read_parquet(
        url, io_config=public_storage_io_config, multithreaded_io=multithreaded_io, row_groups=[1, 0]
    )
    assert len(out_of_order) == 20
    assert all_rows.to_arrow()[10:20] == out_of_order.to_arrow()[:10]
    assert all_rows.to_arrow()[0:10] == out_of_order.to_arrow()[10:20]


@pytest.mark.integration()
@pytest.mark.parametrize(
    "multithreaded_io",
    [False, True],
)
def test_row_groups_selection_bulk(public_storage_io_config, multithreaded_io):
    url = ["s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet"] * 11
    row_groups = [list(range(10))] + [[i] for i in range(10)]
    first, *rest = Table.read_parquet_bulk(
        url, io_config=public_storage_io_config, multithreaded_io=multithreaded_io, row_groups_per_path=row_groups
    )
    assert len(first) == 100
    assert len(rest) == 10

    for i, t in enumerate(rest):
        assert len(t) == 10
        assert first.to_arrow()[i * 10 : (i + 1) * 10] == t.to_arrow()


@pytest.mark.integration()
@pytest.mark.parametrize(
    "multithreaded_io",
    [False, True],
)
def test_row_groups_selection_into_pyarrow_bulk(public_storage_io_config, multithreaded_io):
    url = ["s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet"] * 11
    row_groups = [list(range(10))] + [[i] for i in range(10)]
    first, *rest = daft.table.read_parquet_into_pyarrow_bulk(
        url, io_config=public_storage_io_config, multithreaded_io=multithreaded_io, row_groups_per_path=row_groups
    )
    assert len(first) == 100
    assert len(rest) == 10

    for i, t in enumerate(rest):
        assert len(t) == 10
        assert first[i * 10 : (i + 1) * 10] == t
