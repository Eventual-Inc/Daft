from __future__ import annotations

import pytest
from pyiceberg.io.pyarrow import schema_to_pyarrow

import daft
from daft.logical.schema import Schema


@pytest.mark.integration()
def test_daft_iceberg_table_open(local_iceberg_tables):
    df = daft.read_iceberg(local_iceberg_tables)
    iceberg_schema = local_iceberg_tables.schema()
    as_pyarrow_schema = schema_to_pyarrow(iceberg_schema)
    as_daft_schema = Schema.from_pyarrow_schema(as_pyarrow_schema)
    assert df.schema() == as_daft_schema


WORKING_SHOW_COLLECT = [
    "test_all_types",
    "test_limit",
    "test_null_nan",
    "test_null_nan_rewritten",
    "test_partitioned_by_bucket",
    "test_partitioned_by_days",
    "test_partitioned_by_hours",
    "test_partitioned_by_identity",
    "test_partitioned_by_months",
    "test_partitioned_by_truncate",
    "test_partitioned_by_years",
    "test_positional_mor_deletes",
    "test_positional_mor_double_deletes",
    "test_table_sanitized_character",
    "test_table_version",
    "test_uuid_and_fixed_unpartitioned",
]


@pytest.mark.integration()
def test_daft_iceberg_table_show(local_iceberg_tables):
    df = daft.read_iceberg(local_iceberg_tables)
    df.show()
