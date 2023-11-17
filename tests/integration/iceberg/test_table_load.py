from __future__ import annotations

import pytest

pyiceberg = pytest.importorskip("pyiceberg")
from pyiceberg.io.pyarrow import schema_to_pyarrow

import daft
from daft.logical.schema import Schema
from tests.conftest import assert_df_equals


@pytest.mark.integration()
def test_daft_iceberg_table_open(local_iceberg_tables):
    df = daft.read_iceberg(local_iceberg_tables)
    iceberg_schema = local_iceberg_tables.schema()
    as_pyarrow_schema = schema_to_pyarrow(iceberg_schema)
    as_daft_schema = Schema.from_pyarrow_schema(as_pyarrow_schema)
    assert df.schema() == as_daft_schema


WORKING_SHOW_COLLECT = [
    # "test_all_types", # ValueError: DaftError::ArrowError Not yet implemented: Deserializing type Decimal(10, 2) from parquet
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
    # "test_positional_mor_deletes", # Need Merge on Read
    # "test_positional_mor_double_deletes", # Need Merge on Read
    # "test_table_sanitized_character", # Bug in scan().to_arrow().to_arrow()
    # "test_table_version", # we have bugs when loading no files
    "test_uuid_and_fixed_unpartitioned",
]


@pytest.mark.integration()
@pytest.mark.parametrize("table_name", WORKING_SHOW_COLLECT)
def test_daft_iceberg_table_show(table_name, local_iceberg_catalog):
    tab = local_iceberg_catalog.load_table(f"default.{table_name}")
    df = daft.read_iceberg(tab)
    df.show()


@pytest.mark.integration()
@pytest.mark.parametrize("table_name", WORKING_SHOW_COLLECT)
def test_daft_iceberg_table_collect_correct(table_name, local_iceberg_catalog):
    tab = local_iceberg_catalog.load_table(f"default.{table_name}")
    df = daft.read_iceberg(tab)
    df.collect()
    daft_pandas = df.to_pandas()
    iceberg_pandas = tab.scan().to_arrow().to_pandas()
    assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])
