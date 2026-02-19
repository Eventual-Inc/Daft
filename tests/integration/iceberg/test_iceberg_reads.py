from __future__ import annotations

import datetime

import pytest

pyiceberg = pytest.importorskip("pyiceberg")

import pyarrow.compute as pc
from pyiceberg.io.pyarrow import schema_to_pyarrow

import daft
from daft.logical.schema import Schema
from tests.conftest import assert_df_equals


@pytest.mark.integration()
def test_daft_iceberg_table_open(local_iceberg_tables, local_iceberg_catalog):
    catalog_name, pyiceberg_catalog = local_iceberg_catalog
    tab = pyiceberg_catalog.load_table(local_iceberg_tables)
    df = daft.read_table(f"{catalog_name}.{local_iceberg_tables}")
    iceberg_schema = tab.schema()
    as_pyarrow_schema = schema_to_pyarrow(iceberg_schema)
    as_daft_schema = Schema.from_pyarrow_schema(as_pyarrow_schema)
    assert df.schema() == as_daft_schema


WORKING_SHOW_COLLECT = [
    "test_all_types",  # Commented out due to issue https://github.com/Eventual-Inc/Daft/issues/2996
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
    "test_table_version",  # we have bugs when loading no files
    "test_uuid_and_fixed_unpartitioned",
    "test_add_new_column",
    "test_new_column_with_no_data",
    "test_table_rename",
    "test_overlapping_deletes",
    "test_mixed_delete_types",
    # Partition evolution currently not supported, see issue: https://github.com/Eventual-Inc/Daft/issues/2249
    # "test_evolve_partitioning",
]

SORT_KEYS = {
    "test_all_types": [],
    "test_limit": [],
    "test_null_nan": [],
    "test_null_nan_rewritten": [],
    "test_partitioned_by_bucket": ["number"],
    "test_partitioned_by_days": ["number"],
    "test_partitioned_by_hours": ["number"],
    "test_partitioned_by_identity": ["number"],
    "test_partitioned_by_months": ["number"],
    "test_partitioned_by_truncate": ["number"],
    "test_partitioned_by_years": ["number"],
    "test_positional_mor_deletes": ["number"],
    "test_positional_mor_double_deletes": ["number"],
    "test_table_sanitized_character": [],
    "test_table_version": [],
    "test_uuid_and_fixed_unpartitioned": ["fixed_col"],
    "test_add_new_column": ["idx"],
    "test_new_column_with_no_data": [],
    "test_table_rename": [],
    "test_overlapping_deletes": [],
    "test_mixed_delete_types": [],
}


@pytest.mark.integration()
@pytest.mark.parametrize("table_name", WORKING_SHOW_COLLECT)
def test_daft_iceberg_table_show(table_name, local_iceberg_catalog):
    catalog_name, _ = local_iceberg_catalog
    df = daft.read_table(f"{catalog_name}.default.{table_name}")
    df.show()


@pytest.mark.integration()
@pytest.mark.parametrize(
    "table_name, sort_key", [(table_name, SORT_KEYS[table_name]) for table_name in WORKING_SHOW_COLLECT]
)
def test_daft_iceberg_table_collect_correct(table_name, sort_key, local_iceberg_catalog):
    catalog_name, pyiceberg_catalog = local_iceberg_catalog
    tab = pyiceberg_catalog.load_table(f"default.{table_name}")
    df = daft.read_table(f"{catalog_name}.default.{table_name}")
    df.collect()
    daft_pandas = df.to_pandas()
    iceberg_pandas = tab.scan().to_arrow().to_pandas()
    assert_df_equals(daft_pandas, iceberg_pandas, sort_key=sort_key)


@pytest.mark.integration()
def test_daft_iceberg_table_renamed_filtered_collect_correct(local_iceberg_catalog):
    catalog_name, pyiceberg_catalog = local_iceberg_catalog
    tab = pyiceberg_catalog.load_table("default.test_table_rename")
    df = daft.read_table(f"{catalog_name}.default.test_table_rename")
    df = df.where(df["idx_renamed"] <= 1)
    daft_pandas = df.to_pandas()
    iceberg_pandas = tab.scan().to_arrow().to_pandas()
    iceberg_pandas = iceberg_pandas[iceberg_pandas["idx_renamed"] <= 1]
    assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])


@pytest.mark.integration()
def test_daft_iceberg_table_renamed_column_pushdown_collect_correct(local_iceberg_catalog):
    catalog_name, pyiceberg_catalog = local_iceberg_catalog
    tab = pyiceberg_catalog.load_table("default.test_table_rename")
    df = daft.read_table(f"{catalog_name}.default.test_table_rename")
    df = df.select("idx_renamed")
    daft_pandas = df.to_pandas()
    iceberg_pandas = tab.scan().to_arrow().to_pandas()
    iceberg_pandas = iceberg_pandas[["idx_renamed"]]
    assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])


@pytest.mark.integration()
def test_daft_iceberg_table_read_partition_column_identity(local_iceberg_catalog):
    catalog_name, pyiceberg_catalog = local_iceberg_catalog
    tab = pyiceberg_catalog.load_table("default.test_partitioned_by_identity")
    df = daft.read_table(f"{catalog_name}.default.test_partitioned_by_identity")
    df = df.select("ts", "number")
    daft_pandas = df.to_pandas()
    iceberg_pandas = tab.scan().to_arrow().to_pandas()
    iceberg_pandas = iceberg_pandas[["ts", "number"]]
    assert_df_equals(daft_pandas, iceberg_pandas, sort_key=["ts", "number"])


@pytest.mark.integration()
def test_daft_iceberg_table_read_partition_column_identity_filter(local_iceberg_catalog):
    catalog_name, pyiceberg_catalog = local_iceberg_catalog
    tab = pyiceberg_catalog.load_table("default.test_partitioned_by_identity")
    df = daft.read_table(f"{catalog_name}.default.test_partitioned_by_identity")
    df = df.where(df["number"] > 0)
    df = df.select("ts")
    daft_pandas = df.to_pandas()
    iceberg_pandas = tab.scan().to_arrow().to_pandas()
    iceberg_pandas = iceberg_pandas[iceberg_pandas["number"] > 0][["ts"]]
    assert_df_equals(daft_pandas, iceberg_pandas, sort_key=["ts"])


@pytest.mark.skip(
    reason="Selecting just the identity-transformed partition key in an iceberg table is not yet supported. "
    "Issue: https://github.com/Eventual-Inc/Daft/issues/2129"
)
@pytest.mark.integration()
def test_daft_iceberg_table_read_partition_column_identity_filter_on_partkey(local_iceberg_catalog):
    catalog_name, pyiceberg_catalog = local_iceberg_catalog
    tab = pyiceberg_catalog.load_table("default.test_partitioned_by_identity")
    df = daft.read_table(f"{catalog_name}.default.test_partitioned_by_identity")
    df = df.select("ts")
    df = df.where(df["ts"] > datetime.date(2022, 3, 1))
    daft_pandas = df.to_pandas()
    iceberg_pandas = tab.scan().to_arrow().to_pandas()
    iceberg_pandas = iceberg_pandas[iceberg_pandas["ts"] > datetime.date(2022, 3, 1)][["ts"]]
    assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])


@pytest.mark.skip(
    reason="Selecting just the identity-transformed partition key in an iceberg table is not yet supported. "
    "Issue: https://github.com/Eventual-Inc/Daft/issues/2129"
)
@pytest.mark.integration()
def test_daft_iceberg_table_read_partition_column_identity_only(local_iceberg_catalog):
    catalog_name, pyiceberg_catalog = local_iceberg_catalog
    tab = pyiceberg_catalog.load_table("default.test_partitioned_by_identity")
    df = daft.read_table(f"{catalog_name}.default.test_partitioned_by_identity")
    df = df.select("ts")
    daft_pandas = df.to_pandas()
    iceberg_pandas = tab.scan().to_arrow().to_pandas()
    iceberg_pandas = iceberg_pandas[["ts"]]
    assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])


@pytest.mark.integration()
def test_daft_iceberg_table_read_partition_column_transformed(local_iceberg_catalog):
    catalog_name, pyiceberg_catalog = local_iceberg_catalog
    tab = pyiceberg_catalog.load_table("default.test_partitioned_by_bucket")
    df = daft.read_table(f"{catalog_name}.default.test_partitioned_by_bucket")
    df = df.select("number")
    daft_pandas = df.to_pandas()
    iceberg_pandas = tab.scan().to_arrow().to_pandas()
    iceberg_pandas = iceberg_pandas[["number"]]
    assert_df_equals(daft_pandas, iceberg_pandas, sort_key=["number"])


@pytest.mark.integration()
def test_daft_iceberg_table_read_table_snapshot(local_iceberg_catalog):
    _catalog_name, pyiceberg_catalog = local_iceberg_catalog
    tab = pyiceberg_catalog.load_table("default.test_snapshotting")

    snapshots = tab.history()
    assert len(snapshots) == 2

    for snapshot in snapshots:
        # TODO: Provide read_table API for reading iceberg with snapshot ID
        daft_pandas = daft.read_iceberg(tab, snapshot_id=snapshot.snapshot_id).to_pandas()
        iceberg_pandas = tab.scan(snapshot_id=snapshot.snapshot_id).to_pandas()
        assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])


@pytest.mark.integration()
@pytest.mark.parametrize("table_name", ["test_positional_mor_deletes", "test_positional_mor_double_deletes"])
def test_daft_iceberg_table_mor_limit_collect_correct(table_name, local_iceberg_catalog):
    catalog_name, pyiceberg_catalog = local_iceberg_catalog
    tab = pyiceberg_catalog.load_table(f"default.{table_name}")
    df = daft.read_table(f"{catalog_name}.default.{table_name}")
    df = df.limit(10)
    df.collect()
    daft_pandas = df.to_pandas()

    iceberg_arrow = tab.scan().to_arrow()
    iceberg_arrow = iceberg_arrow.slice(length=10)
    iceberg_pandas = iceberg_arrow.to_pandas()

    assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])


@pytest.mark.integration()
@pytest.mark.parametrize("table_name", ["test_positional_mor_deletes", "test_positional_mor_double_deletes"])
def test_daft_iceberg_table_mor_predicate_collect_correct(table_name, local_iceberg_catalog):
    catalog_name, pyiceberg_catalog = local_iceberg_catalog
    tab = pyiceberg_catalog.load_table(f"default.{table_name}")
    df = daft.read_table(f"{catalog_name}.default.{table_name}")
    df = df.where(df["number"] > 5)
    df.collect()
    daft_pandas = df.to_pandas()

    iceberg_arrow = tab.scan().to_arrow()
    iceberg_arrow = iceberg_arrow.filter(pc.field("number") > 5)
    iceberg_pandas = iceberg_arrow.to_pandas()

    assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])


class TestIcebergCountPushdown:
    """Test suite for Iceberg Count pushdown optimization."""

    @pytest.mark.integration()
    @pytest.mark.parametrize(
        "table_name",
        [
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
            "test_table_sanitized_character",
            "test_table_version",
            "test_uuid_and_fixed_unpartitioned",
            "test_add_new_column",
            "test_new_column_with_no_data",
            "test_table_rename",
        ],
    )
    def test_count_pushdown_basic(self, table_name, local_iceberg_catalog, capsys):
        """Test count pushdown on partitioned tables."""
        catalog_name, pyiceberg_catalog = local_iceberg_catalog
        tab = pyiceberg_catalog.load_table(f"default.{table_name}")

        # Test Daft count with pushdown on partitioned table
        df = daft.read_table(f"{catalog_name}.default.{table_name}").count()
        _ = capsys.readouterr()
        df.explain(True)
        actual = capsys.readouterr()
        assert "daft.io.iceberg.iceberg_scan:_iceberg_count_result_function" in actual.out

        daft_count = df.collect().to_pydict()["count"][0]

        # Compare with PyIceberg count
        iceberg_count = len(tab.scan().to_arrow())

        assert daft_count == iceberg_count

    @pytest.mark.integration()
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_positional_mor_deletes",
            "test_positional_mor_double_deletes",
            "test_overlapping_deletes",
            "test_mixed_delete_types",
        ],
    )
    def test_count_pushdown_with_delete_files(self, table_name, local_iceberg_catalog, capsys):
        """Test count with delete files, should not use pushdown optimization."""
        catalog_name, pyiceberg_catalog = local_iceberg_catalog
        tab = pyiceberg_catalog.load_table(f"default.{table_name}")

        # Test Daft count with pushdown on partitioned table
        df = daft.read_table(f"{catalog_name}.default.{table_name}").count()
        _ = capsys.readouterr()
        df.explain(True)
        actual = capsys.readouterr()
        assert "daft.io.iceberg.iceberg_scan:_iceberg_count_result_function" not in actual.out

        daft_count = df.collect().to_pydict()["count"][0]

        # Compare with PyIceberg count
        iceberg_count = len(tab.scan().to_arrow())

        assert daft_count == iceberg_count

    @pytest.mark.integration()
    def test_count_pushdown_with_filter(self, local_iceberg_catalog, capsys):
        """Test count with filter - should not use pushdown optimization."""
        catalog_name, pyiceberg_catalog = local_iceberg_catalog
        tab = pyiceberg_catalog.load_table("default.test_partitioned_by_identity")

        # Test Daft count with filter (should not use pushdown)
        df = daft.read_table(f"{catalog_name}.default.test_partitioned_by_identity")
        df = df.where(df["number"] > 0).count()

        _ = capsys.readouterr()
        df.explain(True)
        actual = capsys.readouterr()
        assert "daft.io.iceberg.iceberg_scan:_iceberg_count_result_function" not in actual.out

        daft_count = df.collect().to_pydict()["count"][0]

        # Compare with PyIceberg filtered count
        iceberg_pandas = tab.scan().to_arrow().to_pandas()
        iceberg_count = len(iceberg_pandas[iceberg_pandas["number"] > 0])

        assert daft_count == iceberg_count

    @pytest.mark.integration()
    def test_count_pushdown_with_column_selection(self, local_iceberg_catalog, capsys):
        """Test count pushdown works correctly with column selection."""
        catalog_name, pyiceberg_catalog = local_iceberg_catalog
        tab = pyiceberg_catalog.load_table("default.test_all_types")

        # Test count with column selection (should still use pushdown)
        df = daft.read_table(f"{catalog_name}.default.test_all_types")
        df = df.select("id").count() if "id" in df.column_names else df.select(df.column_names[0]).count()

        _ = capsys.readouterr()
        df.explain(True)
        actual = capsys.readouterr()
        assert "daft.io.iceberg.iceberg_scan:_iceberg_count_result_function" in actual.out

        daft_count = df.collect().to_pydict()["count"][0]

        # Compare with full table count (should be the same)
        iceberg_count = len(tab.scan().to_arrow())

        assert daft_count == iceberg_count

    @pytest.mark.integration()
    def test_count_pushdown_with_limit(self, local_iceberg_catalog, capsys):
        """Test count behavior with limit operations."""
        catalog_name, _pyiceberg_catalog = local_iceberg_catalog

        # Test count after limit (should not use pushdown optimization)
        df = daft.read_table(f"{catalog_name}.default.test_all_types")
        df = df.limit(5).count()
        daft_count = df.collect().to_pydict()["count"][0]

        _ = capsys.readouterr()
        df.explain(True)
        actual = capsys.readouterr()
        assert "daft.io.iceberg.iceberg_scan:_iceberg_count_result_function" not in actual.out

        # Count after limit should be at most the limit value
        assert daft_count <= 5
        assert daft_count >= 0

    @pytest.mark.integration()
    def test_count_pushdown_snapshot_consistency(self, local_iceberg_catalog, capsys):
        """Test count pushdown with different snapshots."""
        _catalog_name, pyiceberg_catalog = local_iceberg_catalog

        try:
            tab = pyiceberg_catalog.load_table("default.test_snapshotting")
            snapshots = tab.history()

            if len(snapshots) >= 2:
                # Test count on different snapshots
                for snapshot in snapshots[:2]:  # Test first 2 snapshots
                    daft_pandas = daft.read_iceberg(tab, snapshot_id=snapshot.snapshot_id)

                    df = daft_pandas.count()
                    _ = capsys.readouterr()
                    df.explain(True)
                    actual = capsys.readouterr()
                    assert "daft.io.iceberg.iceberg_scan:_iceberg_count_result_function" not in actual.out

                    daft_count = df.collect().to_pydict()["count"][0]

                    # Compare with PyIceberg snapshot count
                    iceberg_count = len(tab.scan(snapshot_id=snapshot.snapshot_id).to_arrow())

                    assert daft_count == iceberg_count
        except Exception:
            # If snapshotting table doesn't exist, skip this test
            pytest.skip("Snapshot test requires test_snapshotting table")
