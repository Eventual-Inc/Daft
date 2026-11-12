"""Tests for read_iceberg against a local SqlCatalog.

The existing read coverage lives in tests/integration/iceberg, which is gated behind
`--integration` and a docker-compose REST catalog. These tests exercise the same read
path (snapshot/branch/tag resolution, schema evolution, projection and filter pushdown)
against a temporary SQLite-backed catalog so they run in the default unit test suite.
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import daft
from tests.conftest import get_tests_daft_runner_name

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType


@pytest.fixture
def local_catalog(tmp_path):
    catalog = SqlCatalog(
        "default",
        uri=f"sqlite:///{tmp_path}/pyiceberg_catalog.db",
        warehouse=f"file://{tmp_path}",
    )
    catalog.create_namespace("default")
    yield catalog
    catalog.engine.dispose()


def _metadata_path(table) -> Path:
    """Local filesystem path of the table's current metadata file."""
    parsed = urlparse(table.metadata_location)
    return Path(unquote(parsed.path)) if parsed.scheme == "file" else Path(table.metadata_location)


def _write(table, **columns) -> None:
    daft.from_pydict(columns).write_iceberg(table)
    table.refresh()


def test_read_iceberg_from_table_and_metadata_path(local_catalog):
    """A table object and its metadata path produce the same DataFrame."""
    table = local_catalog.create_table("default.basic", Schema(NestedField(1, "x", LongType(), required=False)))
    _write(table, x=[1, 2, 3])

    from_table = daft.read_iceberg(table)
    from_path = daft.read_iceberg(_metadata_path(table))

    assert from_table.sort("x").to_pydict() == {"x": [1, 2, 3]}
    assert from_path.sort("x").to_pydict() == {"x": [1, 2, 3]}


def test_read_iceberg_empty_table_keeps_schema(local_catalog):
    """An empty table reads back as zero rows, not an error, and keeps its columns."""
    table = local_catalog.create_table("default.empty", Schema(NestedField(1, "x", LongType(), required=False)))

    df = daft.read_iceberg(table)

    assert df.schema().column_names() == ["x"]
    assert df.to_pydict() == {"x": []}


@pytest.fixture
def table_with_refs(local_catalog):
    """Table with two snapshots; the first is also reachable as branch `b1` and tag `t1`."""
    table = local_catalog.create_table("default.refs", Schema(NestedField(1, "x", LongType(), required=False)))
    _write(table, x=[1, 2])

    first_snapshot_id = table.current_snapshot().snapshot_id
    table.manage_snapshots().create_branch(first_snapshot_id, "b1").create_tag(first_snapshot_id, "t1").commit()
    table.refresh()

    _write(table, x=[3, 4])
    return table, first_snapshot_id


@pytest.mark.parametrize("selector", ["snapshot_id", "branch", "tag"])
def test_read_iceberg_snapshot_branch_and_tag(table_with_refs, selector):
    """snapshot_id, branch, and tag all resolve to the same historical snapshot."""
    table, first_snapshot_id = table_with_refs
    kwargs = {"snapshot_id": first_snapshot_id} if selector == "snapshot_id" else {selector: f"{selector[0]}1"}

    assert daft.read_iceberg(table, **kwargs).sort("x").to_pydict() == {"x": [1, 2]}
    # Without a selector the latest snapshot is read.
    assert daft.read_iceberg(table).sort("x").to_pydict() == {"x": [1, 2, 3, 4]}


@pytest.mark.parametrize(
    "kwargs",
    [
        {"snapshot_id": 1, "branch": "b1"},
        {"snapshot_id": 1, "tag": "t1"},
        {"branch": "b1", "tag": "t1"},
    ],
)
def test_read_iceberg_selectors_are_mutually_exclusive(table_with_refs, kwargs):
    table, _ = table_with_refs
    with pytest.raises(ValueError, match="Only one of snapshot_id, branch, or tag may be provided"):
        daft.read_iceberg(table, **kwargs)


@pytest.mark.parametrize("ref_kind", ["branch", "tag"])
def test_read_iceberg_unknown_ref(table_with_refs, ref_kind):
    table, _ = table_with_refs
    with pytest.raises(ValueError, match=f"Iceberg {ref_kind} 'does_not_exist' does not exist"):
        daft.read_iceberg(table, **{ref_kind: "does_not_exist"})


@pytest.mark.parametrize(("ref_kind", "ref_name", "actual"), [("branch", "t1", "tag"), ("tag", "b1", "branch")])
def test_read_iceberg_ref_kind_mismatch(table_with_refs, ref_kind, ref_name, actual):
    """Passing a tag name as `branch` (or vice versa) reports the actual ref type."""
    table, _ = table_with_refs
    with pytest.raises(ValueError, match=f"Iceberg {ref_kind} '{ref_name}' is a {actual}"):
        daft.read_iceberg(table, **{ref_kind: ref_name})


def test_read_iceberg_schema_evolution_per_snapshot(local_catalog):
    """A snapshot predating an added column reads with the schema it was written under."""
    table = local_catalog.create_table("default.evolved", Schema(NestedField(1, "x", LongType(), required=False)))
    _write(table, x=[1, 2])
    snapshot_before_evolution = table.current_snapshot().snapshot_id

    with table.update_schema() as update:
        update.add_column("y", StringType())
    table.refresh()
    table.append(pa.table({"x": [3, 4], "y": ["a", "b"]}))
    table.refresh()

    current = daft.read_iceberg(table)
    historical = daft.read_iceberg(table, snapshot_id=snapshot_before_evolution)

    assert current.sort("x").to_pydict() == {"x": [1, 2, 3, 4], "y": [None, None, "a", "b"]}
    assert historical.schema().column_names() == ["x"]
    assert historical.sort("x").to_pydict() == {"x": [1, 2]}


def test_read_iceberg_pushdowns(local_catalog):
    """Projection, filter, limit, and count all work through the scan."""
    table = local_catalog.create_table(
        "default.pushdowns",
        Schema(
            NestedField(1, "x", LongType(), required=False),
            NestedField(2, "y", StringType(), required=False),
        ),
    )
    _write(table, x=[1, 2, 3], y=["a", "b", "c"])

    df = daft.read_iceberg(table)

    assert df.select("y").sort("y").to_pydict() == {"y": ["a", "b", "c"]}
    assert df.where(daft.col("x") > 1).sort("x").to_pydict() == {"x": [2, 3], "y": ["b", "c"]}
    assert len(df.limit(2).to_pydict()["x"]) == 2
    assert df.count_rows() == 3


def test_read_iceberg_partition_column_projection(local_catalog):
    """Reading only an identity-partitioned column reconstructs it from partition metadata.

    Iceberg strips identity-partitioned columns from the data files themselves, so the
    values come from the manifest rather than the Parquet payload (see #2129).
    """
    schema = Schema(
        NestedField(1, "part", StringType(), required=False),
        NestedField(2, "v", LongType(), required=False),
    )
    spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="part"))
    table = local_catalog.create_table("default.partitioned", schema, partition_spec=spec)
    table.append(pa.table({"part": ["a", "b", "a"], "v": [1, 2, 3]}))
    table.refresh()

    df = daft.read_iceberg(table)

    assert df.select("part").sort("part").to_pydict() == {"part": ["a", "a", "b"]}
    assert df.where(daft.col("part") == "a").sort("v").to_pydict() == {"part": ["a", "a"], "v": [1, 3]}
    assert df.count_rows() == 3


@pytest.fixture
def partitioned_counts(local_catalog):
    """identity(dt) partitioned table: dt='a' has 3 rows, dt='b' has 5, 8 in total."""
    schema = Schema(
        NestedField(field_id=1, name="dt", type=StringType(), required=False),
        NestedField(field_id=2, name="x", type=LongType(), required=False),
    )
    partition_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="dt"))
    table = local_catalog.create_table("default.partitioned_counts", schema, partition_spec=partition_spec)
    daft.from_pydict({"dt": ["a"] * 3 + ["b"] * 5, "x": list(range(8))}).write_iceberg(table)
    table.refresh()
    return table


@pytest.mark.parametrize(("value", "expected"), [("a", 3), ("b", 5)])
def test_count_rows_applies_partition_predicate(partitioned_counts, value, expected):
    """A predicate on an identity-partitioned column must reach the count pushdown.

    The optimizer moves such predicates out of `filters` and into `partition_filters`,
    so a count path that only looks at `filters` sees no filter and counts the whole table.
    """
    df = daft.read_iceberg(partitioned_counts).where(daft.col("dt") == value)

    assert df.count_rows() == expected
    # Same answer the slow path gives.
    assert len(df.to_pydict()["x"]) == expected


def test_count_rows_without_predicate_counts_every_partition(partitioned_counts):
    assert daft.read_iceberg(partitioned_counts).count_rows() == 8


def test_count_rows_applies_data_column_predicate(partitioned_counts):
    """A row-level predicate cannot be answered from record counts, so it must not be."""
    assert daft.read_iceberg(partitioned_counts).where(daft.col("x") < 3).count_rows() == 3


def test_count_rows_applies_partition_and_data_predicates_together(partitioned_counts):
    df = daft.read_iceberg(partitioned_counts).where((daft.col("dt") == "b") & (daft.col("x") < 5))

    assert df.count_rows() == 2
    assert len(df.to_pydict()["x"]) == 2


def test_count_rows_with_partition_predicate_matching_nothing(partitioned_counts):
    assert daft.read_iceberg(partitioned_counts).where(daft.col("dt") == "missing").count_rows() == 0


def test_count_rows_when_partition_field_is_renamed(local_catalog):
    """The partition field name need not match the source column it partitions on.

    The predicate then references a name absent from the data schema, so it cannot be
    forwarded to PyIceberg as a row filter and no metadata pruning happens. The count
    still has to come out right, which is what the per-file partition check is for.
    """
    schema = Schema(
        NestedField(field_id=1, name="dt", type=StringType(), required=False),
        NestedField(field_id=2, name="x", type=LongType(), required=False),
    )
    partition_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="dt_part")
    )
    table = local_catalog.create_table("default.renamed_partition_field", schema, partition_spec=partition_spec)
    daft.from_pydict({"dt": ["a"] * 3 + ["b"] * 5, "x": list(range(8))}).write_iceberg(table)
    table.refresh()

    assert daft.read_iceberg(table).where(daft.col("dt") == "a").count_rows() == 3
    assert daft.read_iceberg(table).where(daft.col("dt") == "b").count_rows() == 5
    assert daft.read_iceberg(table).count_rows() == 8


def test_count_rows_with_partition_values_past_bounds_truncation(local_catalog):
    """Column bounds are truncated to 16 bytes, partition records are not.

    Two values sharing a 16-byte prefix are indistinguishable to the metrics PyIceberg
    prunes on, so the count has to be settled from the partition record itself.
    """
    schema = Schema(
        NestedField(field_id=1, name="dt", type=StringType(), required=False),
        NestedField(field_id=2, name="x", type=LongType(), required=False),
    )
    partition_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="dt"))
    table = local_catalog.create_table("default.long_partition_values", schema, partition_spec=partition_spec)

    long_a, long_b = "L" * 30 + "aaa", "L" * 30 + "bbb"
    daft.from_pydict({"dt": [long_a, long_a, long_b], "x": [1, 2, 3]}).write_iceberg(table)
    table.refresh()

    assert daft.read_iceberg(table).where(daft.col("dt") == long_a).count_rows() == 2
    assert daft.read_iceberg(table).where(daft.col("dt") == long_b).count_rows() == 1


def _evolving_table(local_catalog, name, initial_spec):
    schema = Schema(
        NestedField(field_id=1, name="dt", type=StringType(), required=False),
        NestedField(field_id=2, name="region", type=StringType(), required=False),
        NestedField(field_id=3, name="x", type=LongType(), required=False),
    )
    table = local_catalog.create_table(f"default.{name}", schema, partition_spec=initial_spec)
    daft.from_pydict({"dt": ["a", "a", "b"], "region": ["cn", "us", "cn"], "x": [1, 2, 3]}).write_iceberg(table)
    table.refresh()
    return table


def test_count_rows_matches_scan_after_partition_field_is_added(local_catalog):
    """A count must never disagree with what the same query returns.

    Files written before a partition field was added carry no value for it, so their
    rows cannot be settled from partition metadata. The count path has to give up and
    scan rather than assume every row in such a file matches.
    """
    table = _evolving_table(local_catalog, "count_after_evolution", UNPARTITIONED_PARTITION_SPEC)
    with table.update_spec() as update:
        update.add_field("dt", IdentityTransform(), "dt")
    table.refresh()
    daft.from_pydict({"dt": ["a", "b"], "region": ["cn", "us"], "x": [4, 5]}).write_iceberg(table)
    table.refresh()

    df = daft.read_iceberg(table).where(daft.col("dt") == "a")

    assert df.count_rows() == len(df.to_pydict()["x"])


def test_count_rows_with_field_present_in_every_spec(local_catalog):
    """Evolution that leaves the filtered field in place keeps the metadata count exact."""
    partition_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="dt"))
    table = _evolving_table(local_catalog, "count_field_in_all_specs", partition_spec)
    with table.update_spec() as update:
        update.add_field("region", IdentityTransform(), "region")
    table.refresh()
    daft.from_pydict({"dt": ["a", "b"], "region": ["cn", "us"], "x": [4, 5]}).write_iceberg(table)
    table.refresh()

    df = daft.read_iceberg(table).where(daft.col("dt") == "a")

    assert df.count_rows() == 3
    assert df.count_rows() == len(df.to_pydict()["x"])


def test_count_rows_after_partition_field_is_dropped(local_catalog):
    """Dropping the field leaves no partition keys, so the predicate stays a row filter."""
    partition_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="dt"))
    table = _evolving_table(local_catalog, "count_after_drop", partition_spec)
    with table.update_spec() as update:
        update.remove_field("dt")
    table.refresh()
    daft.from_pydict({"dt": ["a", "b"], "region": ["cn", "us"], "x": [4, 5]}).write_iceberg(table)
    table.refresh()

    df = daft.read_iceberg(table).where(daft.col("dt") == "a")

    assert df.count_rows() == 3
    assert df.count_rows() == len(df.to_pydict()["x"])


def _add_files_table(local_catalog, tmp_path, data: pa.Table, **write_options):
    """Register a parquet file written without field IDs, as add_files does when migrating Hive tables.

    Its columns can only be resolved through the table's schema.name-mapping.default.
    """
    path = tmp_path / "no_field_ids.parquet"
    pq.write_table(data, path, **write_options)
    table = local_catalog.create_table("default.migrated", data.schema)
    table.add_files([path.as_uri()])
    return table


def test_read_add_files_table(local_catalog, tmp_path):
    table = _add_files_table(local_catalog, tmp_path, pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}))
    df = daft.read_iceberg(table)

    assert df.sort("a").to_pydict() == {"a": [1, 2, 3], "b": ["x", "y", "z"]}
    assert df.where(daft.col("a") > 1).select("b").sort("b").to_pydict() == {"b": ["y", "z"]}


def test_read_add_files_table_after_rename(local_catalog, tmp_path):
    """The file still says `b`; the name mapping keeps that alias for the renamed field."""
    table = _add_files_table(local_catalog, tmp_path, pa.table({"a": [1, 2], "b": ["x", "y"]}))
    with table.update_schema() as update:
        update.rename_column("b", "c")

    assert daft.read_iceberg(table).sort("a").to_pydict() == {"a": [1, 2], "c": ["x", "y"]}


def test_read_add_files_table_with_nested_types(local_catalog, tmp_path):
    data = pa.table(
        {
            "id": [1, 2],
            "s": [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}],
            "l": [[1, 2], [3]],
            "m": pa.array([[("k1", 1)], [("k2", 2)]], type=pa.map_(pa.string(), pa.int64())),
            "ll": [[[1], [2, 3]], [[4]]],
            "lm": pa.array([[[("k1", 1)]], [[("k2", 2)]]], type=pa.list_(pa.map_(pa.string(), pa.int64()))),
        }
    )
    table = _add_files_table(local_catalog, tmp_path, data)

    expected = daft.from_arrow(table.scan().to_arrow()).sort("id").to_pydict()
    assert daft.read_iceberg(table).sort("id").to_pydict() == expected


def test_read_add_files_table_mixed_with_field_id_files(local_catalog, tmp_path):
    table = _add_files_table(local_catalog, tmp_path, pa.table({"a": [1, 2], "b": ["x", "y"]}))
    _write(table, a=[3], b=["z"])

    assert daft.read_iceberg(table).sort("a").to_pydict() == {"a": [1, 2, 3], "b": ["x", "y", "z"]}


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray", reason="Only the distributed runner splits scan tasks by row group"
)
def test_read_add_files_table_split_by_row_groups(local_catalog, tmp_path):
    """Splitting a scan task reads the file's metadata during planning, which needs the name mapping too."""
    data = pa.table({"a": list(range(1_000))})
    table = _add_files_table(local_catalog, tmp_path, data, row_group_size=100)

    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True, scan_tasks_min_size_bytes=0, scan_tasks_max_size_bytes=1
    ):
        assert daft.read_iceberg(table).sort("a").to_pydict() == {"a": list(range(1_000))}
