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
import pytest

import daft

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
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
