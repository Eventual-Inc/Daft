from __future__ import annotations

import pytest
import pyarrow as pa


@pytest.fixture(scope="function")
def local_paimon_catalog(tmp_path):
    """Create a local filesystem Paimon catalog backed by a tmp directory."""
    pypaimon = pytest.importorskip("pypaimon")
    catalog = pypaimon.CatalogFactory.create({"warehouse": str(tmp_path)})
    catalog.create_database("test_db", ignore_if_exists=True)
    return catalog, tmp_path


@pytest.fixture
def append_only_table(local_paimon_catalog):
    """Append-only (no primary key) partitioned Paimon table for read/write tests."""
    pypaimon = pytest.importorskip("pypaimon")
    catalog, tmp_path = local_paimon_catalog
    schema = pypaimon.Schema.from_pyarrow_schema(
        pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("value", pa.float64()),
                pa.field("dt", pa.string()),
            ]
        ),
        partition_keys=["dt"],
        options={"bucket": "1", "file.format": "parquet"},
    )
    catalog.create_table("test_db.append_table", schema, ignore_if_exists=False)
    return catalog.get_table("test_db.append_table"), tmp_path


@pytest.fixture
def append_only_table_no_partition(local_paimon_catalog):
    """Append-only table with no partition columns."""
    pypaimon = pytest.importorskip("pypaimon")
    catalog, tmp_path = local_paimon_catalog
    schema = pypaimon.Schema.from_pyarrow_schema(
        pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
            ]
        ),
        options={"bucket": "1", "file.format": "parquet", "bucket-key": "id"},
    )
    catalog.create_table("test_db.no_partition_table", schema, ignore_if_exists=False)
    return catalog.get_table("test_db.no_partition_table"), tmp_path


@pytest.fixture
def pk_table(local_paimon_catalog):
    """Primary-key Paimon table for testing the LSM-merge fall-back path."""
    pypaimon = pytest.importorskip("pypaimon")
    catalog, tmp_path = local_paimon_catalog
    schema = pypaimon.Schema.from_pyarrow_schema(
        pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("dt", pa.string()),
            ]
        ),
        partition_keys=["dt"],
        primary_keys=["id", "dt"],
        options={"bucket": "1", "file.format": "parquet"},
    )
    catalog.create_table("test_db.pk_table", schema, ignore_if_exists=False)
    return catalog.get_table("test_db.pk_table"), tmp_path


def _write_to_paimon(table, arrow_table, mode="append", overwrite_partition=None):
    """Helper: write an Arrow table to a Paimon table via pypaimon BatchTableWrite."""
    write_builder = table.new_batch_write_builder()
    if mode == "overwrite":
        write_builder.overwrite(overwrite_partition or {})
    table_write = write_builder.new_write()
    table_commit = write_builder.new_commit()
    try:
        table_write.write_arrow(arrow_table)
        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
    finally:
        table_write.close()
        table_commit.close()
