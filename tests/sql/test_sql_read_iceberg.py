from __future__ import annotations

import os
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pytest

import daft

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType


def _metadata_path(metadata_location: str) -> str:
    parsed = urlparse(metadata_location)
    return unquote(parsed.path) if parsed.scheme == "file" else metadata_location


def _local_path(path: str) -> str:
    parsed = urlparse(path)
    return unquote(parsed.path) if parsed.scheme else path


def _iceberg_data_file_local_paths(table) -> list[str]:
    return sorted(_local_path(task.file.file_path) for task in table.scan().plan_files())


def test_sql_read_iceberg_branch_and_tag_with_schema_evolution(tmp_path):
    catalog = SqlCatalog(
        "default",
        uri=f"sqlite:///{tmp_path}/pyiceberg_catalog.db",
        warehouse=f"file://{tmp_path}",
    )
    try:
        catalog.create_namespace("default")
        table = catalog.create_table("default.t", Schema(NestedField(1, "x", LongType())))

        daft.from_pydict({"x": [1, 2]}).write_iceberg(table)
        first_snapshot_id = table.current_snapshot().snapshot_id
        table.manage_snapshots().create_branch(first_snapshot_id, "first_branch").create_tag(
            first_snapshot_id, "first_tag"
        ).commit()
        table.refresh()

        daft.from_pydict({"x": [3, 4]}).write_iceberg(table)
        main_snapshot_id = table.current_snapshot().snapshot_id
        table.manage_snapshots().create_branch(main_snapshot_id, "schema_branch").commit()
        table.refresh()

        with table.update_schema() as update:
            update.add_column("y", StringType())
        table.refresh()
        table.append(pa.table({"x": [5, 6], "y": ["a", "b"]}), branch="schema_branch")
        table.refresh()

        schema_branch_snapshot_id = table.refs()["schema_branch"].snapshot_id
        table.manage_snapshots().create_tag(schema_branch_snapshot_id, "schema_tag").commit()
        table.refresh()

        metadata_location = _metadata_path(table.metadata_location)

        main_df = daft.sql(f"SELECT * FROM read_iceberg('{metadata_location}')")
        first_snapshot_df = daft.sql(
            f"SELECT * FROM read_iceberg('{metadata_location}', snapshot_id => {first_snapshot_id})"
        )
        first_branch_df = daft.sql(f"SELECT * FROM read_iceberg('{metadata_location}', branch => 'first_branch')")
        first_tag_df = daft.sql(f"SELECT * FROM read_iceberg('{metadata_location}', tag => 'first_tag')")
        schema_branch_df = daft.sql(f"SELECT * FROM read_iceberg('{metadata_location}', branch => 'schema_branch')")
        schema_tag_df = daft.sql(f"SELECT * FROM read_iceberg('{metadata_location}', tag => 'schema_tag')")

        assert main_df.sort("x").to_pydict() == {"x": [1, 2, 3, 4], "y": [None, None, None, None]}
        assert first_snapshot_df.sort("x").to_pydict() == {"x": [1, 2]}
        assert first_branch_df.sort("x").to_pydict() == {"x": [1, 2]}
        assert first_tag_df.sort("x").to_pydict() == {"x": [1, 2]}
        assert schema_branch_df.sort("x").to_pydict() == {
            "x": [1, 2, 3, 4, 5, 6],
            "y": [None, None, None, None, "a", "b"],
        }
        assert schema_tag_df.sort("x").to_pydict() == {
            "x": [1, 2, 3, 4, 5, 6],
            "y": [None, None, None, None, "a", "b"],
        }
        with pytest.raises(Exception, match="Only one of snapshot_id, branch, or tag may be provided") as exc_info:
            daft.sql(
                f"SELECT * FROM read_iceberg('{metadata_location}', "
                f"snapshot_id => {first_snapshot_id}, branch => 'first_branch', tag => 'first_tag')"
            )
        assert exc_info.type.__name__ == "InvalidSQLException"
        for ref_kind in ("branch", "tag"):
            with pytest.raises(Exception, match=f"Iceberg {ref_kind} 'does_not_exist' does not exist") as exc_info:
                daft.sql(f"SELECT * FROM read_iceberg('{metadata_location}', {ref_kind} => 'does_not_exist')")
            assert exc_info.type.__name__ == "InvalidSQLException"
    finally:
        catalog.engine.dispose()


def test_sql_read_iceberg_ignore_corrupt_files_skips_data_file(tmp_path):
    catalog = SqlCatalog(
        "default",
        uri=f"sqlite:///{tmp_path}/pyiceberg_catalog.db",
        warehouse=f"file://{tmp_path}",
    )
    try:
        catalog.create_namespace("default")
        table = catalog.create_table("default.t_corrupt", Schema(NestedField(1, "id", LongType())))

        table.append(pa.table({"id": pa.array([1, 2, 3], type=pa.int64())}))
        table.append(pa.table({"id": pa.array([4, 5, 6], type=pa.int64())}))

        data_files = _iceberg_data_file_local_paths(table)
        assert len(data_files) == 2

        with open(data_files[0], "wb") as f:
            f.write(b"PAR1" + b"\x00" * 20 + b"PAR1")

        metadata_location = _metadata_path(table.metadata_location)

        with pytest.raises(Exception):
            daft.sql(f"SELECT * FROM read_iceberg('{metadata_location}')").collect()

        df = daft.sql(f"SELECT * FROM read_iceberg('{metadata_location}', ignore_corrupt_files => true)")
        df.collect()

        result = sorted(df.to_pydict()["id"])
        assert len(result) == 3
        assert set(result).issubset({1, 2, 3, 4, 5, 6})

        skipped = df.skipped_corrupt_files
        assert len(skipped) == 1
        path, reason, partial = skipped[0]
        assert os.path.basename(_local_path(path)) == os.path.basename(data_files[0])
        assert reason
        assert not partial
    finally:
        catalog.engine.dispose()


@pytest.mark.skip(
    "invoke manually via `uv run tests/sql/test_table_functions/test_read_iceberg.py <metadata_location>`"
)
def test_read_iceberg(metadata_location):
    df = daft.sql(f"SELECT * FROM read_iceberg('{metadata_location}')")
    print(df.collect())


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("usage: test_read_iceberg.py <metadata_location>")
        sys.exit(1)
    test_read_iceberg(metadata_location=sys.argv[1])
