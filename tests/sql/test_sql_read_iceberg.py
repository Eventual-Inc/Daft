from __future__ import annotations

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
