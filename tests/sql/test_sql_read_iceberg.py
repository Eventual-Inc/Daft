from __future__ import annotations

import pytest

import daft


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
