from __future__ import annotations

import pytest


@pytest.fixture(scope="function")
def csv_files(tmpdir):
    """Writes 3 CSV files, each of 10 bytes in size, to tmpdir and yield tmpdir."""
    for i in range(3):
        path = tmpdir / f"file.{i}.csv"
        path.write_text("a,b,c\n1,2,", "utf8")  # 10 bytes

    return tmpdir
