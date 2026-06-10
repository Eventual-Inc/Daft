from __future__ import annotations

import pyarrow as pa
import pytest

pytest.importorskip("deltalake")

from daft.io.writer import DeltalakeWriter
from daft.recordbatch.micropartition import MicroPartition


def test_deltalake_writer_empty_micropartition(tmp_path):
    empty = MicroPartition.from_arrow(pa.table({"x": pa.array([], type=pa.int64())}))
    writer = DeltalakeWriter(
        root_dir=str(tmp_path),
        file_idx=0,
        version=1,
        large_dtypes=False,
    )
    bytes_written = writer.write(empty)
    assert bytes_written == 0
    # Must not IndexError on metadata_collector[0] when no file was opened.
    result = writer.close()
    assert len(result) == 0
    assert "add_action" in result.schema().column_names()
    assert not (tmp_path / writer.file_name).exists()
