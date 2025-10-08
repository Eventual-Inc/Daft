from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

import daft

if TYPE_CHECKING:
    import pyarrow as pa
else:
    from daft.dependencies import pa


class TestCustomTask:
    tmp_data = {
        "a": ["a", "b", "c", "d", "e", None],
        "b": [1, None, 3, None, 5, 6],
        "c": [1, 2, 3, 4, 5, None],
    }

    """Test cases for add_columns function."""

    def test_add_columns(self, tmp_path):
        """Test basic add columns functionality."""
        path = str(tmp_path / "add_columns_test.lance")
        daft.from_pydict(self.tmp_data).write_lance(path, max_rows_per_file=1)

        def double_score(x: pa.lib.RecordBatch) -> pa.lib.RecordBatch:
            from daft.dependencies import pa

            df = x.to_pandas()
            return pa.lib.RecordBatch.from_pandas(
                pd.DataFrame({"new_column": df["c"] * 2}),
                schema=pa.schema([pa.field("new_column", pa.float64())]),
            )

        # Add columns
        daft.io.lance.merge_columns(
            path,
            transform=double_score,
            read_columns=["c"],
            concurrency=2,
        )

        # Read it back
        df = daft.read_lance(path)
        result = df.select("new_column", "c").collect().to_pandas()
        pd.testing.assert_series_equal(result["new_column"], result["c"] * 2, check_names=False)
