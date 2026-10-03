from __future__ import annotations

import os
import subprocess
import sys
import textwrap

import pytest


@pytest.mark.parametrize("partitioned", [False, True])
def test_pyarrow_parquet_writer_without_pyiceberg(tmp_path, partitioned):
    script = textwrap.dedent("""
        import sys

        # Model a missing Iceberg extra even when CI has it installed.
        sys.modules["pyiceberg"] = None

        import pyarrow.parquet as pq

        import daft
        from daft.context import execution_config_ctx

        partition_cols = ["part"] if sys.argv[1] == "True" else None
        with execution_config_ctx(native_parquet_writer=False):
            daft.from_pydict({"value": [1, 2], "part": ["a", "b"]}).write_parquet(
                "output", partition_cols=partition_cols
            )
        external = pq.read_table("output", columns=["value"], partitioning=None).to_pydict()
        assert sorted(external["value"]) == [1, 2], external
        actual = daft.read_parquet("output/**/*.parquet").select("value").to_pydict()
        assert sorted(actual["value"]) == [1, 2], actual
    """)
    result = subprocess.run(
        [sys.executable, "-c", script, str(partitioned)],
        cwd=tmp_path,
        env={**os.environ, "DAFT_RUNNER": "native", "DAFT_ANALYTICS_ENABLED": "0"},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
