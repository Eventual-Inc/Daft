from __future__ import annotations

import contextlib
import os
import subprocess
import sys

import pytest


@contextlib.contextmanager
def with_otel_endpoint():
    old_otel_endpoint = os.getenv("DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT")
    os.environ["DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT"] = "grpc://localhost:4317"

    try:
        yield
    finally:
        if old_otel_endpoint is not None:
            os.environ["DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT"] = old_otel_endpoint
        else:
            del os.environ["DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT"]


def test_basic_otel_usage() -> None:
    pytest.skip("otel is not fully implemented")
    daft_script = """
import daft

df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [1, None, 3, None]})
res = df.count("*").to_pydict()
assert res == {"count": [4]}
    """
    with with_otel_endpoint():
        result = subprocess.run([sys.executable, "-c", daft_script], capture_output=True, env=os.environ)
        assert result.stdout.decode().strip() == ""
