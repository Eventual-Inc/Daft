"""End-to-end tests: load a native extension and run a query under native and Ray runners.

These protect against regressions in the ScalarFunctionHandle serde round-trip
and the Ray runtime_env propagation.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap


def test_extension_native_runner(hello_extension_path: str):
    """Sanity check: the extension works in-process with the native runner."""
    script = textwrap.dedent(f"""
        import daft
        daft.set_runner_native()
        daft.load_extension({hello_extension_path!r})

        df = daft.from_pydict({{"name": ["John", "Paul"]}})
        result = df.select(daft.get_function("greet", daft.col("name"))).to_pydict()
        assert result["greet"] == ["Hello, John!", "Hello, Paul!"], result
    """)
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_extension_ray_runner(hello_extension_path: str):
    """The core test: extension works on Ray workers after deserialization."""
    script = textwrap.dedent(f"""
        import ray, daft
        ray.init(num_cpus=2, ignore_reinit_error=True, log_to_driver=True)
        daft.load_extension({hello_extension_path!r})
        daft.set_runner_ray(noop_if_initialized=True)

        df = daft.from_pydict({{"name": ["John", "Paul"]}})
        result = df.select(daft.get_function("greet", daft.col("name"))).to_pydict()
        assert result["greet"] == ["Hello, John!", "Hello, Paul!"], result
    """)
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr
