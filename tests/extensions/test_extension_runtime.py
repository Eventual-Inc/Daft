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
        assert result["name"] == ["Hello, John!", "Hello, Paul!"], result
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
        assert result["name"] == ["Hello, John!", "Hello, Paul!"], result
    """)
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_extension_literal_dependent_return_type(hello_extension_path: str):
    """The output type's width comes from a literal argument's value."""
    script = textwrap.dedent(f"""
        import daft
        daft.set_runner_native()
        daft.load_extension({hello_extension_path!r})

        df = daft.from_pydict({{"x": [1, 2, None]}})
        df = df.select(daft.get_function("splat", daft.col("x"), daft.lit(3)))

        expected = daft.DataType.fixed_size_list(daft.DataType.int64(), 3)
        assert df.schema()["splat"].dtype == expected, df.schema()

        result = df.to_pydict()
        assert result["splat"] == [[1, 1, 1], [2, 2, 2], None], result
    """)
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_extension_literal_dependent_return_type_ray(hello_extension_path: str):
    """The captured literal survives the driver → worker plan round-trip."""
    script = textwrap.dedent(f"""
        import ray, daft
        ray.init(num_cpus=2, ignore_reinit_error=True, log_to_driver=True)
        daft.load_extension({hello_extension_path!r})
        daft.set_runner_ray(noop_if_initialized=True)

        df = daft.from_pydict({{"x": [1, 2, 3]}})
        df = df.select(daft.get_function("splat", daft.col("x"), daft.lit(2)))

        result = df.to_pydict()
        assert result["splat"] == [[1, 1], [2, 2], [3, 3]], result
    """)
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_extension_non_foldable_argument_errors(hello_extension_path: str):
    """A column where a literal is required fails during planning."""
    script = textwrap.dedent(f"""
        import daft
        daft.set_runner_native()
        daft.load_extension({hello_extension_path!r})

        df = daft.from_pydict({{"x": [1, 2, 3], "k": [3, 3, 3]}})
        try:
            df.select(daft.get_function("splat", daft.col("x"), daft.col("k"))).schema()
        except Exception as e:
            assert "must be a literal" in str(e), e
        else:
            raise AssertionError("expected a planning error")
    """)
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_extension_ray_runner_aggregate(hello_extension_path: str):
    """Aggregate extension functions also work on Ray workers after deserialization."""
    script = textwrap.dedent(f"""
        import ray, daft
        ray.init(num_cpus=2, ignore_reinit_error=True, log_to_driver=True)
        daft.load_extension({hello_extension_path!r})
        daft.set_runner_ray(noop_if_initialized=True)

        df = daft.from_pydict({{"name": ["Alice", "Bob", "Carol"]}})
        result = df.agg(
            daft.get_aggregate_function("string_count", daft.col("name"))
        ).to_pydict()
        assert result["name"] == [3], result
    """)
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr
