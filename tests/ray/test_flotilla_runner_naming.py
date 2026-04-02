from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import ray

import daft
import daft.runners.flotilla as flotilla
from daft.context import get_context
from daft.daft import DistributedPhysicalPlan, PyExecutionStats
from daft.runners.flotilla import RemoteFlotillaRunner


def test_flotilla_runner_actor_name_is_namespaced(monkeypatch):
    """Ensure that the flotilla actor name is not a bare global constant.

    The actor name should include a per-job (or per-process) suffix so that
    multiple Daft clients connected to the same Ray cluster do not share a
    single RemoteFlotillaRunner instance.
    """
    monkeypatch.setattr(flotilla, "_FLOTILLA_RUNNER_NAME_SUFFIX", None)

    class _DummyRuntimeContext:
        def __init__(self, job_id: str) -> None:
            self.job_id = job_id

    def _fake_get_runtime_context() -> _DummyRuntimeContext:
        return _DummyRuntimeContext("test-job-id")

    monkeypatch.setattr(flotilla.ray, "get_runtime_context", _fake_get_runtime_context)

    name = flotilla.get_flotilla_runner_actor_name()

    assert flotilla.FLOTILLA_RUNNER_NAME in name
    assert name != flotilla.FLOTILLA_RUNNER_NAME
    assert name.endswith("test-job-id")
    assert flotilla.get_flotilla_runner_actor_name() == name


def test_flotilla_runner_actor_name_is_isolated_inside_runner_actor(monkeypatch):
    """Nested queries launched from a runner actor should target a child actor, not themselves."""
    monkeypatch.setattr(flotilla, "_FLOTILLA_RUNNER_NAME_SUFFIX", None)

    class _DummyRuntimeContext:
        def __init__(self, job_id: str, actor_id: str) -> None:
            self.job_id = job_id
            self._actor_id = actor_id

        def get_actor_id(self) -> str:
            return self._actor_id

    def _fake_get_runtime_context() -> _DummyRuntimeContext:
        return _DummyRuntimeContext("test-job-id", "test-actor-id")

    monkeypatch.setenv(flotilla._INSIDE_FLOTILLA_RUNNER_ENV, "1")
    monkeypatch.setattr(flotilla.ray, "get_runtime_context", _fake_get_runtime_context)

    name = flotilla.get_flotilla_runner_actor_name()

    assert name == f"{flotilla.FLOTILLA_RUNNER_NAME}-test-job-id-nested-test-actor-id"


def test_flotilla_runner_actor_name_uses_uuid_fallback_inside_runner_actor(monkeypatch):
    """Nested runner naming should not fall back to the parent actor name when actor id lookup fails."""
    monkeypatch.setattr(flotilla, "_FLOTILLA_RUNNER_NAME_SUFFIX", None)

    class _DummyRuntimeContext:
        def __init__(self, job_id: str) -> None:
            self.job_id = job_id

        def get_actor_id(self) -> str:
            raise RuntimeError("actor id unavailable")

    def _fake_get_runtime_context() -> _DummyRuntimeContext:
        return _DummyRuntimeContext("test-job-id")

    monkeypatch.setenv(flotilla._INSIDE_FLOTILLA_RUNNER_ENV, "1")
    monkeypatch.setattr(flotilla.ray, "get_runtime_context", _fake_get_runtime_context)
    monkeypatch.setattr(flotilla.uuid, "uuid4", lambda: type("_UUID", (), {"hex": "fallback-actor-id"})())

    name = flotilla.get_flotilla_runner_actor_name()

    assert name == f"{flotilla.FLOTILLA_RUNNER_NAME}-test-job-id-nested-fallback-actor-id"


def test_stream_plan_uses_query_id_for_runtime_key(monkeypatch):
    """Flotilla runtime bookkeeping should use a globally unique query identity, not query_idx."""

    class _FakeExecutionStats:
        pass

    class _Call:
        def __init__(self, func, *args, **kwargs) -> None:
            self.func = func
            self.args = args
            self.kwargs = kwargs

    class _RemoteMethod:
        def __init__(self, func) -> None:
            self._func = func

        def remote(self, *args, **kwargs):
            return _Call(self._func, *args, **kwargs)

    class _DummyActor:
        def __init__(self) -> None:
            self.plan_ids: list[str] = []
            self.run_plan = _RemoteMethod(self._run_plan)
            self.get_next_partition = _RemoteMethod(self._get_next_partition)

        def _run_plan(self, plan, partition_sets) -> None:
            self.plan = plan
            self.partition_sets = partition_sets

        def _get_next_partition(self, plan_id: str) -> _FakeExecutionStats:
            self.plan_ids.append(plan_id)
            return _FakeExecutionStats()

    dummy_actor = _DummyActor()
    runner = flotilla.FlotillaRunner.__new__(flotilla.FlotillaRunner)
    runner.runner = dummy_actor

    class _DummyPlan:
        def idx(self) -> str:
            return "0"

        def query_id(self) -> str:
            return "query-123"

    monkeypatch.setattr(flotilla, "PyExecutionStats", _FakeExecutionStats)
    monkeypatch.setattr(flotilla.ray, "get", lambda call: call.func(*call.args, **call.kwargs))

    gen = runner.stream_plan(_DummyPlan(), {})
    with pytest.raises(StopIteration):
        next(gen)

    assert dummy_actor.plan_ids == ["query-123"]


def test_distributed_physical_plan_exposes_query_id():
    df = daft.from_pydict({"x": [1]})
    plan = DistributedPhysicalPlan.from_logical_plan_builder(
        df._builder._builder,
        "query-123",
        get_context().daft_execution_config,
    )

    assert plan.query_id() == "query-123"


def _build_plan(path: str, query_id: str) -> DistributedPhysicalPlan:
    cfg = get_context().daft_execution_config
    df = daft.read_parquet(path)
    builder = df._builder.optimize(cfg)
    return DistributedPhysicalPlan.from_logical_plan_builder(builder._builder, query_id, cfg)


@ray.remote
def _build_remote_plan(path: str, query_id: str) -> tuple[DistributedPhysicalPlan, str, str]:
    plan = _build_plan(path, query_id)
    return plan, plan.idx(), plan.query_id()


def _collect_query_output(runner, query_id: str) -> list[str]:
    values: list[str] = []
    while True:
        result = ray.get(runner.get_next_partition.remote(query_id))
        if isinstance(result, PyExecutionStats):
            return values
        mp = ray.get(result.partition())
        values.extend(mp.to_pydict()["x"])


#
# This regression must run in a fresh Python process.
# The bug only becomes obvious when both the driver process and a Ray worker
# create their first DistributedPhysicalPlan and both receive idx == "0".
#
def _run_cross_process_query_id_regression() -> None:
    """Exercise the real cross-process idx collision and verify query_id keeps plans separate."""
    root = Path(tempfile.mkdtemp(prefix="daft-queryid-e2e-"))
    driver_dir = root / "driver"
    worker_dir = root / "worker"
    driver_dir.mkdir()
    worker_dir.mkdir()
    pq.write_table(pa.table({"x": ["driver"]}), driver_dir / "part-0.parquet")
    pq.write_table(pa.table({"x": ["worker"]}), worker_dir / "part-0.parquet")

    ray.init(ignore_reinit_error=True, configure_logging=False, log_to_driver=False)
    try:
        driver_plan = _build_plan(str(driver_dir), "driver-query")
        worker_plan, worker_idx, worker_query_id = ray.get(_build_remote_plan.remote(str(worker_dir), "worker-query"))

        # idx could be the same in different processes.
        assert driver_plan.idx() == "0"
        assert worker_idx == "0"
        # query_id must be different in different processes.
        assert driver_plan.query_id() == "driver-query"
        assert worker_query_id == "worker-query"

        runner = RemoteFlotillaRunner.remote(None)
        ray.get(runner.run_plan.remote(driver_plan, {}))
        ray.get(runner.run_plan.remote(worker_plan, {}))

        assert _collect_query_output(runner, driver_plan.query_id()) == ["driver"]
        assert _collect_query_output(runner, worker_plan.query_id()) == ["worker"]
    finally:
        ray.shutdown()


# Run the regression in a clean subprocess so the driver-side query counter starts
# from zero again. That gives us the exact condition we care about: the driver and
# the remote worker can both independently produce idx == "0".
def test_query_id_avoids_cross_process_idx_collisions():
    """Regression test for cross-process plan key collisions.

    In a fresh process, the driver and a Ray worker both create their first
    DistributedPhysicalPlan, so both plans receive idx == "0". Flotilla must
    still distinguish them by query_id when storing runtime state.
    """
    env = os.environ.copy()
    env["DAFT_RUNNER"] = "ray"
    env["RAY_DISABLE_DASHBOARD"] = "1"
    repo_root = Path(__file__).resolve().parents[2]
    env["PYTHONPATH"] = f"{repo_root}{os.pathsep}{env['PYTHONPATH']}" if "PYTHONPATH" in env else str(repo_root)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from tests.ray.test_flotilla_runner_naming import _run_cross_process_query_id_regression; "
            "_run_cross_process_query_id_regression()",
        ],
        capture_output=True,
        text=True,
        env=env,
        cwd=repo_root,
        check=False,
    )

    assert result.returncode == 0, f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
