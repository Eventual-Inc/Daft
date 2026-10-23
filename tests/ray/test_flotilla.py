from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

import daft.runners.flotilla as flotilla
from daft.context import execution_config_ctx
from daft.runners.flotilla import RaySwordfishTaskHandle
from tests.conftest import get_tests_daft_runner_name


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="flotilla-specific behavior")
def test_swordfish_task_handle_cancel_does_not_fail_ray_task(monkeypatch):
    """Regression: cancelling a swordfish task must not surface it as FAILED in Ray.

    Cancellation used to go through `ray.cancel(result_handle)`, which Ray
    records as a FAILED entry in its task table. The handle now routes
    through `actor.cancel_input.remote(...)`, which closes the per-input
    result channel inside the worker so `run_plan` returns cleanly — the
    Ray task ends FINISHED.

    The test tolerates both the pre-fix and post-fix constructor /
    coroutine shapes of `RaySwordfishTaskHandle` so the failure on pre-fix
    code reads `must not call ray.cancel` (the actual contract) instead
    of a `TypeError` on the missing kwargs.
    """
    import inspect

    ray_cancel_calls = []
    monkeypatch.setattr(
        "daft.runners.flotilla.ray.cancel",
        lambda *a, **kw: ray_cancel_calls.append((a, kw)),
    )

    actor = MagicMock()
    actor.cancel_input.remote.side_effect = lambda *a, **kw: asyncio.sleep(0)

    params = inspect.signature(RaySwordfishTaskHandle).parameters
    kwargs = {"result_handle": MagicMock()}
    if "actor_handle" in params:
        kwargs["actor_handle"] = actor
    if "task_id" in params:
        kwargs["task_id"] = 42
    if "plan_fingerprint" in params:
        kwargs["plan_fingerprint"] = 7
    handle = RaySwordfishTaskHandle(**kwargs)

    # Pre-fix `cancel` is sync; post-fix it's async. Handle either shape.
    result = handle.cancel()
    if asyncio.iscoroutine(result):
        asyncio.run(result)

    assert not ray_cancel_calls, "RaySwordfishTaskHandle.cancel must not call ray.cancel"
    if "actor_handle" in params:
        actor.cancel_input.remote.assert_called_once_with(7, 42)


def test_flotilla_runner_actor_name_is_namespaced(monkeypatch):
    """Ensure that the flotilla actor name is not a bare global constant.

    The actor name should include a per-job (or per-process) suffix so that
    multiple Daft clients connected to the same Ray cluster do not share a
    single RemoteFlotillaRunner instance.
    """
    flotilla._FLOTILLA_RUNNER_NAME_SUFFIX = None

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


def test_start_ray_workers_uses_configured_worker_startup_timeout(monkeypatch):
    captured: dict[str, object] = {}
    node_id = "1" * 56

    class _RemoteMethod:
        def __init__(self, result: str) -> None:
            self._result = result

        def remote(self) -> str:
            return self._result

    class _FakeActor:
        def __init__(self, address: str) -> None:
            self.get_address = _RemoteMethod(address)

    class _FakeActorOptions:
        def remote(self, **kwargs: object) -> _FakeActor:
            captured["remote_args"] = kwargs
            return _FakeActor("grpc://10.0.0.1:9999")

    class _FakeRaySwordfishActor:
        @staticmethod
        def options(**kwargs) -> _FakeActorOptions:
            captured["options_kwargs"] = kwargs
            return _FakeActorOptions()

    def _fake_ray_get(address_refs: list[str], *, timeout: int) -> list[str]:
        captured["address_refs"] = address_refs
        captured["timeout"] = timeout
        return ["grpc://10.0.0.1:9999"]

    monkeypatch.setattr(
        flotilla.ray,
        "nodes",
        lambda: [{"NodeID": node_id, "Resources": {"CPU": 4, "memory": 1024, "GPU": 1}}],
    )
    monkeypatch.setattr(flotilla.ray, "get", _fake_ray_get)
    monkeypatch.setattr(flotilla, "RaySwordfishActor", _FakeRaySwordfishActor)
    monkeypatch.setattr(flotilla, "RaySwordfishActorHandle", lambda actor: ("handle", actor))
    monkeypatch.setattr(
        flotilla,
        "RaySwordfishWorker",
        lambda *args: {
            "node_id": args[0],
            "actor_handle": args[1],
            "num_cpus": args[2],
            "num_gpus": args[3],
            "memory": args[4],
            "ip_address": args[5],
        },
    )

    with execution_config_ctx(worker_startup_timeout=321):
        workers = flotilla.start_ray_workers(existing_worker_ids=[])

    assert captured["timeout"] == 321
    assert captured["address_refs"] == ["grpc://10.0.0.1:9999"]
    assert len(workers) == 1
    assert workers[0]["node_id"] == node_id
    assert workers[0]["num_cpus"] == 4
    assert workers[0]["num_gpus"] == 1
    assert workers[0]["memory"] == 1024
    assert workers[0]["ip_address"] == "grpc://10.0.0.1:9999"
