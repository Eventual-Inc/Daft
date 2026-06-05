from __future__ import annotations

import daft.runners.flotilla as flotilla


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


def test_start_ray_workers_uses_passed_worker_startup_timeout(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_ray_get(address_refs: list[str], *, timeout: int) -> list[str]:
        captured["address_refs"] = address_refs
        captured["timeout"] = timeout
        return []

    monkeypatch.setattr(flotilla.ray, "nodes", lambda: [])
    monkeypatch.setattr(flotilla.ray, "get", _fake_ray_get)

    workers = flotilla.start_ray_workers(existing_worker_ids=[], worker_startup_timeout=321)

    assert captured["timeout"] == 321
    assert captured["address_refs"] == []
    assert workers == []
