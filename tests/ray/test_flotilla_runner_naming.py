from __future__ import annotations

import pytest

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


class _FakeAddressRef:
    """Stand-in for the ObjectRef returned by ``actor.get_address.remote()``."""

    def __init__(self, key: int) -> None:
        self.key = key


class _FakeRemoteMethod:
    def __init__(self, key: int) -> None:
        self._key = key

    def remote(self) -> _FakeAddressRef:
        return _FakeAddressRef(self._key)


class _FakeActor:
    def __init__(self, key: int) -> None:
        self.key = key
        self.get_address = _FakeRemoteMethod(key)


def _hex_node_id(n: int) -> str:
    # Ray validates NodeIDs as 28-byte (56 hex char) non-nil hex strings.
    return f"{n + 1:056x}"


def _node(node_id: str) -> dict:
    return {"NodeID": node_id, "Resources": {"CPU": 4.0, "memory": 1000.0, "GPU": 0.0}}


def _install_fakes(monkeypatch, nodes, outcomes):
    """Wire flotilla's Ray surface to in-memory fakes.

    ``outcomes`` maps actor-creation index (in ``ray.nodes()`` order) to one of:
    ``("ok", address)`` (resolves), ``("raise", exception)`` (resolves but
    ``ray.get`` raises), or ``("timeout", None)`` (never resolves -> stays in
    ``ray.wait``'s ``not_ready`` bucket).
    """
    captured: dict[str, object] = {"killed": []}

    created = {"i": 0}

    class _Opts:
        def remote(self, **kwargs):
            idx = created["i"]
            created["i"] += 1
            return _FakeActor(idx)

    class _FakeActorClass:
        @staticmethod
        def options(**kwargs):
            return _Opts()

    def _fake_ray_wait(refs, *, num_returns, timeout):
        captured["timeout"] = timeout
        captured["num_returns"] = num_returns
        ready = [ref for ref in refs if outcomes[ref.key][0] != "timeout"]
        not_ready = [ref for ref in refs if outcomes[ref.key][0] == "timeout"]
        return ready, not_ready

    def _fake_ray_get(ref):
        kind, value = outcomes[ref.key]
        if kind == "raise":
            raise value
        return value

    def _fake_ray_kill(actor, no_restart=False):
        captured["killed"].append(actor.key)

    class _FakeWorker:
        def __init__(self, node_id, handle, cpu, gpu, mem, ip):
            self.node_id = node_id
            self.ip = ip

    monkeypatch.setattr(flotilla.ray, "nodes", lambda: nodes)
    monkeypatch.setattr(flotilla.ray, "wait", _fake_ray_wait)
    monkeypatch.setattr(flotilla.ray, "get", _fake_ray_get)
    monkeypatch.setattr(flotilla.ray, "kill", _fake_ray_kill)
    monkeypatch.setattr(flotilla, "RaySwordfishActor", _FakeActorClass)
    monkeypatch.setattr(flotilla, "RaySwordfishWorker", _FakeWorker)
    monkeypatch.setattr(flotilla, "_extension_runtime_env", lambda: {})
    return captured


def _unschedulable_error() -> Exception:
    # Construct without invoking Ray's constructor (signature varies by version).
    return flotilla.ray.exceptions.ActorUnschedulableError.__new__(
        flotilla.ray.exceptions.ActorUnschedulableError
    )


def _actor_died_error() -> Exception:
    return flotilla.ray.exceptions.ActorDiedError.__new__(flotilla.ray.exceptions.ActorDiedError)


def _actor_unavailable_error() -> Exception:
    return flotilla.ray.exceptions.ActorUnavailableError.__new__(
        flotilla.ray.exceptions.ActorUnavailableError
    )


def _ray_task_error() -> Exception:
    return flotilla.ray.exceptions.RayTaskError.__new__(flotilla.ray.exceptions.RayTaskError)


def test_start_ray_workers_uses_passed_worker_startup_timeout(monkeypatch):
    node_0 = _hex_node_id(0)
    captured = _install_fakes(
        monkeypatch,
        nodes=[_node(node_0)],
        outcomes={0: ("ok", "1.2.3.4:5000")},
    )

    workers = flotilla.start_ray_workers(existing_worker_ids=[], worker_startup_timeout=321)

    # The shared deadline is applied once, on ray.wait -- not per-actor on ray.get.
    assert captured["timeout"] == 321
    assert captured["num_returns"] == 1
    assert len(workers) == 1
    assert workers[0].node_id == node_0
    assert workers[0].ip == "1.2.3.4:5000"
    assert captured["killed"] == []


@pytest.mark.parametrize(
    "outcome",
    [
        ("raise", _unschedulable_error()),
        ("raise", _actor_died_error()),
        ("raise", _actor_unavailable_error()),
        ("raise", _ray_task_error()),
        ("timeout", None),
    ],
)
def test_start_ray_workers_skips_unhealthy_worker(monkeypatch, outcome):
    """A churned/errored/slow node must be skipped and killed, not fail the call.

    Uses a non-empty existing fleet so the initial-startup hard-fail guard does
    not fire (this is the incremental-refresh path).
    """
    captured = _install_fakes(
        monkeypatch,
        nodes=[_node(_hex_node_id(0))],
        outcomes={0: outcome},
    )

    workers = flotilla.start_ray_workers(existing_worker_ids=[_hex_node_id(99)])

    assert workers == []
    assert captured["killed"] == [0]


def test_start_ray_workers_returns_survivors_on_partial_churn(monkeypatch):
    """Unhealthy nodes are skipped while healthy workers still come up."""
    node_0, node_1, node_2, node_3 = (_hex_node_id(i) for i in range(4))
    captured = _install_fakes(
        monkeypatch,
        nodes=[_node(node_0), _node(node_1), _node(node_2), _node(node_3)],
        outcomes={
            0: ("ok", "10.0.0.1:5000"),
            1: ("raise", _unschedulable_error()),
            2: ("timeout", None),
            3: ("ok", "10.0.0.4:5000"),
        },
    )

    workers = flotilla.start_ray_workers(existing_worker_ids=[])

    assert [w.node_id for w in workers] == [node_0, node_3]
    assert sorted(captured["killed"]) == [1, 2]


def test_start_ray_workers_raises_on_initial_total_failure(monkeypatch):
    """No pre-existing fleet + zero workers come up -> hard fail (config/cluster issue)."""
    _install_fakes(
        monkeypatch,
        nodes=[_node(_hex_node_id(0)), _node(_hex_node_id(1))],
        outcomes={0: ("raise", _unschedulable_error()), 1: ("timeout", None)},
    )

    with pytest.raises(RuntimeError, match="No flotilla workers became available"):
        flotilla.start_ray_workers(existing_worker_ids=[])


def test_start_ray_workers_refresh_total_failure_is_non_fatal(monkeypatch):
    """An existing fleet + a fully-failed refresh batch returns [] instead of raising."""
    captured = _install_fakes(
        monkeypatch,
        nodes=[_node(_hex_node_id(0))],
        outcomes={0: ("timeout", None)},
    )

    workers = flotilla.start_ray_workers(existing_worker_ids=[_hex_node_id(99)])

    assert workers == []
    assert captured["killed"] == [0]
