from __future__ import annotations

import time

import daft.runners.flotilla as flotilla
from daft.context import execution_config_ctx

NODE_ID = "1" * 56
FAKE_ADDRESS = "grpc://10.0.0.1:9999"


def _fake_node(node_id: str = NODE_ID) -> dict:
    return {"NodeID": node_id, "Resources": {"CPU": 4, "memory": 1024, "GPU": 1}}


class _RemoteMethod:
    def __init__(self, result: str) -> None:
        self._result = result

    def remote(self) -> str:
        return self._result


class _FakeActor:
    def __init__(self, address: str = FAKE_ADDRESS) -> None:
        self.get_address = _RemoteMethod(address)


class _FakeActorOptions:
    def remote(self, *, num_cpus: int, num_gpus: int) -> _FakeActor:
        return _FakeActor()


class _FakeRaySwordfishActor:
    @staticmethod
    def options(**kwargs) -> _FakeActorOptions:
        return _FakeActorOptions()


def _patch_flotilla(monkeypatch, nodes=None):
    """Set up common monkeypatches for start_ray_workers tests."""
    if nodes is None:
        nodes = [_fake_node()]

    monkeypatch.setattr(flotilla, "_pending_actors", {})
    monkeypatch.setattr(flotilla.ray, "nodes", lambda: nodes)
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


def test_start_ray_workers_returns_ready_immediately(monkeypatch):
    """When actors are ready immediately, they are returned on the first call."""
    _patch_flotilla(monkeypatch)

    # ray.wait returns all refs as ready
    monkeypatch.setattr(flotilla.ray, "wait", lambda refs, num_returns, timeout: (refs, []))
    monkeypatch.setattr(flotilla.ray, "get", lambda ref: FAKE_ADDRESS)

    workers = flotilla.start_ray_workers(existing_worker_ids=[])

    assert len(workers) == 1
    assert workers[0]["node_id"] == NODE_ID
    assert workers[0]["num_cpus"] == 4
    assert workers[0]["num_gpus"] == 1
    assert workers[0]["memory"] == 1024
    assert workers[0]["ip_address"] == FAKE_ADDRESS
    assert len(flotilla._pending_actors) == 0


def test_start_ray_workers_pending_resolved_on_subsequent_call(monkeypatch):
    """Actors not ready on the first call are returned when ready on a subsequent call."""
    _patch_flotilla(monkeypatch)

    # First call: ray.wait returns nothing ready
    monkeypatch.setattr(flotilla.ray, "wait", lambda refs, num_returns, timeout: ([], refs))
    monkeypatch.setattr(flotilla.ray, "get", lambda ref: FAKE_ADDRESS)

    workers = flotilla.start_ray_workers(existing_worker_ids=[])
    assert len(workers) == 0
    assert len(flotilla._pending_actors) == 1

    # Second call: ray.wait returns all ready
    monkeypatch.setattr(flotilla.ray, "wait", lambda refs, num_returns, timeout: (refs, []))

    workers = flotilla.start_ray_workers(existing_worker_ids=[])
    assert len(workers) == 1
    assert workers[0]["node_id"] == NODE_ID
    assert len(flotilla._pending_actors) == 0


def test_start_ray_workers_expires_timed_out_actors(monkeypatch):
    """Pending actors that exceed the startup timeout are killed and removed."""
    _patch_flotilla(monkeypatch)

    killed_actors: list[object] = []
    monkeypatch.setattr(flotilla.ray, "kill", lambda actor: killed_actors.append(actor))
    # ray.wait returns nothing ready
    monkeypatch.setattr(flotilla.ray, "wait", lambda refs, num_returns, timeout: ([], refs))

    # Seed a pending actor that was spawned long ago
    fake_actor = _FakeActor()
    flotilla._pending_actors[NODE_ID] = flotilla._PendingActor(
        node=_fake_node(),
        actor=fake_actor,
        address_ref=fake_actor.get_address.remote(),
        spawn_time=time.monotonic() - 9999,
    )

    with execution_config_ctx(worker_startup_timeout=60):
        workers = flotilla.start_ray_workers(existing_worker_ids=[])

    assert len(workers) == 0
    assert len(flotilla._pending_actors) == 0
    assert fake_actor in killed_actors


def test_start_ray_workers_handles_dead_actor(monkeypatch):
    """If a ready actor's get_address raises ActorDiedError, it is removed from pending."""
    _patch_flotilla(monkeypatch)

    # ray.wait returns all refs as ready
    monkeypatch.setattr(flotilla.ray, "wait", lambda refs, num_returns, timeout: (refs, []))

    def _raise_actor_died(ref):
        raise ray.exceptions.ActorDiedError()

    monkeypatch.setattr(flotilla.ray, "get", _raise_actor_died)

    workers = flotilla.start_ray_workers(existing_worker_ids=[])

    assert len(workers) == 0
    assert len(flotilla._pending_actors) == 0


def test_start_ray_workers_prunes_disappeared_nodes(monkeypatch):
    """Pending actors for nodes that disappear from ray.nodes() are cleaned up."""
    _patch_flotilla(monkeypatch, nodes=[_fake_node()])

    killed_actors: list[object] = []
    monkeypatch.setattr(flotilla.ray, "kill", lambda actor: killed_actors.append(actor))

    # Seed a pending actor for a node that will disappear
    disappeared_node_id = "2" * 56
    fake_actor = _FakeActor()
    flotilla._pending_actors[disappeared_node_id] = flotilla._PendingActor(
        node=_fake_node(disappeared_node_id),
        actor=fake_actor,
        address_ref=fake_actor.get_address.remote(),
        spawn_time=time.monotonic(),
    )

    # ray.wait returns all as ready (for the new node spawned this call)
    monkeypatch.setattr(flotilla.ray, "wait", lambda refs, num_returns, timeout: (refs, []))
    monkeypatch.setattr(flotilla.ray, "get", lambda ref: FAKE_ADDRESS)

    workers = flotilla.start_ray_workers(existing_worker_ids=[])

    # Disappeared node's actor should be killed
    assert fake_actor in killed_actors
    assert disappeared_node_id not in flotilla._pending_actors
    # The real node should produce a worker
    assert len(workers) == 1
    assert workers[0]["node_id"] == NODE_ID


def test_start_ray_workers_skips_already_pending_nodes(monkeypatch):
    """Nodes with actors already pending should not spawn duplicate actors."""
    _patch_flotilla(monkeypatch)

    # ray.wait returns nothing ready
    monkeypatch.setattr(flotilla.ray, "wait", lambda refs, num_returns, timeout: ([], refs))

    # First call spawns an actor
    flotilla.start_ray_workers(existing_worker_ids=[])
    assert len(flotilla._pending_actors) == 1
    first_pending = flotilla._pending_actors[NODE_ID]

    # Second call should not replace the pending actor
    flotilla.start_ray_workers(existing_worker_ids=[])
    assert len(flotilla._pending_actors) == 1
    assert flotilla._pending_actors[NODE_ID] is first_pending


try:
    import ray
except ImportError:
    pass
