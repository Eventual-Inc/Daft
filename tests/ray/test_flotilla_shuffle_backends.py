from __future__ import annotations

import asyncio

import pytest

from daft.runners import flotilla


class _AwaitableResultHandle:
    def __init__(self, result):
        self._result = result

    def __await__(self):
        async def _wait():
            return self._result

        return _wait().__await__()


class _RemoteMethod:
    def __init__(self, result):
        self._result = result

    async def remote(self):
        return self._result


class _FakeActorHandle:
    def __init__(self, address: str):
        self.get_address = _RemoteMethod(address)


class _ClearShuffleRemoteMethod:
    def __init__(self):
        self.calls = []

    async def remote(self, shuffle_ids):
        self.calls.append(shuffle_ids)
        return len(shuffle_ids)


class _FakeClearShuffleActorHandle:
    def __init__(self):
        self.clear_flight_shuffles = _ClearShuffleRemoteMethod()


class _FlightPartitionRef:
    def __init__(self, shuffle_id, server_address, partition_ref_id, num_rows, size_bytes):
        self.shuffle_id = shuffle_id
        self.server_address = server_address
        self.partition_ref_id = partition_ref_id
        self.num_rows = num_rows
        self.size_bytes = size_bytes


@pytest.mark.skipif(flotilla.ray is None, reason="Ray is required for flotilla tests")
def test_task_handle_converts_ray_shuffle_results(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_success(refs, stats):
        captured["refs"] = refs
        captured["stats"] = stats
        return ("ray", refs, stats)

    monkeypatch.setattr(flotilla.RayTaskResult, "ray_shuffle_success", staticmethod(_fake_success))

    object_ref = object()
    handle = flotilla.RaySwordfishTaskHandle(
        result_handle=_AwaitableResultHandle(("ray", [(object_ref, None, 3, 24)], b"stats")),
        actor_handle=_FakeActorHandle("grpc://unused"),
        shuffle_write_info=("ray", 0, 1),
    )

    result = asyncio.run(handle._get_result())

    assert result[0] == "ray"
    refs = captured["refs"]
    assert isinstance(refs, list)
    assert len(refs) == 1
    assert refs[0].object_ref is object_ref
    assert refs[0].num_rows == 3
    assert refs[0].size_bytes == 24
    assert captured["stats"] == b"stats"


@pytest.mark.skipif(flotilla.ray is None, reason="Ray is required for flotilla tests")
def test_task_handle_converts_flight_shuffle_results(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_success(refs, stats):
        captured["refs"] = refs
        captured["stats"] = stats
        return ("flight", refs, stats)

    monkeypatch.setattr(flotilla.RayTaskResult, "flight_shuffle_success", staticmethod(_fake_success))
    monkeypatch.setattr(flotilla, "FlightShufflePartitionRef", _FlightPartitionRef)

    handle = flotilla.RaySwordfishTaskHandle(
        result_handle=_AwaitableResultHandle(("flight", [(None, 101, 4, 40), (None, 102, 5, 50)], b"stats")),
        actor_handle=_FakeActorHandle("grpc://127.0.0.1:9000"),
        shuffle_write_info=("flight", 17, 2),
    )

    result = asyncio.run(handle._get_result())

    assert result[0] == "flight"
    refs = captured["refs"]
    assert isinstance(refs, list)
    assert len(refs) == 2
    assert refs[0].shuffle_id == 17
    assert refs[0].server_address == "grpc://127.0.0.1:9000"
    assert refs[0].partition_ref_id == 101
    assert refs[0].num_rows == 4
    assert refs[0].size_bytes == 40
    assert refs[1].partition_ref_id == 102
    assert refs[1].num_rows == 5
    assert refs[1].size_bytes == 50
    assert captured["stats"] == b"stats"


@pytest.mark.skipif(flotilla.ray is None, reason="Ray is required for flotilla tests")
def test_clear_flight_shuffle_state_on_workers_uses_actor_remote():
    worker_a = flotilla.RaySwordfishActorHandle(_FakeClearShuffleActorHandle())
    worker_b = flotilla.RaySwordfishActorHandle(_FakeClearShuffleActorHandle())

    asyncio.run(flotilla.clear_flight_shuffle_state_on_workers([worker_a, worker_b], [11, 12]))

    assert worker_a.actor_handle.clear_flight_shuffles.calls == [[11, 12]]
    assert worker_b.actor_handle.clear_flight_shuffles.calls == [[11, 12]]
