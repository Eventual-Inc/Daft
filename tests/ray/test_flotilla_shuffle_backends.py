from __future__ import annotations

import asyncio
import pickle

import pytest

from daft.runners import flotilla


class _AwaitableResultHandle:
    def __init__(self, result):
        self._result = result

    def __await__(self):
        async def _wait():
            return self._result

        return _wait().__await__()


class _ClearShuffleRemoteMethod:
    def __init__(self):
        self.calls = []

    async def remote(self, shuffle_ids):
        self.calls.append(shuffle_ids)
        return len(shuffle_ids)


class _FakeClearShuffleActorHandle:
    def __init__(self):
        self.clear_flight_shuffles = _ClearShuffleRemoteMethod()


@pytest.mark.skipif(flotilla.ray is None, reason="Ray is required for flotilla tests")
def test_ray_partition_ref_is_pickleable():
    ref = flotilla.RayPartitionRef({"fake": "ray-ref"}, 3, 24)
    restored = pickle.loads(pickle.dumps(ref))

    assert restored.object_ref == {"fake": "ray-ref"}
    assert restored.num_rows == 3
    assert restored.size_bytes == 24


@pytest.mark.skipif(flotilla.ray is None, reason="Ray is required for flotilla tests")
def test_ray_task_result_is_pickleable_for_materialized_and_shuffle_outputs():
    ray_ref = flotilla.RayPartitionRef({"fake": "ray-ref"}, 3, 24)
    materialized = flotilla.RayTaskResult.success_materialized([ray_ref], b"stats")
    pickle.loads(pickle.dumps(materialized))

    flight_refs = [
        flotilla.FlightPartitionRef(17, "grpc://127.0.0.1:9000", 101, 4, 40),
        flotilla.FlightPartitionRef(17, "grpc://127.0.0.1:9000", 102, 5, 50),
    ]
    shuffle = flotilla.RayTaskResult.success_shuffle_flight(flight_refs, b"stats")
    pickle.loads(pickle.dumps(shuffle))


@pytest.mark.skipif(flotilla.ray is None, reason="Ray is required for flotilla tests")
def test_task_handle_returns_unified_ray_task_result():
    expected = flotilla.RayTaskResult.success_shuffle_ray(
        [flotilla.RayPartitionRef({"fake": "ray-ref"}, 3, 24)],
        b"stats",
    )
    handle = flotilla.RaySwordfishTaskHandle(result_handle=_AwaitableResultHandle(expected))

    result = asyncio.run(handle._get_result())

    assert type(result) is type(expected)


@pytest.mark.skipif(flotilla.ray is None, reason="Ray is required for flotilla tests")
def test_clear_flight_shuffle_state_on_workers_uses_actor_remote():
    worker_a = flotilla.RaySwordfishActorHandle(_FakeClearShuffleActorHandle())
    worker_b = flotilla.RaySwordfishActorHandle(_FakeClearShuffleActorHandle())

    asyncio.run(flotilla.clear_flight_shuffle_state_on_workers([worker_a, worker_b], [11, 12]))

    assert worker_a.actor_handle.clear_flight_shuffles.calls == [[11, 12]]
    assert worker_b.actor_handle.clear_flight_shuffles.calls == [[11, 12]]
