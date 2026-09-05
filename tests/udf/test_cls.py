from __future__ import annotations

import asyncio
from collections.abc import Iterator
from types import SimpleNamespace

import pytest

import daft
from daft import DataType


@pytest.mark.parametrize("concurrency", [None, 2])
def test_cls(concurrency):
    df = daft.from_pydict(
        {"a": ["foo", "bar", "baz"], "b": [1, 2, 3], "c": [True, None, True]},
    )

    @daft.cls(max_concurrency=concurrency)
    class RepeatN:
        def __init__(self, n: int):
            self.n = n

        def __call__(self, x) -> str:
            return x * self.n

        @daft.method(return_dtype=DataType.list(DataType.string()))
        def repeat_list(self, x):
            return [x] * self.n

    repeat_2 = RepeatN(2)
    result = df.select(repeat_2(df["a"]))
    assert result.to_pydict() == {"a": ["foofoo", "barbar", "bazbaz"]}

    result = df.select(repeat_2(df["a"]), "b")
    assert result.to_pydict() == {"a": ["foofoo", "barbar", "bazbaz"], "b": [1, 2, 3]}

    result = df.select(repeat_2.repeat_list(df["a"]))
    assert result.to_pydict() == {"a": [["foo", "foo"], ["bar", "bar"], ["baz", "baz"]]}


def test_cls_scalar_eval():
    @daft.cls
    class RepeatN:
        def __init__(self, n: int):
            self.n = n

        def __call__(self, x) -> str:
            return x * self.n

        @daft.method(return_dtype=DataType.list(DataType.string()))
        def repeat_list(self, x):
            return [x] * self.n

    repeat_2 = RepeatN(2)
    assert repeat_2("foo") == "foofoo"
    assert repeat_2.repeat_list("foo") == ["foo", "foo"]


@pytest.mark.parametrize("concurrency", [None, 2])
def test_cls_method_without_decorator(concurrency):
    df = daft.from_pydict({"a": [1, 2, 3], "b": ["foo", "bar", "baz"]})

    @daft.cls(max_concurrency=concurrency)
    class Multiplier:
        def __init__(self, factor: int):
            self.factor = factor

        def multiply(self, x: int) -> int:
            return x * self.factor

    m = Multiplier(5)
    result = df.select("b", m.multiply(df["a"]))
    assert result.to_pydict() == {"b": ["foo", "bar", "baz"], "a": [5, 10, 15]}


@pytest.mark.parametrize("concurrency", [None, 2])
def test_cls_multiple_instances(concurrency):
    df = daft.from_pydict({"a": [10, 20, 30], "b": ["foo", "bar", "baz"]})

    @daft.cls(max_concurrency=concurrency)
    class Adder:
        def __init__(self, increment: int):
            self.increment = increment

        def add(self, x: int) -> int:
            return x + self.increment

    adder_1 = Adder(1)
    adder_10 = Adder(10)

    result = df.select(
        adder_1.add(df["a"]).alias("plus_1"),
        adder_10.add(df["a"]).alias("plus_10"),
        "b",
    )
    assert result.to_pydict() == {
        "plus_1": [11, 21, 31],
        "plus_10": [20, 30, 40],
        "b": ["foo", "bar", "baz"],
    }


def test_cls_with_ray_options():
    @daft.cls(cpus=1, ray_options={"scheduling_strategy": "SPREAD"})
    class MyModel:
        def __init__(self):
            pass

        @daft.method(return_dtype=DataType.int64())
        def predict(self, x):
            return x

    model = MyModel()

    df = daft.from_pydict({"x": [1, 2, 3]})

    import io

    f = io.StringIO()
    df.select(model.predict(df["x"])).explain(file=f, show_all=True)
    explanation = f.getvalue()

    # Check ray_options
    assert "num_cpus = 1" in explanation
    assert "'scheduling_strategy': 'SPREAD'" in explanation

    # Verify execution
    result = df.select(model.predict(df["x"])).to_pydict()
    assert result == {"x": [1, 2, 3]}


def test_cls_min_max_concurrency_explain_and_execution():
    @daft.cls(min_concurrency=1, max_concurrency=2)
    class MyModel:
        @daft.method(return_dtype=DataType.int64())
        def predict(self, x):
            return x

    model = MyModel()
    df = daft.from_pydict({"x": [1, 2, 3]})

    import io

    f = io.StringIO()
    df.select(model.predict(df["x"])).explain(file=f, show_all=True)
    explanation = f.getvalue()

    assert "min_concurrency = 1" in explanation
    assert "concurrency = 2" in explanation
    assert df.select(model.predict(df["x"])).to_pydict() == {"x": [1, 2, 3]}


def test_cls_rejects_invalid_min_max_concurrency():
    with pytest.raises(ValueError, match="min_concurrency for udf must be non-zero"):

        @daft.cls(min_concurrency=0, max_concurrency=1)
        class MinZero:
            pass

    with pytest.raises(ValueError, match="min_concurrency for udf must be non-zero"):

        @daft.cls(min_concurrency=-1, max_concurrency=1)
        class MinNegative:
            pass

    with pytest.raises(ValueError, match="max_concurrency for udf must be non-zero"):

        @daft.cls(min_concurrency=1, max_concurrency=-1)
        class MaxNegative:
            pass

    with pytest.raises(ValueError, match="min_concurrency for udf requires max_concurrency"):

        @daft.cls(min_concurrency=1)
        class MinWithoutMax:
            pass

    with pytest.raises(ValueError, match="min_concurrency for udf must be less than or equal to max_concurrency"):

        @daft.cls(min_concurrency=3, max_concurrency=2)
        class MinGreaterThanMax:
            pass


def test_actor_pool_scales_active_handles_with_backlog(monkeypatch):
    from daft.execution import ray_actor_pool_udf

    teardown_calls = []

    class FakeHandle:
        def __init__(self, name: str):
            self.name = name

        def teardown(self) -> None:
            teardown_calls.append(self.name)

    created_handles = []

    async def fake_start_actor(self, rank: int):
        handle = FakeHandle(f"actor-{rank}")
        created_handles.append(handle.name)
        return handle

    monkeypatch.setattr(ray_actor_pool_udf.UDFActorPool, "_start_actor", fake_start_actor)

    pool = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=3,
        udf_options={},
        timeout=1,
        actor_name="test",
    )

    async def run_test():
        await pool.start()

        assert [handle.name for handle in await pool.get_actor_handles(pending_tasks=1)] == ["actor-0"]
        assert [handle.name for handle in await pool.get_actor_handles(pending_tasks=3)] == [
            "actor-0",
            "actor-1",
            "actor-2",
        ]
        assert created_handles == ["actor-0", "actor-1", "actor-2"]

        assert [handle.name for handle in await pool.get_actor_handles(pending_tasks=1)] == ["actor-0"]
        assert teardown_calls == []

        pool.cleanup_retired_actors()
        assert teardown_calls == ["actor-1", "actor-2"]

    asyncio.run(run_test())


def test_start_actor_pool_preserves_requested_max_when_cluster_can_grow(monkeypatch):
    from daft.execution import ray_actor_pool_udf

    async def fake_start(self):
        pass

    monkeypatch.setattr(ray_actor_pool_udf.Expression, "_from_pyexpr", lambda projection: projection)
    monkeypatch.setattr(ray_actor_pool_udf, "ExpressionsProjection", lambda expressions: expressions)
    monkeypatch.setattr(ray_actor_pool_udf.UDFActorPool, "start", fake_start)
    monkeypatch.setattr(ray_actor_pool_udf.ray, "nodes", lambda: [{"Alive": True, "Resources": {"CPU": 2}}])
    monkeypatch.setattr(ray_actor_pool_udf.ray, "cluster_resources", lambda: {"CPU": 2})
    monkeypatch.setattr(ray_actor_pool_udf.ray, "available_resources", lambda: {"CPU": 2})

    async def run_test():
        pool = await ray_actor_pool_udf.start_udf_actor_pool(
            projection=SimpleNamespace(),
            min_actors=1,
            max_actors=4,
            num_gpus_per_actor=0,
            num_cpus_per_actor=1,
            memory_per_actor=0,
            ray_options=None,
            timeout=1,
        )

        assert pool.max_actors == 4

    asyncio.run(run_test())


def test_actor_pool_refreshes_cluster_capacity_after_interval(monkeypatch):
    from daft.execution import ray_actor_pool_udf

    class FakeHandle:
        def __init__(self, name: str):
            self.name = name

        def teardown(self) -> None:
            pass

    async def fake_start_actor(self, rank: int):
        return FakeHandle(f"actor-{rank}")

    monkeypatch.setattr(ray_actor_pool_udf.UDFActorPool, "_start_actor", fake_start_actor)

    now = 0.0
    available_cpus = 1.0
    cluster_cpus = 1.0
    available_resource_queries = 0
    cluster_resource_queries = 0

    def monotonic_time():
        return now

    def available_resources():
        nonlocal available_resource_queries
        available_resource_queries += 1
        return {"CPU": available_cpus, "GPU": 0}

    def cluster_resources():
        nonlocal cluster_resource_queries
        cluster_resource_queries += 1
        return {"CPU": cluster_cpus, "GPU": 0}

    resource_manager = ray_actor_pool_udf.UDFActorPoolResourceManager(
        total_cpus=1,
        total_gpus=0,
        available_resources_getter=available_resources,
        cluster_resources_getter=cluster_resources,
        resource_refresh_interval_s=60,
        monotonic_time_getter=monotonic_time,
    )
    pool = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=4,
        udf_options={},
        timeout=1,
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )
    pool._initial_above_min_deferrals_remaining = 0

    async def run_test():
        nonlocal now, available_cpus, cluster_cpus

        await pool.start()
        assert available_resource_queries >= 1
        assert cluster_resource_queries == 1

        available_cpus = 3
        cluster_cpus = 4
        now = 30
        assert len(await pool.get_actor_handles(pending_tasks=4)) == 1
        assert available_resource_queries >= 2
        assert cluster_resource_queries == 1

        now = 61
        assert len(await pool.get_actor_handles(pending_tasks=4)) == 4
        assert available_resource_queries >= 3
        assert cluster_resource_queries == 2

    asyncio.run(run_test())


def test_global_actor_pool_resource_manager_survives_cluster_resize(monkeypatch):
    from daft.execution import ray_actor_pool_udf

    monkeypatch.setattr(ray_actor_pool_udf, "_GLOBAL_UDF_ACTOR_POOL_RESOURCE_MANAGER", None)

    first_manager = ray_actor_pool_udf._get_global_udf_actor_pool_resource_manager(
        total_cpus=2,
        total_gpus=0,
        available_resources_getter=lambda: {"CPU": 2},
    )
    pool = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=4,
        udf_options={},
        timeout=1,
        resource_manager=first_manager,
    )
    first_manager.register(pool)

    resized_manager = ray_actor_pool_udf._get_global_udf_actor_pool_resource_manager(
        total_cpus=4,
        total_gpus=0,
        available_resources_getter=lambda: {"CPU": 4},
    )

    assert resized_manager is first_manager
    assert pool in resized_manager._pools
    assert resized_manager.total_cpus == 4


def test_actor_pool_keeps_at_least_min_and_at_most_max_active_handles(monkeypatch):
    from daft.execution import ray_actor_pool_udf

    class FakeHandle:
        def __init__(self, name: str):
            self.name = name

        def teardown(self) -> None:
            pass

    async def fake_start_actor(self, rank: int):
        return FakeHandle(f"actor-{rank}")

    monkeypatch.setattr(ray_actor_pool_udf.UDFActorPool, "_start_actor", fake_start_actor)

    pool = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=2,
        max_actors=4,
        udf_options={},
        timeout=1,
        actor_name=None,
    )

    async def run_test():
        await pool.start()

        assert [handle.name for handle in await pool.get_actor_handles(pending_tasks=0)] == ["actor-0", "actor-1"]
        assert [handle.name for handle in await pool.get_actor_handles(pending_tasks=99)] == [
            "actor-0",
            "actor-1",
            "actor-2",
            "actor-3",
        ]

    asyncio.run(run_test())


def test_actor_pool_reuses_retired_handles_when_scaling_back_up(monkeypatch):
    from daft.execution import ray_actor_pool_udf

    class FakeHandle:
        def __init__(self, name: str):
            self.name = name

        def teardown(self) -> None:
            pass

    created_handles = []

    async def fake_start_actor(self, rank: int):
        handle = FakeHandle(f"actor-{rank}")
        created_handles.append(handle.name)
        return handle

    monkeypatch.setattr(ray_actor_pool_udf.UDFActorPool, "_start_actor", fake_start_actor)

    pool = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=3,
        udf_options={},
        timeout=1,
        actor_name=None,
    )

    async def run_test():
        await pool.start()

        assert [handle.name for handle in await pool.get_actor_handles(pending_tasks=3)] == [
            "actor-0",
            "actor-1",
            "actor-2",
        ]
        assert [handle.name for handle in await pool.get_actor_handles(pending_tasks=1)] == ["actor-0"]
        assert [handle.name for handle in await pool.get_actor_handles(pending_tasks=3)] == [
            "actor-0",
            "actor-1",
            "actor-2",
        ]
        assert created_handles == ["actor-0", "actor-1", "actor-2"]

    asyncio.run(run_test())


def test_actor_pool_preserves_capacity_for_downstream_min(monkeypatch):
    from daft.execution import ray_actor_pool_udf

    teardown_calls = []

    class FakeHandle:
        def __init__(self, name: str):
            self.name = name

        def teardown(self) -> None:
            teardown_calls.append(self.name)

    async def fake_start_actor(self, rank: int):
        return FakeHandle(f"{self.actor_name}-{rank}")

    monkeypatch.setattr(ray_actor_pool_udf.UDFActorPool, "_start_actor", fake_start_actor)

    resource_manager = ray_actor_pool_udf.UDFActorPoolResourceManager(total_cpus=2, total_gpus=0)
    upstream = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=2,
        udf_options={},
        timeout=1,
        actor_name="upstream",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )
    downstream = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=1,
        udf_options={},
        timeout=1,
        actor_name="downstream",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )
    downstream = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=2,
        udf_options={},
        timeout=1,
        actor_name="downstream",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )

    async def run_test():
        await upstream.start()
        assert [handle.name for handle in await upstream.get_actor_handles(pending_tasks=3)] == ["upstream-0"]
        assert [handle.name for handle in await upstream.get_actor_handles(pending_tasks=3)] == ["upstream-0"]

        await downstream.start()

        assert [handle.name for handle in await upstream.get_actor_handles(pending_tasks=3)] == ["upstream-0"]
        assert [handle.name for handle in await downstream.get_actor_handles(pending_tasks=1)] == ["downstream-0"]
        assert teardown_calls == []

    asyncio.run(run_test())


def test_actor_pool_defers_initial_above_min_until_downstream_can_register(monkeypatch):
    from daft.execution import ray_actor_pool_udf

    class FakeHandle:
        def __init__(self, name: str):
            self.name = name

        def teardown(self) -> None:
            pass

    async def fake_start_actor(self, rank: int):
        return FakeHandle(f"{self.actor_name}-{rank}")

    monkeypatch.setattr(ray_actor_pool_udf.UDFActorPool, "_start_actor", fake_start_actor)

    resource_manager = ray_actor_pool_udf.UDFActorPoolResourceManager(total_cpus=2, total_gpus=0)
    upstream = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=2,
        udf_options={},
        timeout=1,
        actor_name="upstream",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )
    downstream = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=2,
        udf_options={},
        timeout=1,
        actor_name="downstream",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )

    async def run_test():
        await upstream.start()

        assert [handle.name for handle in await upstream.get_actor_handles(pending_tasks=3)] == ["upstream-0"]

        await downstream.start()

        assert [handle.name for handle in await downstream.get_actor_handles(pending_tasks=1)] == ["downstream-0"]

    asyncio.run(run_test())


def test_actor_pool_resource_reclaim_waits_for_actor_handle_lease(monkeypatch):
    from daft.execution import ray_actor_pool_udf

    teardown_calls = []

    class FakeHandle:
        def __init__(self, name: str):
            self.name = name

        def teardown(self) -> None:
            teardown_calls.append(self.name)

    async def fake_start_actor(self, rank: int):
        return FakeHandle(f"{self.actor_name}-{rank}")

    monkeypatch.setattr(ray_actor_pool_udf.UDFActorPool, "_start_actor", fake_start_actor)

    resource_manager = ray_actor_pool_udf.UDFActorPoolResourceManager(total_cpus=4, total_gpus=0)
    upstream = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=2,
        udf_options={},
        timeout=1,
        actor_name="upstream",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )

    async def run_test():
        await upstream.start()
        assert [handle.name for handle in await upstream.get_actor_handles(pending_tasks=3)] == [
            "upstream-0",
            "upstream-1",
        ]

        upstream.lease_actor_handles()
        upstream._release_one_above_min_for_resource_reclaim()

        assert [handle.name for handle in upstream._active_actors] == ["upstream-0"]
        assert [handle.name for handle in upstream._retired_actors] == ["upstream-1"]
        assert teardown_calls == []

        upstream.release_actor_handles()

        assert teardown_calls == ["upstream-1"]
        assert upstream._retired_actors == []

    asyncio.run(run_test())


def test_actor_pool_does_not_create_leased_upstream_extra_that_blocks_downstream_min(monkeypatch):
    from daft.execution import ray_actor_pool_udf

    class FakeHandle:
        def __init__(self, name: str):
            self.name = name

        def teardown(self) -> None:
            pass

    async def fake_start_actor(self, rank: int):
        return FakeHandle(f"{self.actor_name}-{rank}")

    monkeypatch.setattr(ray_actor_pool_udf.UDFActorPool, "_start_actor", fake_start_actor)

    resource_manager = ray_actor_pool_udf.UDFActorPoolResourceManager(total_cpus=2, total_gpus=0)
    upstream = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=2,
        udf_options={},
        timeout=1,
        actor_name="upstream",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )
    downstream = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=2,
        udf_options={},
        timeout=1,
        actor_name="downstream",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )

    async def run_test():
        await upstream.start()
        assert [handle.name for handle in await upstream.get_actor_handles(pending_tasks=3)] == ["upstream-0"]
        assert [handle.name for handle in await upstream.get_actor_handles(pending_tasks=3)] == ["upstream-0"]
        upstream.lease_actor_handles()

        assert [handle.name for handle in await downstream.get_actor_handles(pending_tasks=1)] == ["downstream-0"]

        upstream.release_actor_handles()

    asyncio.run(run_test())


def test_actor_pool_defers_middle_pool_above_min_until_its_downstream_can_register(monkeypatch):
    from daft.execution import ray_actor_pool_udf

    class FakeHandle:
        def __init__(self, name: str):
            self.name = name

        def teardown(self) -> None:
            pass

    async def fake_start_actor(self, rank: int):
        return FakeHandle(f"{self.actor_name}-{rank}")

    monkeypatch.setattr(ray_actor_pool_udf.UDFActorPool, "_start_actor", fake_start_actor)

    resource_manager = ray_actor_pool_udf.UDFActorPoolResourceManager(total_cpus=3, total_gpus=0)
    upstream = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=2,
        udf_options={},
        timeout=1,
        actor_name="upstream",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )
    middle = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=2,
        udf_options={},
        timeout=1,
        actor_name="middle",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )
    downstream = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=2,
        udf_options={},
        timeout=1,
        actor_name="downstream",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )

    async def run_test():
        await upstream.start()
        await middle.start()

        assert [handle.name for handle in await middle.get_actor_handles(pending_tasks=3)] == ["middle-0"]

        await downstream.start()

        assert [handle.name for handle in await downstream.get_actor_handles(pending_tasks=1)] == ["downstream-0"]

    asyncio.run(run_test())


def test_actor_pool_middle_scales_up_then_down_for_slow_downstream(monkeypatch):
    from daft.execution import ray_actor_pool_udf

    teardown_calls = []

    class FakeHandle:
        def __init__(self, name: str):
            self.name = name

        def teardown(self) -> None:
            teardown_calls.append(self.name)

    async def fake_start_actor(self, rank: int):
        return FakeHandle(f"{self.actor_name}-{rank}")

    monkeypatch.setattr(ray_actor_pool_udf.UDFActorPool, "_start_actor", fake_start_actor)

    # Six actor slots allow the middle UDF to use two extra slots after the slow tail has reserved
    # its min actor, while still leaving one extra slot for the tail: first=1, middle=3, tail=2.
    # Once the tail asks for backlog capacity, the middle UDF is treated as upstream and is scaled
    # back to min so the tail can scale up to 3.
    resource_manager = ray_actor_pool_udf.UDFActorPoolResourceManager(total_cpus=6, total_gpus=0)
    first = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=1,
        udf_options={},
        timeout=1,
        actor_name="first",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )
    middle = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=3,
        udf_options={},
        timeout=1,
        actor_name="middle",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )
    slow_tail = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=3,
        udf_options={},
        timeout=1,
        actor_name="slow_tail",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )

    async def run_test():
        await first.start()
        await middle.start()
        await slow_tail.start()

        assert [handle.name for handle in await middle.get_actor_handles(pending_tasks=3)] == [
            "middle-0",
            "middle-1",
            "middle-2",
        ]

        assert [handle.name for handle in await slow_tail.get_actor_handles(pending_tasks=3)] == ["slow_tail-0"]
        assert [handle.name for handle in await slow_tail.get_actor_handles(pending_tasks=3)] == ["slow_tail-0"]
        assert [handle.name for handle in await slow_tail.get_actor_handles(pending_tasks=3)] == [
            "slow_tail-0",
            "slow_tail-1",
            "slow_tail-2",
        ]
        assert [handle.name for handle in await middle.get_actor_handles(pending_tasks=3)] == ["middle-0"]
        middle.cleanup_retired_actors()
        assert sorted(teardown_calls) == ["middle-1", "middle-2"]

    asyncio.run(run_test())


def test_actor_pool_counts_pending_actor_starts_as_reserved_resources(monkeypatch):
    from daft.execution import ray_actor_pool_udf

    pool1_actor_started = asyncio.Event()
    pool1_actor_can_finish = asyncio.Event()

    class FakeHandle:
        def __init__(self, name: str):
            self.name = name

        def teardown(self) -> None:
            pass

    async def fake_start_actor(self, rank: int):
        if self.actor_name == "pool1" and rank == 0:
            pool1_actor_started.set()
            await pool1_actor_can_finish.wait()
        return FakeHandle(f"{self.actor_name}-{rank}")

    monkeypatch.setattr(ray_actor_pool_udf.UDFActorPool, "_start_actor", fake_start_actor)

    resource_manager = ray_actor_pool_udf.UDFActorPoolResourceManager(total_cpus=1, total_gpus=0)
    pool1 = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=1,
        udf_options={},
        timeout=1,
        actor_name="pool1",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )
    pool2 = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=1,
        udf_options={},
        timeout=1,
        actor_name="pool2",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )

    async def run_test():
        pool1_start_task = asyncio.create_task(pool1.start())
        await pool1_actor_started.wait()

        with pytest.raises(RuntimeError):
            await pool2.start()

        pool1_actor_can_finish.set()
        await pool1_start_task

    asyncio.run(run_test())


def test_actor_pool_uses_dynamic_available_resources_for_above_min_capacity(monkeypatch):
    from daft.execution import ray_actor_pool_udf

    class FakeHandle:
        def __init__(self, name: str):
            self.name = name

        def teardown(self) -> None:
            pass

    async def fake_start_actor(self, rank: int):
        return FakeHandle(f"{self.actor_name}-{rank}")

    monkeypatch.setattr(ray_actor_pool_udf.UDFActorPool, "_start_actor", fake_start_actor)

    available_cpus = 3
    resource_manager = ray_actor_pool_udf.UDFActorPoolResourceManager(
        total_cpus=4,
        total_gpus=0,
        available_resources_getter=lambda: {"CPU": available_cpus, "GPU": 0},
        resource_refresh_interval_s=0,
    )
    pool = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=4,
        udf_options={},
        timeout=1,
        actor_name="pool",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )
    downstream = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=1,
        udf_options={},
        timeout=1,
        actor_name="downstream",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )

    async def run_test():
        nonlocal available_cpus

        await pool.start()
        await downstream.start()
        # Another Daft job takes resources. This query already owns two actors across its pools, so
        # it should only be allowed to grow to three total actors: two currently owned + one
        # currently available CPU.
        available_cpus = 1
        assert [handle.name for handle in await pool.get_actor_handles(pending_tasks=4)] == ["pool-0", "pool-1"]
        available_cpus = 0

        # Once the other job releases resources, this pool can observe the larger dynamic budget and
        # grow again.
        available_cpus = 1
        assert [handle.name for handle in await pool.get_actor_handles(pending_tasks=4)] == [
            "pool-0",
            "pool-1",
            "pool-2",
        ]

    asyncio.run(run_test())


def test_actor_pool_prioritizes_downstream_backlog_over_upstream_above_min(monkeypatch):
    from daft.execution import ray_actor_pool_udf

    teardown_calls = []

    class FakeHandle:
        def __init__(self, name: str):
            self.name = name

        def teardown(self) -> None:
            teardown_calls.append(self.name)

    async def fake_start_actor(self, rank: int):
        return FakeHandle(f"{self.actor_name}-{rank}")

    monkeypatch.setattr(ray_actor_pool_udf.UDFActorPool, "_start_actor", fake_start_actor)

    resource_manager = ray_actor_pool_udf.UDFActorPoolResourceManager(total_cpus=4, total_gpus=0)
    upstream = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=2,
        udf_options={},
        timeout=1,
        actor_name="upstream",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )
    downstream = ray_actor_pool_udf.UDFActorPool(
        expr_projection=SimpleNamespace(),
        min_actors=1,
        max_actors=2,
        udf_options={},
        timeout=1,
        actor_name="downstream",
        cpus_per_actor=1,
        resource_manager=resource_manager,
    )

    async def run_test():
        await upstream.start()
        assert [handle.name for handle in await upstream.get_actor_handles(pending_tasks=3)] == [
            "upstream-0",
            "upstream-1",
        ]
        await downstream.start()
        resource_manager.total_cpus = 3

        assert [handle.name for handle in await downstream.get_actor_handles(pending_tasks=3)] == ["downstream-0"]
        assert [handle.name for handle in await downstream.get_actor_handles(pending_tasks=3)] == ["downstream-0"]
        assert [handle.name for handle in await downstream.get_actor_handles(pending_tasks=3)] == [
            "downstream-0",
            "downstream-1",
        ]
        assert [handle.name for handle in await upstream.get_actor_handles(pending_tasks=3)] == ["upstream-0"]
        assert teardown_calls == ["upstream-1"]

    asyncio.run(run_test())


@pytest.mark.parametrize("concurrency", [None, 1, 2])
def test_cls_async_method(concurrency):
    df = daft.from_pydict({"a": [1, 2, 3], "b": ["foo", "bar", "baz"]})

    @daft.cls(max_concurrency=concurrency)
    class AsyncProcessor:
        def __init__(self, delay: float):
            self.delay = delay

        async def process(self, x: int) -> int:
            await asyncio.sleep(self.delay)
            return x * 2

    processor = AsyncProcessor(0.01)
    result = df.select(processor.process(df["a"]), "b")
    result = result.to_pydict()
    assert sorted(result["a"]) == [2, 4, 6]
    assert sorted(result["b"]) == ["bar", "baz", "foo"]


@pytest.mark.parametrize("concurrency", [None, 2])
def test_cls_generator_method(concurrency):
    df = daft.from_pydict({"id": [0, 1, 2], "n": [0, 2, 3]})

    @daft.cls(max_concurrency=concurrency)
    class RepeatGenerator:
        def __init__(self, value: str):
            self.value = value

        def generate(self, n: int) -> Iterator[str]:
            for _ in range(n):
                yield self.value

    gen = RepeatGenerator("x")
    result = df.select("id", gen.generate(df["n"])).collect()
    assert result.to_pydict() == {
        "id": [0, 1, 1, 2, 2, 2],
        "n": [None, "x", "x", "x", "x", "x"],
    }


@pytest.mark.parametrize("concurrency", [None, 2])
def test_cls_unnest_struct(concurrency):
    df = daft.from_pydict({"a": [1, 2, 3], "b": ["foo", "bar", "baz"]})

    @daft.cls(max_concurrency=concurrency)
    class Processor:
        def __init__(self, multiplier: int):
            self.multiplier = multiplier

        @daft.method(
            return_dtype=DataType.struct({"doubled": DataType.int64(), "name": DataType.string()}),
            unnest=True,
        )
        def process(self, x: int):
            return {"doubled": x * self.multiplier, "name": f"value_{x}"}

    processor = Processor(2)
    result = df.select("b", processor.process(df["a"]))
    assert result.to_pydict() == {
        "b": ["foo", "bar", "baz"],
        "doubled": [2, 4, 6],
        "name": ["value_1", "value_2", "value_3"],
    }


def test_cls_multiple_methods():
    df = daft.from_pydict({"text": ["hello", "world", "daft"]})

    @daft.cls
    class TextProcessor:
        def __init__(self, prefix: str):
            self.prefix = prefix

        def add_prefix(self, text: str) -> str:
            return f"{self.prefix}{text}"

        @daft.method(return_dtype=DataType.list(DataType.string()))
        def split_chars(self, text: str):
            return list(text)

        def length(self, text: str) -> int:
            return len(text)

    processor = TextProcessor("pre_")
    result = df.select(
        processor.add_prefix(df["text"]).alias("prefixed"),
        processor.split_chars(df["text"]).alias("chars"),
        processor.length(df["text"]).alias("len"),
    )
    expected = {
        "prefixed": ["pre_hello", "pre_world", "pre_daft"],
        "chars": [list("hello"), list("world"), list("daft")],
        "len": [5, 5, 4],
    }
    assert result.to_pydict() == expected


@pytest.mark.parametrize("concurrency", [None, 2])
def test_cls_with_complex_init(concurrency):
    df = daft.from_pydict({"a": [1, 2, 3], "b": ["foo", "bar", "baz"]})

    @daft.cls(max_concurrency=concurrency)
    class Calculator:
        def __init__(self, multiplier: int, offset: int, name: str):
            self.multiplier = multiplier
            self.offset = offset
            self.name = name

        def compute(self, x: int) -> int:
            return (x * self.multiplier) + self.offset

        def get_name(self, x: int) -> str:
            return f"{self.name}_{x}"

    calc = Calculator(multiplier=10, offset=5, name="calc")
    result = df.select(
        calc.compute(df["a"]).alias("result"),
        calc.get_name(df["a"]).alias("name"),
        "b",
    )
    assert result.to_pydict() == {
        "result": [15, 25, 35],
        "name": ["calc_1", "calc_2", "calc_3"],
        "b": ["foo", "bar", "baz"],
    }


@pytest.mark.parametrize("concurrency", [None, 2])
def test_cls_with_list_operations(concurrency):
    df = daft.from_pydict({"id": [0, 1, 2], "lists": [[1, 2, 3], [4, 5], [6, 7, 8, 9]]})

    @daft.cls(max_concurrency=concurrency)
    class ListProcessor:
        def __init__(self, threshold: int):
            self.threshold = threshold

        @daft.method(return_dtype=DataType.list(DataType.int64()))
        def filter_above(self, lst):
            return [x for x in lst if x > self.threshold]

        def count_above(self, lst) -> int:
            return sum(1 for x in lst if x > self.threshold)

    processor = ListProcessor(5)
    result = df.select(
        "id",
        processor.filter_above(df["lists"]).alias("filtered"),
        processor.count_above(df["lists"]).alias("count"),
    )
    assert result.to_pydict() == {
        "id": [0, 1, 2],
        "filtered": [[], [], [6, 7, 8, 9]],
        "count": [0, 0, 4],
    }


@pytest.mark.parametrize("concurrency", [None, 2])
def test_cls_batch_method(concurrency):
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9], "d": [10, 11, 12]})

    @daft.cls(max_concurrency=concurrency)
    class BatchAdder:
        def __init__(self, offset: int):
            self.offset = offset

        @daft.method.batch(return_dtype=DataType.int64())
        def add(self, a: daft.Series, b: daft.Series) -> daft.Series:
            import pyarrow.compute as pc

            a_arrow = a.to_arrow()
            b_arrow = b.to_arrow()
            result = pc.add(a_arrow, b_arrow)
            result = pc.add(result, self.offset)
            return daft.Series.from_arrow(result)

    adder = BatchAdder(10)
    result = df.select(adder.add(df["a"], df["b"]))
    assert result.to_pydict() == {"a": [15, 17, 19]}


def test_cls_batch_method_scalar_eval():
    @daft.cls
    class BatchMultiplier:
        def __init__(self, factor: int):
            self.factor = factor

        @daft.method.batch(return_dtype=DataType.int64())
        def multiply(self, a: daft.Series) -> daft.Series:
            import pyarrow.compute as pc

            a_arrow = a.to_arrow()
            result = pc.multiply(a_arrow, self.factor)
            return daft.Series.from_arrow(result)

    multiplier = BatchMultiplier(5)
    # When called with a scalar, should execute eagerly
    a = daft.Series.from_pylist([1, 2, 3])
    assert multiplier.multiply(a).to_pylist() == [5, 10, 15]


@pytest.mark.parametrize("concurrency", [None, 1, 2])
def test_cls_async_batch_method(concurrency):
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})

    @daft.cls(max_concurrency=concurrency)
    class AsyncBatchProcessor:
        def __init__(self, delay: float):
            self.delay = delay

        @daft.method.batch(return_dtype=DataType.int64())
        async def process(self, a: daft.Series) -> daft.Series:
            await asyncio.sleep(self.delay)
            return a

    processor = AsyncBatchProcessor(delay=0.01)
    result = df.select(processor.process(df["a"]))
    assert sorted(result.to_pydict()["a"]) == [1, 2, 3]


def test_cls_max_concurrency_zero():
    with pytest.raises(ValueError, match="max_concurrency for udf must be non-zero"):

        @daft.cls(max_concurrency=0)
        class MaxConcurrencyZero:
            def __init__(self):
                pass

            def __call__(self, x: int) -> int:
                return x

        df = daft.from_pydict({"a": [1, 2, 3]})
        result = df.select(MaxConcurrencyZero()(df["a"])).to_pydict()
        assert result == {"a": [1, 2, 3]}


@pytest.mark.parametrize("gpus", [0.0, 0, 0.5, 1.0, 1, 2])
def test_cls_accepts_fractional_gpus(gpus):
    @daft.cls(gpus=gpus)
    class Repeat:
        def __init__(self, n: int):
            self.n = n

        def __call__(self, x) -> str:
            return x * self.n

    Repeat(2)


def test_cls_gpus_one_point_five_is_rejected():
    with pytest.raises(BaseException) as excinfo:

        @daft.cls(gpus=1.5)
        class BadRepeat:
            def __init__(self, n: int):
                self.n = n

            def __call__(self, x) -> str:
                return x * self.n

        BadRepeat(2)
    assert "num_gpus greater than 1 must be an integer" in str(excinfo.value)


def test_cls_method_inherits_retry_and_on_error_from_class_defaults():
    @daft.cls(max_retries=2, on_error="ignore")
    class InheritDefaults:
        @daft.method
        def rowwise(self, x: int) -> int:
            return x

        @daft.method.batch(return_dtype=DataType.int64())
        def batch(self, x: daft.Series) -> daft.Series:
            return x

    udf = InheritDefaults()
    assert udf.rowwise.max_retries == 2
    assert udf.rowwise.on_error == "ignore"
    assert udf.batch.max_retries == 2
    assert udf.batch.on_error == "ignore"


def test_cls_method_overrides_retry_and_on_error_per_method():
    @daft.cls(max_retries=0, on_error="raise")
    class MethodOverrides:
        @daft.method(max_retries=5, on_error="log")
        def rowwise(self, x: int) -> int:
            return x

        @daft.method.batch(return_dtype=DataType.int64(), max_retries=3, on_error="ignore")
        def batch(self, x: daft.Series) -> daft.Series:
            return x

    udf = MethodOverrides()
    assert udf.rowwise.max_retries == 5
    assert udf.rowwise.on_error == "log"
    assert udf.batch.max_retries == 3
    assert udf.batch.on_error == "ignore"
