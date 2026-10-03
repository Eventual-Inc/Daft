from __future__ import annotations

import json
import time

import pytest
import ray

import daft
from daft.context import execution_config_ctx
from tests.conftest import get_tests_daft_runner_name

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        get_tests_daft_runner_name() != "ray",
        reason="UDF dynamic autoscaling e2e test requires Ray runner",
    ),
]

_EVENT_LOG_PATH = None


def _configure_actor_pool_event_log(tmp_path, monkeypatch, filename):
    global _EVENT_LOG_PATH
    if _EVENT_LOG_PATH is None or not ray.is_initialized():
        _EVENT_LOG_PATH = tmp_path / filename
    _EVENT_LOG_PATH.write_text("")
    monkeypatch.setenv("DAFT_UDF_ACTOR_POOL_EVENT_LOG", str(_EVENT_LOG_PATH))
    return _EVENT_LOG_PATH


def _read_actor_pool_events(event_log_path):
    if not event_log_path.exists():
        return []
    return [json.loads(line) for line in event_log_path.read_text().splitlines() if line.strip()]


def _actor_event_name(event):
    actor_name_parts = event["actor_name"].split(".")
    return actor_name_parts[-2] if actor_name_parts[-1] == "__call__" else actor_name_parts[-1]


def _scale_sizes_by_actor_name(events):
    scale_sizes = {}
    for event in events:
        if event["event"] != "scale":
            continue
        actor_name = _actor_event_name(event)
        scale_sizes.setdefault(actor_name, []).append(event["active_actors"])
    return scale_sizes


def _ensure_ray_runner(num_cpus: int) -> None:
    if not ray.is_initialized():
        ray.init(num_cpus=num_cpus, include_dashboard=False, log_to_driver=False)
    assert ray.cluster_resources().get("CPU", 0) >= num_cpus
    daft.set_runner_ray(noop_if_initialized=True)


def test_actor_pool_udf_dynamic_autoscaling_e2e(tmp_path, monkeypatch):
    """Run an end-to-end @daft.cls UDF and assert the actor pool scales with queued work."""
    event_log_path = _configure_actor_pool_event_log(tmp_path, monkeypatch, "udf-actor-pool-events.jsonl")

    _ensure_ray_runner(num_cpus=6)

    @daft.cls(min_concurrency=1, max_concurrency=3, cpus=1)
    class SlowScaler:
        @daft.method.batch(return_dtype=daft.DataType.int64())
        def __call__(self, x):
            time.sleep(0.25)
            return x

    scaler = SlowScaler()
    df = daft.from_pydict({"x": list(range(18))}).repartition(6, "x")

    started = time.monotonic()
    result = df.select(scaler(df["x"])).to_pydict()
    elapsed = time.monotonic() - started

    assert sorted(result["x"]) == list(range(18))

    events = _read_actor_pool_events(event_log_path)
    active_sizes = [event["active_actors"] for event in events if event["event"] == "scale"]
    max_active = max(active_sizes, default=0)
    min_active = min(active_sizes, default=0)

    print(
        "UDF dynamic autoscaling e2e effect: "
        f"events={events}, min_active={min_active}, max_active={max_active}, elapsed={elapsed:.2f}s"
    )

    assert min_active == 1
    assert max_active == 3
    assert any(event["event"] == "cleanup_retired" for event in events)


def test_chained_actor_pool_udfs_dynamic_autoscaling_e2e(tmp_path, monkeypatch):
    """Run a long chain of @daft.cls UDFs with different speeds and verify each pool autoscales independently."""
    event_log_path = _configure_actor_pool_event_log(tmp_path, monkeypatch, "chained-udf-actor-pool-events.jsonl")

    _ensure_ray_runner(num_cpus=6)

    @daft.cls(min_concurrency=1, max_concurrency=3, cpus=0.5)
    class SlowFirstScaler:
        @daft.method.batch(return_dtype=daft.DataType.int64())
        def __call__(self, x):
            time.sleep(0.35)
            return [v + 1 for v in x]

    @daft.cls(min_concurrency=1, max_concurrency=2, cpus=0.5)
    class FastMiddleScaler:
        @daft.method.batch(return_dtype=daft.DataType.int64())
        def __call__(self, x):
            time.sleep(0.05)
            return [v * 2 for v in x]

    @daft.cls(min_concurrency=1, max_concurrency=3, cpus=0.5)
    class MediumLastScaler:
        @daft.method.batch(return_dtype=daft.DataType.int64())
        def __call__(self, x):
            time.sleep(0.15)
            return [v - 3 for v in x]

    slow_first = SlowFirstScaler()
    fast_middle = FastMiddleScaler()
    medium_last = MediumLastScaler()
    df = daft.from_pydict({"x": list(range(24))}).repartition(8, "x")

    started = time.monotonic()
    result = df.select(medium_last(fast_middle(slow_first(df["x"])))).to_pydict()
    elapsed = time.monotonic() - started

    assert sorted(result["x"]) == sorted([(x + 1) * 2 - 3 for x in range(24)])

    events = _read_actor_pool_events(event_log_path)
    scale_sizes = _scale_sizes_by_actor_name(events)

    print(
        "Chained UDF dynamic autoscaling e2e effect: "
        f"scale_sizes={scale_sizes}, events={events}, elapsed={elapsed:.2f}s"
    )

    expected_max_by_name = {
        "SlowFirstScaler": 3,
        "FastMiddleScaler": 2,
        "MediumLastScaler": 3,
    }
    assert expected_max_by_name.keys() <= scale_sizes.keys()
    for actor_name, expected_max in expected_max_by_name.items():
        assert min(scale_sizes[actor_name]) == 1
        assert max(scale_sizes[actor_name]) == expected_max
    assert sum(event["event"] == "cleanup_retired" for event in events) >= 3


def test_chained_actor_pool_udfs_prioritize_downstream_under_resource_pressure(tmp_path, monkeypatch):
    """When max concurrency exceeds resources, keep upstream at min and give downstream spare capacity."""
    event_log_path = _configure_actor_pool_event_log(tmp_path, monkeypatch, "resource-pressure-events.jsonl")
    _ensure_ray_runner(num_cpus=6)

    @daft.cls(min_concurrency=1, max_concurrency=2, cpus=2)
    class FastUpstream:
        @daft.method.batch(return_dtype=daft.DataType.int64())
        def __call__(self, x):
            time.sleep(0.05)
            return [v + 1 for v in x]

    @daft.cls(min_concurrency=1, max_concurrency=2, cpus=2)
    class SlowDownstream:
        @daft.method.batch(return_dtype=daft.DataType.int64())
        def __call__(self, x):
            time.sleep(0.35)
            return [v * 2 for v in x]

    with execution_config_ctx(actor_udf_ready_timeout=30, maintain_order=False, min_cpu_per_task=0):
        df = daft.from_pydict({"x": list(range(8))}).repartition(4, "x")
        result = df.select(SlowDownstream()(FastUpstream()(df["x"]))).to_pydict()
    assert sorted(result["x"]) == sorted([(x + 1) * 2 for x in range(8)])

    events = _read_actor_pool_events(event_log_path)
    scale_sizes = _scale_sizes_by_actor_name(events)

    print(f"Resource-pressure chained UDF autoscaling effect: scale_sizes={scale_sizes}, events={events}")

    assert max(scale_sizes["FastUpstream"]) == 1
    assert max(scale_sizes["SlowDownstream"]) == 2


def test_three_chained_actor_pool_udfs_prioritize_slowest_downstream(tmp_path, monkeypatch):
    """With one spare slot, keep first/middle UDFs at min and give the extra actor to the slow tail UDF."""
    event_log_path = _configure_actor_pool_event_log(
        tmp_path, monkeypatch, "three-stage-resource-pressure-events.jsonl"
    )
    _ensure_ray_runner(num_cpus=6)

    @daft.cls(min_concurrency=1, max_concurrency=2, cpus=1.5)
    class FastFirst:
        @daft.method.batch(return_dtype=daft.DataType.int64())
        def __call__(self, x):
            time.sleep(0.02)
            return [v + 1 for v in x]

    @daft.cls(min_concurrency=1, max_concurrency=2, cpus=1.5)
    class MediumSecond:
        @daft.method.batch(return_dtype=daft.DataType.int64())
        def __call__(self, x):
            time.sleep(0.10)
            return [v * 2 for v in x]

    @daft.cls(min_concurrency=1, max_concurrency=2, cpus=1.5)
    class SlowThird:
        @daft.method.batch(return_dtype=daft.DataType.int64())
        def __call__(self, x):
            time.sleep(0.35)
            return [v - 3 for v in x]

    with execution_config_ctx(actor_udf_ready_timeout=30, maintain_order=False, min_cpu_per_task=0):
        df = daft.from_pydict({"x": list(range(12))}).repartition(6, "x")
        result = df.select(SlowThird()(MediumSecond()(FastFirst()(df["x"])))).to_pydict()
    assert sorted(result["x"]) == sorted([(x + 1) * 2 - 3 for x in range(12)])

    events = _read_actor_pool_events(event_log_path)
    scale_sizes = _scale_sizes_by_actor_name(events)

    print(f"Three-stage resource-pressure chained UDF autoscaling effect: scale_sizes={scale_sizes}, events={events}")

    assert max(scale_sizes["FastFirst"]) == 1
    assert max(scale_sizes["MediumSecond"]) == 1
    assert max(scale_sizes["SlowThird"]) == 2


def test_three_chained_actor_pool_udfs_middle_scales_up_then_down_e2e(tmp_path, monkeypatch):
    """End-to-end: the middle UDF can scale up first, then scale back down when the slow tail needs capacity."""
    event_log_path = _configure_actor_pool_event_log(tmp_path, monkeypatch, "three-stage-middle-up-down-events.jsonl")
    _ensure_ray_runner(num_cpus=6)

    @daft.cls(min_concurrency=1, max_concurrency=1, cpus=1)
    class FastHead:
        @daft.method.batch(return_dtype=daft.DataType.int64())
        def __call__(self, x):
            time.sleep(0.02)
            return [v + 1 for v in x]

    @daft.cls(min_concurrency=1, max_concurrency=3, cpus=1)
    class BurstyMiddle:
        @daft.method.batch(return_dtype=daft.DataType.int64())
        def __call__(self, x):
            time.sleep(0.18)
            return [v * 2 for v in x]

    @daft.cls(min_concurrency=1, max_concurrency=3, cpus=1)
    class SlowTail:
        @daft.method.batch(return_dtype=daft.DataType.int64())
        def __call__(self, x):
            time.sleep(0.35)
            return [v - 3 for v in x]

    with execution_config_ctx(actor_udf_ready_timeout=30, maintain_order=False, min_cpu_per_task=0):
        df = daft.from_pydict({"x": list(range(36))}).repartition(12, "x")
        result = df.select(SlowTail()(BurstyMiddle()(FastHead()(df["x"])))).to_pydict()
    assert sorted(result["x"]) == sorted([(x + 1) * 2 - 3 for x in range(36)])

    events = _read_actor_pool_events(event_log_path)
    scale_sizes = _scale_sizes_by_actor_name(events)

    print(f"Three-stage middle-up-then-down UDF autoscaling effect: scale_sizes={scale_sizes}, events={events}")

    assert max(scale_sizes["FastHead"]) == 1
    assert max(scale_sizes["BurstyMiddle"]) == 3
    assert max(scale_sizes["SlowTail"]) == 2

    middle_sizes = scale_sizes["BurstyMiddle"]
    first_middle_max_idx = middle_sizes.index(3)
    assert 1 in middle_sizes[first_middle_max_idx + 1 :]
    assert any(
        event["event"] == "scale"
        and _actor_event_name(event) == "BurstyMiddle"
        and event["active_actors"] == 1
        and event["retired_actors"] > 0
        for event in events
    )
