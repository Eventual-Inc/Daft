from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from daft.expressions.expressions import Expression, ExpressionsProjection
from daft.recordbatch.micropartition import MicroPartition
from daft.runners.ray_compat import validate_and_normalize_ray_options

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ray.actor import ActorHandle as RayActorHandle

    from daft.daft import PyExpr, PyMicroPartition

try:
    import ray
except ImportError:
    raise

MAX_UDFACTOR_ACTOR_RESTARTS = 4
MAX_UDFACTOR_ACTOR_TASK_RETRIES = 4
UDF_ACTOR_POOL_RESOURCE_REFRESH_INTERVAL_S = 60.0


@ray.remote(
    max_restarts=MAX_UDFACTOR_ACTOR_RESTARTS,
    max_task_retries=MAX_UDFACTOR_ACTOR_TASK_RETRIES,
)
class UDFActor:
    def __init__(self, uninitialized_projection: ExpressionsProjection) -> None:
        self.projection = ExpressionsProjection([e._initialize_udfs() for e in uninitialized_projection])

    def get_node_id(self) -> str:
        return ray.get_runtime_context().get_node_id()

    def eval_input(self, input: PyMicroPartition) -> PyMicroPartition:
        mp = MicroPartition._from_pymicropartition(input)
        res = mp.eval_expression_list(self.projection)
        return res._micropartition


class UDFActorHandle:
    def __init__(self, actor_ref: RayActorHandle) -> None:
        self.actor = actor_ref

    def actor_id(self) -> str:
        return self.actor._actor_id.hex()

    async def eval_input(self, input: PyMicroPartition) -> PyMicroPartition:
        return await self.actor.eval_input.remote(input)

    def teardown(self) -> None:
        ray.kill(self.actor)


def get_ready_actors_by_location(
    actor_handles: list[UDFActorHandle],
) -> tuple[list[UDFActorHandle], list[UDFActorHandle]]:
    from ray._private.state import actors

    current_node_id = ray.get_runtime_context().get_node_id()

    local_actors = []
    remote_actors = []
    for actor_handle in actor_handles:
        actor_id = actor_handle.actor_id()
        actor_state = actors(actor_id)
        if actor_state["Address"]["NodeID"] == current_node_id:
            local_actors.append(actor_handle)
        else:
            remote_actors.append(actor_handle)

    return local_actors, remote_actors


class UDFActorPoolResourceManager:
    """Coordinates actor-pool resources across UDFs in the same Ray driver process.

    The manager treats ``min_concurrency`` as the cross-pool hard floor and
    ``max_concurrency`` as opportunistic capacity. Later-registered pools are
    typically downstream in a fused pipeline, so they may reclaim above-min
    actors from earlier pools when they need their min actors or when their own
    backlog warrants extra actors.
    """

    def __init__(
        self,
        total_cpus: float | None,
        total_gpus: float | None,
        available_resources_getter: Callable[[], dict[str, float]] | None = None,
        cluster_resources_getter: Callable[[], dict[str, float]] | None = None,
        resource_refresh_interval_s: float = UDF_ACTOR_POOL_RESOURCE_REFRESH_INTERVAL_S,
        monotonic_time_getter: Callable[[], float] = time.monotonic,
    ) -> None:
        self.total_cpus = total_cpus
        self.total_gpus = total_gpus
        self._available_resources_getter = available_resources_getter
        self._cluster_resources_getter = cluster_resources_getter
        self._resource_refresh_interval_s = resource_refresh_interval_s
        self._monotonic_time_getter = monotonic_time_getter
        self._last_resource_refresh_s: float | None = None
        self._pools: list[UDFActorPool] = []
        self._next_registration_order = 0

    def register(self, pool: UDFActorPool) -> None:
        if pool in self._pools:
            return
        pool._resource_registration_order = self._next_registration_order
        self._next_registration_order += 1
        self._pools.append(pool)

    def unregister(self, pool: UDFActorPool) -> None:
        if pool in self._pools:
            self._pools.remove(pool)

    def allocate_actor_count(self, pool: UDFActorPool, desired: int) -> int:
        """Return the actor count this pool may use right now.

        This may reclaim above-min actors from upstream pools. If the cluster is
        still saturated, above-min requests are reduced until they fit. Min
        requests remain hard requests and will surface as a clear runtime error
        when known registered pools cannot fit together.
        """
        self.register(pool)
        desired = min(pool.max_actors, max(pool.min_actors, desired))
        if desired > pool.min_actors:
            pool._has_requested_above_min = True
        if (
            self._has_downstream_pool(pool)
            and not self._registered_max_would_fit()
            and (
                self._has_downstream_above_min_request(pool)
                or not self._target_leaves_downstream_above_min_capacity(pool, desired)
            )
        ):
            # In a fused chain, downstream backlog is a stronger signal than
            # upstream backlog: if upstream produces faster than downstream can
            # consume, keep upstream at its min once a downstream pool has asked
            # for above-min capacity. Before that signal arrives, downstream min
            # actors are already reserved, so upstream pools may temporarily use
            # spare capacity only when doing so still leaves room for downstream
            # to scale above min while leased retired upstream actors drain.
            desired = pool.min_actors
        elif (
            desired > pool.min_actors
            and not self._has_downstream_pool(pool)
            and (
                (
                    not self._has_upstream_pool(pool)
                    and self._target_would_exceed_capacity(pool, desired + pool.max_actors)
                    and pool._initial_above_min_deferrals_remaining > 0
                )
                or (
                    self._has_upstream_pool(pool)
                    and self._target_would_exhaust_capacity(pool, desired)
                    and pool._initial_above_min_deferrals_remaining > 0
                )
            )
        ):
            # A downstream ActorUDF node initializes its pool before consuming
            # upstream input, but the upstream node may still receive one backlog
            # signal while the downstream task is being scheduled. Cap
            # opportunistic above-min allocations that would consume all known
            # capacity before downstream pools get a chance to register so that
            # pools get a chance to register and reserve their min actors before
            # upstream consumes spare cluster resources.
            pool._has_deferred_initial_above_min_request = True
            pool._initial_above_min_deferrals_remaining = max(0, pool._initial_above_min_deferrals_remaining - 1)
            desired = pool.min_actors
        current = len(pool._active_actors) + pool._pending_actor_starts
        if desired <= current:
            return desired

        self._reclaim_upstream_extras_for(pool, desired)
        if self._would_fit(pool, desired):
            self._reserve_pending_actor_starts(pool, desired)
            return desired

        if desired <= pool.min_actors:
            if self._available_resources_getter is not None and self._would_fit_static_cluster_capacity(pool, desired):
                self._reserve_pending_actor_starts(pool, desired)
                return desired
            raise RuntimeError(
                "Actor UDF min_concurrency cannot be satisfied with currently registered UDF pools "
                f"and cluster resources. actor_name={pool.actor_name!r}, min_concurrency={pool.min_actors}, "
                f"total_cpus={self.total_cpus}, total_gpus={self.total_gpus}."
            )

        allocated = desired
        while allocated > max(pool.min_actors, current) and not self._would_fit(pool, allocated):
            allocated -= 1
        if self._would_fit(pool, allocated):
            self._reserve_pending_actor_starts(pool, allocated)
            return allocated
        return current

    def _reserve_pending_actor_starts(self, pool: UDFActorPool, target_count: int) -> None:
        reusable_count = len(pool._active_actors) + len(pool._retired_actors) + pool._pending_actor_starts
        pool._pending_actor_starts += max(0, target_count - reusable_count)

    def _reclaim_upstream_extras_for(self, pool: UDFActorPool, target_count: int) -> None:
        while not self._would_fit(pool, target_count):
            victim = self._select_reclaim_victim(pool)
            if victim is None:
                return
            victim._release_one_above_min_for_resource_reclaim()

    def _select_reclaim_victim(self, pool: UDFActorPool) -> UDFActorPool | None:
        upstream_victims = [
            victim
            for victim in self._pools
            if victim is not pool
            and victim._resource_registration_order < pool._resource_registration_order
            and len(victim._active_actors) > victim.min_actors
        ]
        if not upstream_victims:
            return None
        return max(upstream_victims, key=lambda victim: len(victim._active_actors) - victim.min_actors)

    def _has_downstream_pool(self, pool: UDFActorPool) -> bool:
        return any(
            other_pool is not pool and other_pool._resource_registration_order > pool._resource_registration_order
            for other_pool in self._pools
        )

    def _has_downstream_above_min_request(self, pool: UDFActorPool) -> bool:
        return any(
            other_pool is not pool
            and other_pool._resource_registration_order > pool._resource_registration_order
            and other_pool._has_requested_above_min
            for other_pool in self._pools
        )

    def _target_leaves_downstream_above_min_capacity(self, pool: UDFActorPool, target_count: int) -> bool:
        downstream_pools = [
            other_pool
            for other_pool in self._pools
            if other_pool is not pool
            and other_pool._resource_registration_order > pool._resource_registration_order
            and other_pool.max_actors > other_pool.min_actors
        ]
        if not downstream_pools:
            return True
        for downstream_pool in downstream_pools:
            downstream_target = max(self._resource_count(downstream_pool), downstream_pool.min_actors + 1)
            if not self._would_fit_with_targets({pool: target_count, downstream_pool: downstream_target}):
                return False
        return True

    def _resource_count(self, pool: UDFActorPool) -> int:
        return len(pool._active_actors) + len(pool._retired_actors) + pool._pending_actor_starts

    def _would_fit_with_targets(self, target_counts: dict[UDFActorPool, int]) -> bool:
        target_cpus = 0.0
        target_gpus = 0.0
        for tracked_pool in self._pools:
            count = max(target_counts.get(tracked_pool, 0), self._resource_count(tracked_pool))
            target_cpus += tracked_pool.cpus_per_actor * count
            target_gpus += tracked_pool.gpus_per_actor * count

        total_cpus, total_gpus = self._effective_resource_limits()
        if total_cpus is not None and target_cpus > total_cpus + 1e-9:
            return False
        if total_gpus is not None and target_gpus > total_gpus + 1e-9:
            return False
        return True

    def _has_upstream_pool(self, pool: UDFActorPool) -> bool:
        return any(
            other_pool is not pool and other_pool._resource_registration_order < pool._resource_registration_order
            for other_pool in self._pools
        )

    def _registered_max_would_fit(self) -> bool:
        max_cpus = sum(pool.cpus_per_actor * pool.max_actors for pool in self._pools)
        max_gpus = sum(pool.gpus_per_actor * pool.max_actors for pool in self._pools)
        total_cpus, total_gpus = self._effective_resource_limits()
        if total_cpus is not None and max_cpus > total_cpus + 1e-9:
            return False
        if total_gpus is not None and max_gpus > total_gpus + 1e-9:
            return False
        return True

    def _target_would_exhaust_capacity(self, pool: UDFActorPool, target_count: int) -> bool:
        target_cpus = 0.0
        target_gpus = 0.0
        for tracked_pool in self._pools:
            count = target_count if tracked_pool is pool else len(tracked_pool._active_actors)
            target_cpus += tracked_pool.cpus_per_actor * count
            target_gpus += tracked_pool.gpus_per_actor * count

        total_cpus, total_gpus = self._effective_resource_limits()
        if total_cpus is not None and target_cpus > 0 and target_cpus >= total_cpus - 1e-9:
            return True
        if total_gpus is not None and target_gpus > 0 and target_gpus >= total_gpus - 1e-9:
            return True
        return False

    def _target_would_exceed_capacity(self, pool: UDFActorPool, target_count: int) -> bool:
        target_cpus = 0.0
        target_gpus = 0.0
        for tracked_pool in self._pools:
            count = target_count if tracked_pool is pool else len(tracked_pool._active_actors)
            target_cpus += tracked_pool.cpus_per_actor * count
            target_gpus += tracked_pool.gpus_per_actor * count

        total_cpus, total_gpus = self._effective_resource_limits()
        if total_cpus is not None and target_cpus > total_cpus + 1e-9:
            return True
        if total_gpus is not None and target_gpus > total_gpus + 1e-9:
            return True
        return False

    def _would_fit(self, pool: UDFActorPool, target_count: int) -> bool:
        target_cpus = 0.0
        target_gpus = 0.0
        for tracked_pool in self._pools:
            resource_count = self._resource_count(tracked_pool)
            count = max(target_count, resource_count) if tracked_pool is pool else resource_count
            target_cpus += tracked_pool.cpus_per_actor * count
            target_gpus += tracked_pool.gpus_per_actor * count

        total_cpus, total_gpus = self._effective_resource_limits()
        if total_cpus is not None and target_cpus > total_cpus + 1e-9:
            return False
        if total_gpus is not None and target_gpus > total_gpus + 1e-9:
            return False
        return True

    def _would_fit_static_cluster_capacity(self, pool: UDFActorPool, target_count: int) -> bool:
        target_cpus = 0.0
        target_gpus = 0.0
        for tracked_pool in self._pools:
            resource_count = self._resource_count(tracked_pool)
            count = max(target_count, resource_count) if tracked_pool is pool else resource_count
            target_cpus += tracked_pool.cpus_per_actor * count
            target_gpus += tracked_pool.gpus_per_actor * count

        if self.total_cpus is not None and target_cpus > self.total_cpus + 1e-9:
            return False
        if self.total_gpus is not None and target_gpus > self.total_gpus + 1e-9:
            return False
        return True

    def _effective_resource_limits(self) -> tuple[float | None, float | None]:
        if self._available_resources_getter is None:
            return self.total_cpus, self.total_gpus

        available_resources = self._available_resources_getter()
        self._refresh_cluster_resources_if_needed()
        owned_cpus = sum(pool.cpus_per_actor * self._resource_count(pool) for pool in self._pools)
        owned_gpus = sum(pool.gpus_per_actor * self._resource_count(pool) for pool in self._pools)
        available_cpus = available_resources.get("CPU", 0.0) + owned_cpus
        available_gpus = available_resources.get("GPU", 0.0) + owned_gpus
        total_cpus = min(self.total_cpus, available_cpus) if self.total_cpus is not None else available_cpus
        total_gpus = min(self.total_gpus, available_gpus) if self.total_gpus is not None else available_gpus
        return total_cpus, total_gpus

    def _refresh_cluster_resources_if_needed(self) -> None:
        if self._cluster_resources_getter is None:
            return
        now = self._monotonic_time_getter()
        should_refresh = (
            self._last_resource_refresh_s is None
            or now - self._last_resource_refresh_s >= self._resource_refresh_interval_s
        )
        if should_refresh:
            cluster_resources = self._cluster_resources_getter()
            self.total_cpus = cluster_resources.get("CPU", 0.0)
            self.total_gpus = cluster_resources.get("GPU", 0.0)
            self._last_resource_refresh_s = now


_GLOBAL_UDF_ACTOR_POOL_RESOURCE_MANAGER: UDFActorPoolResourceManager | None = None


def _get_global_udf_actor_pool_resource_manager(
    total_cpus: float | None,
    total_gpus: float | None,
    available_resources_getter: Callable[[], dict[str, float]] | None = None,
    cluster_resources_getter: Callable[[], dict[str, float]] | None = None,
) -> UDFActorPoolResourceManager:
    global _GLOBAL_UDF_ACTOR_POOL_RESOURCE_MANAGER
    if _GLOBAL_UDF_ACTOR_POOL_RESOURCE_MANAGER is None:
        _GLOBAL_UDF_ACTOR_POOL_RESOURCE_MANAGER = UDFActorPoolResourceManager(
            total_cpus,
            total_gpus,
            available_resources_getter=available_resources_getter,
            cluster_resources_getter=cluster_resources_getter,
        )
    else:
        _GLOBAL_UDF_ACTOR_POOL_RESOURCE_MANAGER.total_cpus = total_cpus
        _GLOBAL_UDF_ACTOR_POOL_RESOURCE_MANAGER.total_gpus = total_gpus
        _GLOBAL_UDF_ACTOR_POOL_RESOURCE_MANAGER._available_resources_getter = available_resources_getter
        _GLOBAL_UDF_ACTOR_POOL_RESOURCE_MANAGER._cluster_resources_getter = cluster_resources_getter
        _GLOBAL_UDF_ACTOR_POOL_RESOURCE_MANAGER._last_resource_refresh_s = None
    return _GLOBAL_UDF_ACTOR_POOL_RESOURCE_MANAGER


class UDFActorPool:
    def __init__(
        self,
        expr_projection: ExpressionsProjection,
        min_actors: int,
        max_actors: int,
        udf_options: dict[str, Any],
        timeout: int,
        actor_name: str | None = None,
        cpus_per_actor: float | None = None,
        gpus_per_actor: float | None = None,
        resource_manager: UDFActorPoolResourceManager | None = None,
    ) -> None:
        self.expr_projection = expr_projection
        self.min_actors = min_actors
        self.max_actors = max_actors
        self.udf_options = udf_options
        self.timeout = timeout
        self.actor_name = actor_name
        self.cpus_per_actor = float(cpus_per_actor if cpus_per_actor is not None else udf_options.get("num_cpus", 1.0))
        self.gpus_per_actor = float(gpus_per_actor if gpus_per_actor is not None else udf_options.get("num_gpus", 0.0))
        self.resource_manager = resource_manager
        self._resource_registration_order = -1
        self._has_deferred_initial_above_min_request = False
        self._has_requested_above_min = False
        self._initial_above_min_deferrals_remaining = 2
        self._pending_actor_starts = 0
        self._actor_handle_leases = 0
        self._active_actors: list[UDFActorHandle] = []
        self._retired_actors: list[UDFActorHandle] = []
        self._next_actor_rank = 0

    def _record_event(self, event: str, pending_tasks: int | None = None) -> None:
        event_log_path = os.environ.get("DAFT_UDF_ACTOR_POOL_EVENT_LOG")
        if event_log_path is None:
            return
        record = {
            "event": event,
            "actor_name": self.actor_name,
            "pending_tasks": pending_tasks,
            "min_actors": self.min_actors,
            "max_actors": self.max_actors,
            "active_actors": len(self._active_actors),
            "retired_actors": len(self._retired_actors),
        }
        with open(event_log_path, "a") as f:
            f.write(json.dumps(record, sort_keys=True) + "\n")

    async def start(self) -> None:
        if self.resource_manager is not None:
            self.resource_manager.register(self)
        try:
            await self._scale_up_to(self._allocate_actor_count(self.min_actors))
            self._record_event("scale", pending_tasks=self.min_actors)
        except Exception:
            if self.resource_manager is not None:
                self.resource_manager.unregister(self)
            raise

    async def get_actor_handles(self, pending_tasks: int = 1) -> list[UDFActorHandle]:
        desired = self._allocate_actor_count(self._desired_actor_count(pending_tasks))
        await self._scale_up_to(desired)
        self._retire_down_to(desired)
        self._record_event("scale", pending_tasks=pending_tasks)
        return list(self._active_actors)

    async def get_leased_actor_handles(self, pending_tasks: int = 1) -> list[UDFActorHandle]:
        actor_handles = await self.get_actor_handles(pending_tasks)
        self.lease_actor_handles()
        return actor_handles

    def cleanup_retired_actors(self) -> None:
        if self._actor_handle_leases > 0:
            return
        retired_actors = self._retired_actors
        self._retired_actors = []
        for actor in retired_actors:
            actor.teardown()
        if retired_actors:
            self._record_event("cleanup_retired")

    def lease_actor_handles(self) -> None:
        self._actor_handle_leases += 1

    def release_actor_handles(self) -> None:
        if self._actor_handle_leases == 0:
            return
        self._actor_handle_leases -= 1
        if self._actor_handle_leases == 0:
            self.cleanup_retired_actors()

    def teardown(self) -> None:
        if len(self._active_actors) > self.min_actors:
            self._retire_down_to(self.min_actors)
            self._record_event("scale", pending_tasks=0)
            self.cleanup_retired_actors()
        actors = [*self._active_actors, *self._retired_actors]
        self._active_actors = []
        self._retired_actors = []
        for actor in actors:
            actor.teardown()
        if actors:
            self._record_event("teardown")
        if self.resource_manager is not None:
            self.resource_manager.unregister(self)

    def _desired_actor_count(self, pending_tasks: int) -> int:
        return min(self.max_actors, max(self.min_actors, pending_tasks))

    def _allocate_actor_count(self, desired: int) -> int:
        if self.resource_manager is None:
            return desired
        return self.resource_manager.allocate_actor_count(self, desired)

    async def _scale_up_to(self, desired: int) -> None:
        if desired <= len(self._active_actors):
            return
        num_reusable_actors = min(desired - len(self._active_actors), len(self._retired_actors))
        if num_reusable_actors:
            self._active_actors.extend(self._retired_actors[:num_reusable_actors])
            self._retired_actors = self._retired_actors[num_reusable_actors:]
        if desired <= len(self._active_actors):
            return
        start_rank = self._next_actor_rank
        end_rank = self._next_actor_rank + desired - len(self._active_actors)
        num_pending_starts = end_rank - start_rank
        try:
            new_actors = await asyncio.gather(*(self._start_actor(rank) for rank in range(start_rank, end_rank)))
        finally:
            self._pending_actor_starts = max(0, self._pending_actor_starts - num_pending_starts)
        self._next_actor_rank += len(new_actors)
        self._active_actors.extend(new_actors)

    def _retire_down_to(self, desired: int) -> None:
        if desired >= len(self._active_actors):
            return
        self._retired_actors.extend(self._active_actors[desired:])
        self._active_actors = self._active_actors[:desired]

    def _release_one_above_min_for_resource_reclaim(self) -> None:
        if len(self._active_actors) <= self.min_actors:
            return
        self._retire_down_to(len(self._active_actors) - 1)
        self._record_event("resource_reclaim")
        self.cleanup_retired_actors()

    async def _start_actor(self, rank: int) -> UDFActorHandle:
        actor = UDFActor.options(  # type: ignore
            name=None if self.actor_name is None else f"{self.actor_name}:{str(uuid.uuid4())[:8]}-{rank}",
            **self.udf_options,
        ).remote(self.expr_projection)
        ready_future = asyncio.wrap_future(actor.__ray_ready__.remote().future())
        await asyncio.wait_for(ready_future, timeout=self.timeout)
        return UDFActorHandle(actor)


async def start_udf_actor_pool(
    projection: PyExpr,
    min_actors: int,
    max_actors: int,
    num_gpus_per_actor: float,
    num_cpus_per_actor: float,
    memory_per_actor: float,
    ray_options: dict[str, Any] | None,
    timeout: int,
    actor_name: str | None = None,
) -> UDFActorPool:
    expr_projection = ExpressionsProjection([Expression._from_pyexpr(projection)])

    # If resources are already in ray_options, we should prioritize them and avoid passing them twice
    # to avoid "keyword argument repeated" errors.
    ray_options = (ray_options or {}).copy()

    udf_options = validate_and_normalize_ray_options(
        {
            "scheduling_strategy": "SPREAD",
            "num_gpus": num_gpus_per_actor,
            "num_cpus": num_cpus_per_actor,
            "memory": memory_per_actor,
            **ray_options,
        }
    )

    # Read from udf_options (not the params) because ray_options can override num_cpus / num_gpus via the spread above.
    cpus_per_actor = udf_options.get("num_cpus", 1.0)
    gpus_per_actor = udf_options.get("num_gpus", 0.0)
    alive_nodes = [n for n in ray.nodes() if n["Alive"]]
    can_schedule = any(
        n["Resources"].get("CPU", 0) >= cpus_per_actor and n["Resources"].get("GPU", 0) >= gpus_per_actor
        for n in alive_nodes
    )
    if not can_schedule:
        raise RuntimeError(
            f"Actor UDF requires {cpus_per_actor} CPUs and {gpus_per_actor} GPUs per actor, "
            f"but no single node can satisfy this. "
            f"No single actor can be scheduled."
        )

    cluster_resources = ray.cluster_resources()
    cluster_cpus = cluster_resources.get("CPU", 0)
    cluster_gpus = cluster_resources.get("GPU", 0)
    if cpus_per_actor > 0 or gpus_per_actor > 0:
        limits = []
        if cpus_per_actor > 0:
            limits.append(cluster_cpus / cpus_per_actor)
        if gpus_per_actor > 0:
            limits.append(cluster_gpus / gpus_per_actor)
        max_schedulable = int(min(limits))
        if min_actors > max_schedulable:
            raise RuntimeError(
                f"Actor UDF requires at least {min_actors} actors but only {max_schedulable} actors can be scheduled "
                f"({cluster_cpus:g} CPUs, {cluster_gpus:g} GPUs available)."
            )
        if max_actors > max_schedulable:
            logger.warning(
                "with_concurrency(%d) requested but only %d actors can be scheduled "
                "(%g CPUs, %g GPUs available). The actor pool will use current capacity "
                "and can grow when cluster resources increase.",
                max_actors,
                max_schedulable,
                cluster_cpus,
                cluster_gpus,
            )

    pool = UDFActorPool(
        expr_projection=expr_projection,
        min_actors=min_actors,
        max_actors=max_actors,
        udf_options=udf_options,
        timeout=timeout,
        actor_name=actor_name,
        cpus_per_actor=cpus_per_actor,
        gpus_per_actor=gpus_per_actor,
        resource_manager=_get_global_udf_actor_pool_resource_manager(
            cluster_cpus,
            cluster_gpus,
            available_resources_getter=ray.available_resources,
            cluster_resources_getter=ray.cluster_resources,
        ),
    )
    await pool.start()
    return pool


async def start_udf_actors(
    projection: PyExpr,
    min_actors: int,
    max_actors: int,
    num_gpus_per_actor: float,
    num_cpus_per_actor: float,
    memory_per_actor: float,
    ray_options: dict[str, Any] | None,
    timeout: int,
    actor_name: str | None = None,
) -> list[UDFActorHandle]:
    pool = await start_udf_actor_pool(
        projection,
        min_actors,
        max_actors,
        num_gpus_per_actor,
        num_cpus_per_actor,
        memory_per_actor,
        ray_options,
        timeout,
        actor_name,
    )
    return await pool.get_actor_handles(max_actors)
