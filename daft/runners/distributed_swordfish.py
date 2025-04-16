import logging
from typing import AsyncGenerator, Dict, Iterator, List, Optional, Tuple

from daft.daft import (
    DistributedPhysicalPlanner,
    LocalPhysicalPlan,
    NativeExecutor,
    PyDaftExecutionConfig,
)
from daft.recordbatch.micropartition import MicroPartition

try:
    import ray
    import ray.util.scheduling_strategies
except ImportError:
    raise

logger = logging.getLogger(__name__)


@ray.remote(max_restarts=-1, max_task_retries=-1)
class SwordfishActor:
    def __init__(self):
        self.native_executor = NativeExecutor()
        self.actor_handle = ray.get_runtime_context().current_actor

    # Run a plan on swordfish and yield partitions
    async def _run_plan(
        self,
        local_physical_plan: LocalPhysicalPlan,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: Optional[int] = None,
    ) -> AsyncGenerator[MicroPartition, None]:
        async for partition in self.native_executor.run_distributed(
            local_physical_plan, daft_execution_config, results_buffer_size
        ):
            if partition is None:
                break
            yield MicroPartition._from_pymicropartition(partition)

    # Run a plan on swordfish and collect partitions
    async def run_plan_and_collect(
        self,
        local_physical_plan: LocalPhysicalPlan,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: Optional[int] = None,
    ) -> List[ray.ObjectRef]:
        # We use the actor handle to run the plan so that the outputs are yielded into the object store
        result_gen = self.actor_handle._run_plan.remote(local_physical_plan, daft_execution_config, results_buffer_size)

        # Collect the partitions into a list and return them, once the this is finished then the plan
        # is considered complete.
        res = []
        async for partition in result_gen:
            res.append(partition)
        return res

    async def run_plan_into_shuffle_cache(
        self,
        local_physical_plan: LocalPhysicalPlan,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: Optional[int] = None,
    ) -> None:
        pass


class ActorManager:
    ACTOR_MAX_TASKS = 4  # TODO: Make this configurable

    def __init__(self):
        self.actors = []
        self.active_tasks_by_actor: Dict[SwordfishActor, int] = {}
        self.task_to_actor: Dict[ray.ObjectRef, SwordfishActor] = {}
        self._initialize_actors()

    def _initialize_actors(self) -> None:
        for node in ray.nodes():
            if "Resources" in node and "CPU" in node["Resources"] and node["Resources"]["CPU"] > 0:
                actor = SwordfishActor.options(  # type: ignore
                    num_cpus=node["Resources"]["CPU"],
                    scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                        node_id=node["NodeID"], soft=True
                    ),
                ).remote()
                self.actors.append(actor)
                self.active_tasks_by_actor[actor] = 0

    def get_available_actors(self) -> List[SwordfishActor]:
        return [actor for actor in self.actors if self.active_tasks_by_actor[actor] < self.ACTOR_MAX_TASKS]

    def increment_actor_tasks(self, actor: SwordfishActor) -> None:
        self.active_tasks_by_actor[actor] += 1

    def decrement_actor_tasks(self, actor: SwordfishActor) -> None:
        self.active_tasks_by_actor[actor] -= 1

    def can_submit_task(self, actor: SwordfishActor) -> bool:
        return self.active_tasks_by_actor[actor] < self.ACTOR_MAX_TASKS

    def submit_task(
        self,
        task: LocalPhysicalPlan,
        actor: SwordfishActor,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: Optional[int] = None,
    ) -> ray.ObjectRef:
        task_ref = actor.run_plan_and_collect.remote(  # type: ignore
            task, daft_execution_config, results_buffer_size
        )
        self.increment_actor_tasks(actor)
        self.task_to_actor[task_ref] = actor
        return task_ref

    def complete_task(self, task_ref: ray.ObjectRef) -> None:
        """Mark a task as completed and clean up actor tracking."""
        if task_ref in self.task_to_actor:
            actor = self.task_to_actor[task_ref]
            self.decrement_actor_tasks(actor)
            del self.task_to_actor[task_ref]


class TaskDispatcher:
    def __init__(self, actor_manager: ActorManager):
        self.actor_manager = actor_manager
        self.pending_tasks: List[Tuple[ray.ObjectRef, int]] = []
        self.completed_results: Dict[int, List[ray.ObjectRef]] = {}
        self.next_submission_order = 0
        self.current_submission_order = 0

    def dispatch_tasks(
        self,
        planner: DistributedPhysicalPlanner,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: Optional[int] = None,
    ) -> DistributedPhysicalPlanner:
        available_actors = self.actor_manager.get_available_actors()
        for actor in available_actors:
            while self.actor_manager.can_submit_task(actor) and planner.has_remaining_plans():
                plan = planner.next_plan()
                assert plan is not None
                task_ref = self.actor_manager.submit_task(plan, actor, daft_execution_config, results_buffer_size)
                self.pending_tasks.append((task_ref, self.current_submission_order))
                self.current_submission_order += 1

        return planner

    def process_completed_tasks(self) -> List[Tuple[int, List[ray.ObjectRef]]]:
        if not self.pending_tasks:
            return []

        ready_task_refs, remaining_task_refs = ray.wait(
            [task_ref for task_ref, _ in self.pending_tasks],
            timeout=1.0,
        )

        # Find task orders before removing from pending tasks
        task_orders = {task_ref: order for task_ref, order in self.pending_tasks if task_ref in ready_task_refs}

        # Update pending tasks
        self.pending_tasks = [
            (task_ref, order) for task_ref, order in self.pending_tasks if task_ref in remaining_task_refs
        ]

        completed = []
        for task_ref in ready_task_refs:
            # Get order from the saved mapping
            submission_order = task_orders[task_ref]

            # Update actor task count
            self.actor_manager.complete_task(task_ref)

            # Get results
            results = ray.get(task_ref)
            completed.append((submission_order, results))

        return completed

    def has_pending_work(self) -> bool:
        return bool(self.pending_tasks or self.completed_results)


def run_distributed_swordfish(
    planner: DistributedPhysicalPlanner,
    daft_execution_config: PyDaftExecutionConfig,
    results_buffer_size: Optional[int] = None,
) -> Iterator[MicroPartition]:
    """Executes distributed physical plans using Ray actors."""
    actor_manager = ActorManager()
    dispatcher = TaskDispatcher(actor_manager)

    # Submit initial tasks
    planner = dispatcher.dispatch_tasks(planner, daft_execution_config, results_buffer_size)

    # Process results and submit new tasks
    while dispatcher.has_pending_work() or planner.has_remaining_plans():
        # Process completed tasks
        completed_tasks = dispatcher.process_completed_tasks()
        for order, results in completed_tasks:
            dispatcher.completed_results[order] = results

        # Submit new tasks if any actors have become available
        if planner.has_remaining_plans() and completed_tasks:
            # Only try to submit new tasks if some tasks completed, freeing up actors
            planner = dispatcher.dispatch_tasks(planner, daft_execution_config, results_buffer_size)

        # Yield results in order
        while dispatcher.next_submission_order in dispatcher.completed_results:
            results = dispatcher.completed_results.pop(dispatcher.next_submission_order)
            yield from results
            dispatcher.next_submission_order += 1
