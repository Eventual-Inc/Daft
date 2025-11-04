from __future__ import annotations

import asyncio
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ray import ObjectRef

from daft.recordbatch import RecordBatch


class VLLMExecutor(ABC):
    @abstractmethod
    def submit(self, prefix: str, prompts: list[str], rows: RecordBatch) -> None:
        """Submit a batch of prompts and their corresponding rows to the executor, returning once all tasks are started."""
        pass

    @abstractmethod
    def poll(self) -> tuple[list[str], RecordBatch] | None:
        """Poll the executor for completed tasks. Returns a tuple of completed outputs and their corresponding rows, or None if no tasks are completed."""
        pass

    @abstractmethod
    def finished_submitting(self) -> None:
        """Call this when all tasks have been submitted."""
        pass

    @abstractmethod
    def all_tasks_finished(self) -> bool:
        """Check if all tasks have been completed and all results have been polled."""
        pass


class DummyVLLMExecutor(VLLMExecutor):
    def __init__(self, _model: str, _engine_args: dict[str, Any], _generate_args: dict[str, Any]):
        self.prompts: list[str] = []
        self.rows: list[RecordBatch] = []
        self._finished_submitting = False

    def submit(self, prefix: str, prompts: list[str], rows: RecordBatch) -> None:
        self.prompts.extend(prompts)
        self.rows.append(rows)

    def poll(self) -> tuple[list[str], RecordBatch] | None:
        if len(self.prompts) == 0:
            return None
        else:
            results = (self.prompts, RecordBatch.concat(self.rows))
            self.prompts = []
            self.rows = []
            return results

    def finished_submitting(self) -> None:
        self._finished_submitting = True

    def all_tasks_finished(self) -> bool:
        return self._finished_submitting and len(self.prompts) == 0


class BlockingVLLMExecutor(VLLMExecutor):
    def __init__(self, model: str, engine_args: dict[str, Any], generate_args: dict[str, Any]):
        from vllm import LLM, SamplingParams

        self.llm = LLM(model=model, **engine_args)

        self.sampling_params = generate_args.pop("sampling_params", SamplingParams())
        self.generate_args = generate_args

        self._finished_submitting = False

        self.completed_tasks: list[tuple[list[str], RecordBatch]] = []

    def submit(self, prefix: str, prompts: list[str], rows: RecordBatch) -> None:
        import threading

        results_container = {}

        def run_generate() -> None:
            results_container["results"] = self.llm.generate(prompts, self.sampling_params, **self.generate_args)

        generate_thread = threading.Thread(target=run_generate)
        generate_thread.start()
        generate_thread.join()
        results = results_container["results"]
        outputs = [r.outputs[0].text for r in results]

        self.completed_tasks.append((outputs, rows))

    def poll(self) -> tuple[list[str], RecordBatch] | None:
        if len(self.completed_tasks) == 0:
            return None

        completed_outputs = [output for outputs in (t[0] for t in self.completed_tasks) for output in outputs]
        completed_rows = [t[1] for t in self.completed_tasks]
        completed_rows_batch = RecordBatch.concat(completed_rows)

        self.completed_tasks = []
        return completed_outputs, completed_rows_batch

    def finished_submitting(self) -> None:
        self._finished_submitting = True

    def all_tasks_finished(self) -> bool:
        return self._finished_submitting and len(self.completed_tasks) == 0


class LocalVLLMExecutor(VLLMExecutor):
    def __init__(
        self, model: str, engine_args: dict[str, Any], generate_args: dict[str, Any], use_threading: bool = True
    ):
        from vllm import AsyncEngineArgs, AsyncLLMEngine, SamplingParams

        args = AsyncEngineArgs(model=model, **engine_args)

        self.llm = AsyncLLMEngine.from_engine_args(args)

        self.sampling_params = generate_args.pop("sampling_params", SamplingParams())
        self.generate_args = generate_args

        self.counter = 0
        self.counter_lock = threading.Lock()

        self.running_task_count = 0
        self.task_count_lock = threading.Lock()

        self.completed_tasks: deque[tuple[str, RecordBatch]] = deque()

        self.last_log_time = time.perf_counter()
        self.last_log_lock = threading.Lock()

        self.use_threading = use_threading
        if self.use_threading:
            # Create a dedicated event loop in a separate thread
            self.loop_ready = threading.Event()
            self.loop_thread = threading.Thread(target=self._run_event_loop, daemon=True)
            self.loop_thread.start()
            self.loop_ready.wait()

        self._finished_submitting = False

    def _run_event_loop(self) -> None:
        """Run the event loop in a separate thread."""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop_ready.set()  # Signal that the loop is ready
        self.loop.run_forever()
        self.loop.create_task(self._periodic_log())

    async def _periodic_log(self) -> None:
        while True:
            await asyncio.sleep(5)
            await self.llm.do_log_stats()

    async def _generate(self, prompt: str, row: RecordBatch) -> None:
        with self.counter_lock:
            id = self.counter
            self.counter += 1

        final_output = None
        async for output in self.llm.generate(prompt, self.sampling_params, str(id), **self.generate_args):
            final_output = output

        output_text: str = final_output.outputs[0].text  # type: ignore
        self.completed_tasks.append((output_text, row))

        with self.task_count_lock:
            self.running_task_count -= 1

        # Check if we need to log (in case periodic log isn't running in distributed mode)
        current_time = time.perf_counter()
        with self.last_log_lock:
            time_since_last_log = current_time - self.last_log_time
            if time_since_last_log >= 5.0:
                await self.llm.do_log_stats()
                self.last_log_time = time.perf_counter()

    def submit(self, _prefix: str | None, prompts: list[str], rows: RecordBatch) -> None:
        """Submit a batch of prompts and rows to the executor, returning once all tasks are started."""
        assert len(prompts) == len(rows), "Number of prompts and rows must match"

        if not self.use_threading:
            raise ValueError("Synchronous mode not supported when use_threading is False")

        with self.task_count_lock:
            self.running_task_count += len(prompts)

        for i in range(len(prompts)):
            prompt = prompts[i]
            row = rows.slice(i, i + 1)
            asyncio.run_coroutine_threadsafe(self._generate(prompt, row), self.loop)

    async def submit_async(self, prompts: list[str], rows: RecordBatch) -> None:
        assert len(prompts) == len(rows), "Number of prompts and rows must match"

        with self.task_count_lock:
            self.running_task_count += len(prompts)

        for i in range(len(prompts)):
            prompt = prompts[i]
            row = rows.slice(i, i + 1)
            asyncio.create_task(self._generate(prompt, row))

    def poll(self) -> tuple[list[str], RecordBatch] | None:
        """Poll the executor for completed tasks."""
        # Drain the queues (no lock needed - queues are thread-safe)
        completed_outputs = []
        completed_rows = []

        while True:
            try:
                (output, row) = self.completed_tasks.popleft()
                completed_outputs.append(output)
                completed_rows.append(row)
            except IndexError:
                break

        if not completed_outputs:
            return None

        completed_rows_batch = RecordBatch.concat(completed_rows)
        return completed_outputs, completed_rows_batch

    def finished_submitting(self) -> None:
        self._finished_submitting = True

    def all_tasks_finished(self) -> bool:
        with self.task_count_lock:
            return self._finished_submitting and self.running_task_count == 0 and len(self.completed_tasks) == 0


class RemoteVLLMExecutor(VLLMExecutor):
    def __init__(self, llm_actors: LLMActors):
        self.router_actor = llm_actors.router_actor
        self.router_actor.report_start.remote()

        self.llm_actors = llm_actors.llm_actors

    def submit(self, prefix: str | None, prompts: list[str], rows: RecordBatch) -> None:
        import ray

        route_to = ray.get(self.router_actor.route.remote(prefix, len(prompts)))
        ray.get(self.llm_actors[route_to].submit_async.remote(prompts, rows))

    def poll(self) -> tuple[list[str], RecordBatch] | None:
        import ray

        next_llm_to_poll = ray.get(self.router_actor.get_next_llm_to_poll.remote())
        if next_llm_to_poll is not None:
            return ray.get(self.llm_actors[next_llm_to_poll].poll.remote())
        else:
            return None

    def finished_submitting(self) -> None:
        self.router_actor.report_completion.remote()

    def all_tasks_finished(self) -> bool:
        import ray

        return ray.get(self.router_actor.all_tasks_finished.remote())


class PrefixRouter:
    def __init__(
        self, llm_actors: list[ObjectRef[LocalVLLMExecutor]], load_balance_threshold: int, max_recent_prefixes: int = 8
    ):
        self.llm_actors = llm_actors
        self.loads = [0] * len(self.llm_actors)
        self.recent_prefixes: list[list[str | None]] = [[] for _ in range(len(self.llm_actors))]
        self.load_balance_threshold = load_balance_threshold
        self.max_recent_prefixes = max_recent_prefixes
        self.unfinished_actors = 0
        self.next_llm_to_poll = list(range(len(self.llm_actors)))

    def route(self, prefix: str | None, batch_size: int) -> int:
        min_load = min(self.loads)

        if prefix is None:
            best_actor = self.loads.index(min_load)
        else:
            best_actor = None
            best_actor_recency = None
            for i in range(len(self.loads)):
                if self.loads[i] < min_load + self.load_balance_threshold:
                    try:
                        recency = self.recent_prefixes[i].index(prefix)
                    except ValueError:
                        recency = None

                    if best_actor_recency is None:
                        best_actor = i
                        best_actor_recency = recency
                    elif recency is not None and recency < best_actor_recency:
                        best_actor = i
                        best_actor_recency = recency

        assert best_actor is not None, "No actor found for prefix"

        self.recent_prefixes[best_actor].insert(0, prefix)
        if len(self.recent_prefixes[best_actor]) > self.max_recent_prefixes:
            self.recent_prefixes[best_actor].pop()

        self.loads[best_actor] += batch_size

        return best_actor

    def report_start(self) -> None:
        self.unfinished_actors += 1

    def report_completion(self) -> None:
        self.unfinished_actors -= 1

        if self.unfinished_actors == 0:
            for actor in self.llm_actors:
                actor.finished_submitting.remote()

    def get_next_llm_to_poll(self) -> int | None:
        import ray

        while len(self.next_llm_to_poll) > 0:
            next_llm = self.next_llm_to_poll.pop(0)

            if not ray.get(self.llm_actors[next_llm].all_tasks_finished.remote()):
                self.next_llm_to_poll.append(next_llm)
                return next_llm

        return None

    def all_tasks_finished(self) -> bool:
        return len(self.next_llm_to_poll) == 0


class LLMActors:
    def __init__(
        self,
        model: str,
        engine_args: dict[str, Any],
        generate_args: dict[str, Any],
        gpus_per_actor: int,
        concurrency: int,
        load_balance_threshold: int,
    ):
        import ray

        LocalVLLMExecutorActor = ray.remote(num_gpus=gpus_per_actor, max_restarts=4)(LocalVLLMExecutor)
        PrefixRouterActor = ray.remote(PrefixRouter)

        self.llm_actors = [LocalVLLMExecutorActor.remote(model, engine_args, generate_args) for _ in range(concurrency)]
        self.router_actor = PrefixRouterActor.remote(self.llm_actors, load_balance_threshold)
