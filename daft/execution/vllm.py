from __future__ import annotations

import asyncio
import threading
from abc import ABC, abstractmethod
from collections import deque
from typing import Any

from daft.recordbatch import RecordBatch


class VLLMExecutor(ABC):
    @abstractmethod
    def submit(self, prefix: str, prompts: list[str], rows: RecordBatch) -> None:
        pass

    @abstractmethod
    def poll(self) -> tuple[list[str], RecordBatch] | None:
        pass

    @abstractmethod
    def finished_submitting(self) -> None:
        pass

    @abstractmethod
    def all_tasks_finished(self) -> bool:
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
        return self._finished_submitting


class LocalVLLMExecutor(VLLMExecutor):
    def __init__(self, model: str, engine_args: dict[str, Any], generate_args: dict[str, Any]):
        import time

        from vllm import AsyncEngineArgs, AsyncLLMEngine, SamplingParams

        args = AsyncEngineArgs(model=model, **engine_args)

        start_time = time.perf_counter()
        self.llm = AsyncLLMEngine.from_engine_args(args)
        end_time = time.perf_counter()
        print(f"LLM initialized in {end_time - start_time:.2f} seconds.")

        self.sampling_params = generate_args.pop("sampling_params", SamplingParams())
        self.generate_args = generate_args

        self.counter = 0
        self.counter_lock = threading.Lock()

        self.running_task_count = 0
        self.task_count_lock = threading.Lock()

        self.completed_tasks: deque[tuple[str, RecordBatch]] = deque()

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

    def submit(self, _prefix: str, prompts: list[str], rows: RecordBatch) -> None:
        """Submit a batch of prompts and rows to the executor, returning once all tasks are started."""
        assert len(prompts) == len(rows), "Number of prompts and rows must match"

        with self.task_count_lock:
            self.running_task_count += len(prompts)

        for i in range(len(prompts)):
            prompt = prompts[i]
            row = rows.slice(i, i + 1)
            asyncio.run_coroutine_threadsafe(self._generate(prompt, row), self.loop)

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
            return self._finished_submitting and self.running_task_count == 0


class DistributedVLLMExecutor(VLLMExecutor):
    def __init__(self, llm_actors: LLMActors):
        import ray
        from ray._private.state import actors

        self.router_actor = llm_actors.router_actor
        self.router_actor.report_start.remote()

        self.llm_actors = llm_actors.llm_actors

        current_node_id = ray.get_runtime_context().get_node_id()

        self.local_llm_actors = []
        for llm_actor in self.llm_actors:
            actor_id = llm_actor._actor_id.hex()
            actor_state = actors(actor_id)
            if actor_state["Address"]["NodeID"] == current_node_id:
                self.local_llm_actors.append(llm_actor)

    def submit(self, prefix: str, prompts: list[str], rows: RecordBatch) -> None:
        import ray

        route_to = ray.get(self.router_actor.route.remote(prefix, len(prompts)))
        self.local_llm_actors[route_to].submit.remote(prefix, prompts, rows)

    def poll(self) -> tuple[list[str], RecordBatch] | None:
        import ray

        all_outputs = []
        all_rows = []

        for llm_actor in self.local_llm_actors:
            result = ray.get(llm_actor.poll.remote())
            if result is not None:
                outputs, rows = result
                all_outputs.extend(outputs)
                all_rows.append(rows)

        if len(all_outputs) == 0:
            return None
        else:
            return all_outputs, RecordBatch.concat(all_rows)

    def finished_submitting(self) -> None:
        self.router_actor.report_completion.remote()
        for llm_actor in self.local_llm_actors:
            llm_actor.finished_submitting.remote()

    def all_tasks_finished(self) -> bool:
        import ray

        return ray.get(self.router_actor.all_actors_finished.remote()) and all(
            ray.get(llm_actor.all_tasks_finished.remote()) for llm_actor in self.local_llm_actors
        )


class PrefixRouter:
    def __init__(self, num_llm_actors: int, load_balance_threshold: int, max_recent_prefixes: int = 8):
        self.loads = [0] * num_llm_actors
        self.recent_prefixes: list[list[str]] = [[] for _ in range(num_llm_actors)]
        self.load_balance_threshold = load_balance_threshold
        self.max_recent_prefixes = max_recent_prefixes
        self.unfinished_actors = 0

    def route(self, prefix: str, batch_size: int) -> int:
        min_load = min(self.loads)

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

    def all_actors_finished(self) -> bool:
        return self.unfinished_actors == 0


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

        LocalVLLMExecutorActor = ray.remote(num_gpus=gpus_per_actor)(LocalVLLMExecutor)
        PrefixRouterActor = ray.remote(PrefixRouter)

        self.llm_actors = [LocalVLLMExecutorActor.remote(model, engine_args, generate_args) for _ in range(concurrency)]
        self.router_actor = PrefixRouterActor.remote(len(self.llm_actors), load_balance_threshold)
