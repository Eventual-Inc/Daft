from __future__ import annotations

import asyncio
import threading
from collections import deque
from typing import Any

from daft.recordbatch import RecordBatch


class VLLMExecutor:
    def __init__(self, model: str, engine_args: dict[str, Any], generate_args: dict[str, Any]):
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

        # Create a dedicated event loop in a separate thread
        self.loop_ready = threading.Event()
        self.loop_thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self.loop_thread.start()
        self.loop_ready.wait()

        self._shutdown = False
        self._finished_submitting = False

    def _run_event_loop(self) -> None:
        """Run the event loop in a separate thread."""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop_ready.set()  # Signal that the loop is ready
        self.loop.run_forever()

    def __del__(self) -> None:
        if self._shutdown:
            return

        self._shutdown = True

        if self.loop is not None and self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)

        if self.loop_thread.is_alive():
            self.loop_thread.join(timeout=5.0)

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

    def submit(self, prompts: list[str], rows: RecordBatch) -> None:
        """Submit a batch of prompts and rows to the executor, returning once all tasks are started."""
        if self._shutdown:
            raise RuntimeError("Cannot submit tasks to a shutdown executor")

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
