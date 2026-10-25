from __future__ import annotations

import asyncio
import sys
import types

import pytest

import daft
from daft.execution.vllm import LocalVLLMExecutor
from daft.functions import prompt
from daft.recordbatch import RecordBatch
from tests.conftest import get_tests_daft_runner_name


class FakeAsyncLLMEngine:
    """Stands in for `vllm.AsyncLLMEngine`: echoes prompts in upper case, fails prompts containing "fail" and never finishes prompts containing "hang"."""

    def __init__(self):
        # Keep hung requests reachable, as a real engine does; otherwise GC can collect the pending task.
        self.hung_requests = []

    @classmethod
    def from_engine_args(cls, args):
        return cls()

    async def generate(self, prompt, sampling_params, request_id, **kwargs):
        if "fail" in prompt:
            raise RuntimeError(f"engine rejected request {request_id}")
        if "hang" in prompt:
            hung = asyncio.Event()
            self.hung_requests.append(hung)
            await hung.wait()
        yield types.SimpleNamespace(outputs=[types.SimpleNamespace(text=prompt.upper())])

    async def do_log_stats(self):
        pass


@pytest.fixture
def fake_vllm(monkeypatch):
    module = types.ModuleType("vllm")
    module.AsyncEngineArgs = lambda model, **kwargs: types.SimpleNamespace(model=model, **kwargs)
    module.AsyncLLMEngine = FakeAsyncLLMEngine
    module.SamplingParams = lambda **kwargs: types.SimpleNamespace(**kwargs)
    monkeypatch.setitem(sys.modules, "vllm", module)
    return module


@pytest.fixture
def executor(fake_vllm):
    executor = LocalVLLMExecutor("fake-model", {}, {})
    yield executor
    executor.loop.call_soon_threadsafe(executor.loop.stop)


def run_on_executor_loop(executor, coro):
    return asyncio.run_coroutine_threadsafe(coro, executor.loop).result(timeout=10)


def generate_tasks():
    return [
        t for t in asyncio.all_tasks() if getattr(t.get_coro(), "__qualname__", None) == "LocalVLLMExecutor._generate"
    ]


async def wait_for_generate_tasks():
    """Wait for every `_generate` task scheduled on this loop so far."""
    tasks = generate_tasks()
    if tasks:
        await asyncio.wait(tasks)


def test_local_executor_schedules_periodic_log(executor):
    executor.submit(None, ["a"], RecordBatch.from_pydict({"p": ["a"]}))

    async def scheduled_coroutines():
        return {getattr(t.get_coro(), "__qualname__", None) for t in asyncio.all_tasks()}

    assert "LocalVLLMExecutor._periodic_log" in run_on_executor_loop(executor, scheduled_coroutines())


def test_local_executor_raises_failed_generate(executor):
    prompts = ["a", "b fail", "c"]
    executor.submit(None, prompts, RecordBatch.from_pydict({"p": prompts}))
    executor.finished_submitting()
    run_on_executor_loop(executor, wait_for_generate_tasks())

    with pytest.raises(RuntimeError, match="engine rejected request"):
        executor.poll()
    with pytest.raises(RuntimeError, match="engine rejected request"):
        executor.all_tasks_finished()
    assert executor.running_task_count == 0


def test_local_executor_raises_cancelled_generate(executor):
    prompts = ["a", "b hang"]
    executor.submit(None, prompts, RecordBatch.from_pydict({"p": prompts}))
    executor.finished_submitting()

    async def cancel_pending_requests():
        tasks = generate_tasks()
        for task in tasks:
            task.cancel()
        await asyncio.wait(tasks)

    run_on_executor_loop(executor, cancel_pending_requests())

    with pytest.raises(asyncio.CancelledError):
        executor.poll()


def test_local_executor_raises_failed_generate_from_submit_async(executor):
    # submit_async is the path Ray actors use (LLMActors / RemoteVLLMExecutor).
    prompts = ["a", "b fail", "c"]

    async def submit_and_wait():
        await executor.submit_async(prompts, RecordBatch.from_pydict({"p": prompts}))
        await wait_for_generate_tasks()

    run_on_executor_loop(executor, submit_and_wait())
    executor.finished_submitting()

    with pytest.raises(RuntimeError, match="engine rejected request"):
        executor.all_tasks_finished()


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="The fake vllm module only exists in this process")
def test_prompt_vllm_returns_outputs(fake_vllm):
    df = daft.from_pydict({"p": ["a", "b", "c"]})
    df = df.with_column("r", prompt(df["p"], provider="vllm-prefix-caching", model="fake-model"))

    result = df.to_pydict()
    assert dict(zip(result["p"], result["r"])) == {"a": "A", "b": "B", "c": "C"}


@pytest.mark.timeout(30)
@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="The fake vllm module only exists in this process")
def test_prompt_vllm_fails_query_when_a_request_fails(fake_vllm):
    df = daft.from_pydict({"p": ["a", "b fail", "c"]})
    df = df.with_column("r", prompt(df["p"], provider="vllm-prefix-caching", model="fake-model"))

    with pytest.raises(RuntimeError, match="engine rejected request"):
        df.collect()
