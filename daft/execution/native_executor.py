from __future__ import annotations

import secrets
from typing import TYPE_CHECKING

from daft.daft import (
    FlightPartitions,
    Input,
    LocalPhysicalPlan,
    PyDaftExecutionConfig,
    PyExecutionStats,
    PyMicroPartition,
)
from daft.daft import (
    NativeExecutor as _NativeExecutor,
)
from daft.dataframe.display import MermaidOptions
from daft.event_loop import get_or_init_event_loop
from daft.recordbatch import MicroPartition
from daft.runners.partitioning import LocalMaterializedResult

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator, Hashable, Iterator, Mapping, Sequence

    from typing_extensions import Self

    from daft.context import DaftContext
    from daft.execution.context_protocol import (
        BatchLease,
        ContextBatch,
        ContextProtocol,
        ContextProtocolConfig,
        RowTransformRequest,
        SourceSnapshot,
    )
    from daft.logical.builder import LogicalPlanBuilder


async def _next_context_result(result_handle: object) -> object:
    return await result_handle.__anext__()


async def _finish_context_result(result_handle: object) -> PyExecutionStats:
    return await result_handle.try_finish()


async def _submit_context_round(
    executor: object,
    local_physical_plan: LocalPhysicalPlan,
    ctx: DaftContext,
    input_id: int,
    inputs: Mapping[int, Input | list[PyMicroPartition]],
    context: Mapping[str, str],
    maintain_order: bool,
) -> object:
    return await executor.run_retained(
        local_physical_plan,
        ctx._ctx,
        input_id,
        dict(inputs),
        dict(context),
        maintain_order,
    )


async def _close_context_plan(executor: object, fingerprint: int) -> None:
    await executor.close_plan(fingerprint)


class NativeExecutor:
    def __init__(self) -> None:
        self._executor = _NativeExecutor(False, "")

    def run(
        self,
        local_physical_plan: LocalPhysicalPlan,
        inputs: Mapping[int, Input | list[PyMicroPartition]],
        ctx: DaftContext,
        context: dict[str, str] | None,
    ) -> Generator[LocalMaterializedResult, None, tuple[str, PyExecutionStats]]:
        stats: PyExecutionStats | None = None

        async def stream_results() -> AsyncGenerator[PyMicroPartition | FlightPartitions | None, None]:
            result_handle = await self._executor.run(
                local_physical_plan,
                ctx._ctx,
                0,
                dict(inputs),
                context,
                ctx.daft_execution_config.maintain_order,
            )
            nonlocal stats
            try:
                async for batch in result_handle:
                    yield batch
            finally:
                stats = await result_handle.try_finish()

        event_loop = get_or_init_event_loop()
        async_exec = stream_results()
        should_raise_errors_from_close = True
        try:
            while True:
                part = event_loop.run(async_exec.__anext__())
                if part is None:
                    break
                yield LocalMaterializedResult(MicroPartition._from_pymicropartition(part))
        except BaseException:
            # Preserve the original exception/GeneratorExit by not masking it with errors from async_exec.aclose().
            should_raise_errors_from_close = False
            raise
        finally:
            try:
                event_loop.run(async_exec.aclose())
            except Exception:
                if should_raise_errors_from_close:
                    raise

        assert stats is not None
        return stats.query_plan or "", stats

    def pretty_print(
        self,
        builder: LogicalPlanBuilder,
        daft_execution_config: PyDaftExecutionConfig,
        simple: bool = False,
        format: str = "ascii",
    ) -> str:
        """Pretty prints the current underlying logical plan."""
        if format == "ascii":
            return _NativeExecutor.repr_ascii(builder._builder, daft_execution_config, simple)
        elif format == "mermaid":
            return _NativeExecutor.repr_mermaid(builder._builder, daft_execution_config, MermaidOptions(simple))
        else:
            raise ValueError(f"Unknown format: {format}")


class NativeContextRound:
    """One submitted input round in an experimental native context session."""

    def __init__(
        self,
        session: NativeContextSession,
        input_id: int,
        result_handle: object,
    ) -> None:
        self._session = session
        self._input_id = input_id
        self._result_handle = result_handle
        self._finished = False
        self._saw_eof = False
        self._stats: PyExecutionStats | None = None

    @property
    def input_id(self) -> int:
        return self._input_id

    @property
    def stats(self) -> PyExecutionStats | None:
        return self._stats

    def __iter__(self) -> Iterator[LocalMaterializedResult]:
        return self

    def __next__(self) -> LocalMaterializedResult:
        if self._finished:
            raise StopIteration

        event_loop = get_or_init_event_loop()
        try:
            part = event_loop.run(_next_context_result(self._result_handle))
        except BaseException:
            self._finished = True
            self._session._round_failed(self)
            raise

        if part is None:
            self._saw_eof = True
            try:
                self._stats = event_loop.run(_finish_context_result(self._result_handle))
            except BaseException:
                self._finished = True
                self._session._round_failed(self)
                raise
            self._finished = True
            self._session._round_finished(self)
            raise StopIteration

        return LocalMaterializedResult(MicroPartition._from_pymicropartition(part))

    def close(self) -> None:
        """Stop this round.

        A round closed before end-of-stream cancels the whole session because
        the retained pipeline may still be processing that input.
        """
        if self._finished:
            return

        self._finished = True
        if not self._saw_eof:
            self._session._round_failed(self)


class NativePendingContextRound:
    """A context round with output credit reserved before plan submission."""

    def __init__(
        self,
        session: NativeContextSession,
        result_round: NativeContextRound,
        reservation: BatchLease[object],
        replace_seeds: Sequence[Hashable] | None,
        row_transform: RowTransformRequest | None,
    ) -> None:
        self._session = session
        self._result_round = result_round
        self._reservation = reservation
        self._replace_seeds = replace_seeds
        self._row_transform = row_transform
        self._collected = False

    def collect(self) -> ContextBatch:
        if self._collected:
            raise RuntimeError("Context round has already been collected")
        self._collected = True
        try:
            parts = [result.partition() for result in self._result_round]
            if not parts:
                self._session.cancel()
                raise RuntimeError("Context round produced no output partition")
            table = MicroPartition.concat(parts).to_arrow()
            stats = self._result_round.stats
            query_plan = "" if stats is None else (stats.query_plan or "")
            return self._session.protocol.accept(
                table,
                plan_fingerprint=self._session.fingerprint,
                input_id=self._result_round.input_id,
                query_plan=query_plan,
                replace_seeds=self._replace_seeds,
                row_transform=self._row_transform,
                reserved_lease=self._reservation,
            )
        except BaseException:
            self._reservation.release()
            raise

    def cancel(self) -> None:
        self._reservation.release()
        self._result_round.close()


class NativeContextSession:
    """Experimental retained native executor session.

    One local physical plan is translated once. Each call to ``run_round``
    supplies a new input mapping and receives a new input ID while reusing the
    same pipeline and class-UDF worker state.
    """

    def __init__(
        self,
        local_physical_plan: LocalPhysicalPlan,
        ctx: DaftContext,
        context: Mapping[str, str] | None = None,
        *,
        maintain_order: bool = True,
        protocol_config: ContextProtocolConfig | None = None,
    ) -> None:
        from daft.runners import get_or_infer_runner_type

        runner_type = get_or_infer_runner_type()
        if runner_type != "native":
            raise RuntimeError(
                f"NativeContextSession requires the native runner, but the configured runner is {runner_type!r}"
            )
        self._executor = _NativeExecutor(False, "")
        self._local_physical_plan = local_physical_plan
        self._ctx = ctx
        self._context = dict(context or {})
        self._fingerprint = secrets.randbits(63) or 1
        self._session_id = f"native-context-{self._fingerprint:016x}"
        self._context["plan_fingerprint"] = str(self._fingerprint)
        self._context["context_session_id"] = self._session_id
        self._maintain_order = maintain_order
        self._next_input_id = 0
        self._active_round: NativeContextRound | None = None
        self._closed = False
        if protocol_config is None:
            self._protocol: ContextProtocol | None = None
        else:
            from daft.execution.context_protocol import ContextProtocol

            self._protocol = ContextProtocol(self._session_id, protocol_config)

    @property
    def fingerprint(self) -> int:
        return self._fingerprint

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def protocol(self) -> ContextProtocol:
        if self._protocol is None:
            raise RuntimeError("Native context session was created without a context protocol")
        return self._protocol

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def active_plan_count(self) -> int:
        return self._executor.active_plan_count()

    def run_round(
        self,
        inputs: Mapping[int, Input | list[PyMicroPartition]],
    ) -> NativeContextRound:
        """Submit one bounded input round.

        The experimental wrapper permits one submitted round at a time. Consume
        or close the returned iterator before submitting the next round.
        """
        if self._closed:
            raise RuntimeError("Native context session is closed")
        if self._active_round is not None:
            raise RuntimeError("Native context session already has an active input round")

        input_id = self._next_input_id
        self._next_input_id += 1
        event_loop = get_or_init_event_loop()
        try:
            result_handle = event_loop.run(
                _submit_context_round(
                    self._executor,
                    self._local_physical_plan,
                    self._ctx,
                    input_id,
                    dict(inputs),
                    self._context,
                    self._maintain_order,
                )
            )
        except BaseException:
            self.cancel()
            raise

        result_round = NativeContextRound(self, input_id, result_handle)
        self._active_round = result_round
        return result_round

    def run_context_round(
        self,
        inputs: Mapping[int, Input | list[PyMicroPartition]],
        *,
        replace_seeds: Sequence[Hashable] | None = None,
        row_transform: RowTransformRequest | None = None,
        lease_timeout: float | None = None,
    ) -> ContextBatch:
        """Run one retained round and return leased rows plus row-keyed changes."""
        return self.start_context_round(
            inputs,
            replace_seeds=replace_seeds,
            row_transform=row_transform,
            lease_timeout=lease_timeout,
        ).collect()

    def start_context_round(
        self,
        inputs: Mapping[int, Input | list[PyMicroPartition]],
        *,
        replace_seeds: Sequence[Hashable] | None = None,
        row_transform: RowTransformRequest | None = None,
        lease_timeout: float | None = None,
    ) -> NativePendingContextRound:
        """Reserve output credit, then submit one retained context round."""
        if self._closed:
            raise RuntimeError("Native context session is closed")
        if self._active_round is not None:
            raise RuntimeError("Native context session already has an active input round")

        reservation = self.protocol.reserve_result(timeout=lease_timeout)
        try:
            result_round = self.run_round(inputs)
        except BaseException:
            reservation.release()
            raise
        return NativePendingContextRound(
            self,
            result_round,
            reservation,
            replace_seeds,
            row_transform,
        )

    def update_source(
        self,
        source_snapshot: SourceSnapshot,
        changed_rows: Mapping[str, Sequence[Hashable]],
    ) -> int:
        """Advance the source token and invalidate only named cached rows."""
        return self.protocol.update_source(source_snapshot, changed_rows)

    def close(self) -> None:
        """Cleanly close an idle session and wait for the retained pipeline."""
        if self._closed:
            return
        if self._active_round is not None:
            raise RuntimeError("Consume or close the active input round before closing the session")
        if self._protocol is not None:
            self._protocol.close()

        try:
            get_or_init_event_loop().run(_close_context_plan(self._executor, self._fingerprint))
        finally:
            self._closed = True

    def cancel(self) -> None:
        """Cancel the retained pipeline and discard cached plan state."""
        if self._closed:
            return
        if self._protocol is not None:
            self._protocol.cancel()
        self._executor.cancel_plan(self._fingerprint)
        self._active_round = None
        self._closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if exc_type is None:
            self.close()
        else:
            self.cancel()

    def _round_finished(self, result_round: NativeContextRound) -> None:
        if self._active_round is result_round:
            self._active_round = None

    def _round_failed(self, result_round: NativeContextRound) -> None:
        if self._active_round is result_round:
            self._active_round = None
        self.cancel()
