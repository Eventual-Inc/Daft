from __future__ import annotations

import warnings
from abc import ABC
from functools import singledispatchmethod
from typing import TYPE_CHECKING

from daft.daft import StatType
from daft.subscribers.events import (
    Event,
    ExecutionFinished,
    ExecutionStarted,
    OperatorFinished,
    OperatorStarted,
    OptimizationCompleted,
    OptimizationStarted,
    ProcessStats,
    QueryFinished,
    QueryHeartbeat,
    QueryStarted,
    ResultProduced,
    Stats,
)

if TYPE_CHECKING:
    from daft.daft import PyQueryMetadata, PyQueryResult


class Subscriber(ABC):
    """Experimental subscriber API for Daft's query lifecycle.

    This API is under active development and may change in minor releases.
    Backward compatibility is not yet guaranteed.
    """

    @singledispatchmethod
    def on_event(self, event: Event) -> None:
        """Unified dispatch for execution events."""
        warnings.warn(f"Unhandled event type: {type(event).__name__}", stacklevel=2)

    def close(self) -> None:
        """Called when the subscriber is detached or the process is shutting down.

        Override to release resources (file handles, connections, etc.).
        """
        pass

    @on_event.register
    def _(self, event: QueryStarted) -> None:
        self.on_query_started(event)

    @on_event.register
    def _(self, event: QueryHeartbeat) -> None:
        self.on_query_heartbeat(event)

    @on_event.register
    def _(self, event: QueryFinished) -> None:
        self.on_query_finished(event)

    @on_event.register
    def _(self, event: OptimizationStarted) -> None:
        self.on_optimization_started(event)

    @on_event.register
    def _(self, event: OptimizationCompleted) -> None:
        self.on_optimization_completed(event)

    @on_event.register
    def _(self, event: ExecutionStarted) -> None:
        self.on_execution_started(event)

    @on_event.register
    def _(self, event: ExecutionFinished) -> None:
        self.on_execution_finished(event)

    @on_event.register
    def _(self, event: ResultProduced) -> None:
        self.on_result_produced(event)

    @on_event.register
    def _(self, event: ProcessStats) -> None:
        self.on_process_stats(event)

    @on_event.register
    def _(self, event: OperatorStarted) -> None:
        self.on_operator_start(event)

    @on_event.register
    def _(self, event: OperatorFinished) -> None:
        self.on_operator_end(event)

    @on_event.register
    def _(self, event: Stats) -> None:
        self.on_stats(event)

    def on_operator_start(self, event: OperatorStarted) -> None:
        """Called when an operator has started executing."""
        pass

    def on_operator_end(self, event: OperatorFinished) -> None:
        """Called when an operator has completed."""
        pass

    def on_stats(self, event: Stats) -> None:
        """Called when emitting stats for all running operators in a query."""
        pass

    def on_query_started(self, event: QueryStarted) -> None:
        """Called when starting the run for a new query."""
        pass

    def on_query_heartbeat(self, event: QueryHeartbeat) -> None:
        """Called regularly while a query is running."""
        pass

    def on_query_finished(self, event: QueryFinished) -> None:
        """Called when a query has completed."""
        pass

    def on_optimization_completed(self, event: OptimizationCompleted) -> None:
        """Called when planning for a query has completed."""
        pass

    def on_optimization_started(self, event: OptimizationStarted) -> None:
        """Called when planning for a query starts."""
        pass

    def on_execution_started(self, event: ExecutionStarted) -> None:
        """Called when starting to execute a query."""
        pass

    def on_execution_finished(self, event: ExecutionFinished) -> None:
        """Called when a query has finished executing."""
        pass

    def on_result_produced(self, event: ResultProduced) -> None:
        """Called when a query emits result rows."""
        pass

    def on_query_start(self, query_id: str, metadata: PyQueryMetadata) -> None:
        """Called when starting the run for a new query."""
        self.on_query_started(QueryStarted(query_id=query_id, metadata=metadata))

    def on_query_end(self, query_id: str, result: PyQueryResult) -> None:
        """Called when a query has completed."""
        self.on_query_finished(QueryFinished(query_id=query_id, result=result, duration_ms=None))

    def on_process_stats(self, event: ProcessStats) -> None:
        """Called with process-level stats on each tick."""
        pass


__all__ = ["StatType", "Subscriber"]
