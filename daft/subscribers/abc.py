from __future__ import annotations

import warnings
from abc import ABC
from functools import singledispatchmethod
from typing import TYPE_CHECKING, Any

from daft.daft import StatType
from daft.subscribers.events import OperatorFinished, OperatorStarted, Stats

if TYPE_CHECKING:
    from collections.abc import Mapping

    from daft.daft import PyMicroPartition, PyQueryMetadata, PyQueryResult


class Subscriber(ABC):
    """Experimental subscriber API for Daft's query lifecycle.

    This API is under active development and may change in minor releases.
    Backward compatibility is not yet guaranteed.
    """

    @singledispatchmethod
    def on_event(self, event: object) -> None:
        """Unified dispatch for execution events."""
        warnings.warn(f"Unhandled event type: {type(event).__name__}", stacklevel=2)

    def close(self) -> None:
        """Called when the subscriber is detached or the process is shutting down.

        Override to release resources (file handles, connections, etc.).
        """
        pass

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

    def on_query_start(self, query_id: str, metadata: PyQueryMetadata) -> None:
        """Called when starting the run for a new query."""
        pass

    def on_query_end(self, query_id: str, result: PyQueryResult) -> None:
        """Called when a query has completed."""
        pass

    def on_result_out(self, query_id: str, result: PyMicroPartition) -> None:
        """Called when a result is emitted for a query."""
        pass

    def on_optimization_start(self, query_id: str) -> None:
        """Called when starting to plan / optimize a query."""
        pass

    def on_optimization_end(self, query_id: str, optimized_plan: str) -> None:
        """Called when planning for a query has completed."""
        pass

    def on_exec_start(self, query_id: str, physical_plan: str) -> None:
        """Called when starting to execute a query. Receives the physical plan as JSON string."""
        pass

    def on_exec_operator_start(self, query_id: str, node_id: int) -> None:
        """Deprecated: use on_operator_start instead."""
        self.on_operator_start(OperatorStarted(query_id=query_id, node_id=node_id, name=""))

    def on_exec_emit_stats(self, query_id: str, stats: Mapping[int, Mapping[str, tuple[StatType, Any]]]) -> None:
        """Deprecated: use on_stats instead."""
        normalized_stats = {node_id: dict(node_stats.items()) for node_id, node_stats in stats.items()}
        self.on_stats(Stats(query_id=query_id, stats=normalized_stats))

    def on_exec_operator_end(self, query_id: str, node_id: int) -> None:
        """Deprecated: use on_operator_end instead."""
        self.on_operator_end(OperatorFinished(query_id=query_id, node_id=node_id, name=""))

    def on_exec_end(self, query_id: str) -> None:
        """Called when a query has finished executing."""
        pass

    def on_process_stats(self, query_id: str, stats: Mapping[str, tuple[StatType, Any]]) -> None:
        """Called with process-level stats (memory, CPU) on each tick.

        Override to capture process-level metrics. Not abstract - defaults to no-op.
        """
        pass


__all__ = ["StatType", "Subscriber"]
