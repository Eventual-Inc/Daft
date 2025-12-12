from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from daft.daft import StatType

if TYPE_CHECKING:
    from collections.abc import Mapping

    from daft.daft import PyMicroPartition, PyQueryMetadata, PyQueryResult


class Subscriber(ABC):
    """A framework for subscribing to Daft's query lifecycle.

    The engine triggers the subscriber methods as callbacks at the following points:
    - Query from start to end
    - Optimization from start to end
    - Execution from start to end
    - The execution of an operator from start to end
    - During execution, emitting current stats of all running operators at regular intervals
    """

    @abstractmethod
    def on_query_start(self, query_id: str, metadata: PyQueryMetadata) -> None:
        """Called when starting the run for a new query."""
        pass

    @abstractmethod
    def on_query_end(self, query_id: str, result: PyQueryResult) -> None:
        """Called when a query has completed."""
        pass

    @abstractmethod
    def on_result_out(self, query_id: str, result: PyMicroPartition) -> None:
        """Called when a result is emitted for a query."""
        pass

    @abstractmethod
    def on_optimization_start(self, query_id: str) -> None:
        """Called when starting to plan / optimize a query."""
        pass

    @abstractmethod
    def on_optimization_end(self, query_id: str, optimized_plan: str) -> None:
        """Called when planning for a query has completed."""
        pass

    @abstractmethod
    def on_exec_start(self, query_id: str, physical_plan: str) -> None:
        """Called when starting to execute a query. Receives the physical plan as JSON string."""
        pass

    @abstractmethod
    def on_exec_operator_start(self, query_id: str, node_id: int) -> None:
        """Called when an operator has started executing."""
        pass

    @abstractmethod
    def on_exec_emit_stats(self, query_id: str, stats: Mapping[int, Mapping[str, tuple[StatType, Any]]]) -> None:
        """Called when emitting stats for all running operators in a query."""
        pass

    @abstractmethod
    def on_exec_operator_end(self, query_id: str, node_id: int) -> None:
        """Called when an operator has completed."""
        pass

    @abstractmethod
    def on_exec_end(self, query_id: str) -> None:
        """Called when a query has finished executing."""
        pass


__all__ = ["StatType", "Subscriber"]
