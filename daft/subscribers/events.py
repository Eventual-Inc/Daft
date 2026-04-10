from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from daft.daft import PyQueryMetadata, PyQueryResult, StatType


class Event(ABC):
    """Base class for subscriber execution events."""


@dataclass(frozen=True)
class QueryStarted(Event):
    query_id: str
    metadata: PyQueryMetadata


@dataclass(frozen=True)
class QueryHeartbeat(Event):
    query_id: str


@dataclass(frozen=True)
class QueryFinished(Event):
    query_id: str
    result: PyQueryResult
    duration_ms: float | None


@dataclass(frozen=True)
class OptimizationStarted(Event):
    query_id: str


@dataclass(frozen=True)
class OptimizationCompleted(Event):
    query_id: str
    optimized_plan: str


@dataclass(frozen=True)
class ExecutionStarted(Event):
    query_id: str
    physical_plan: str


@dataclass(frozen=True)
class ExecutionFinished(Event):
    query_id: str
    duration_ms: float | None


@dataclass(frozen=True)
class OperatorStarted(Event):
    query_id: str
    node_id: int
    name: str


@dataclass(frozen=True)
class OperatorFinished(Event):
    query_id: str
    node_id: int
    name: str


@dataclass(frozen=True)
class Stats(Event):
    query_id: str
    stats: dict[int, dict[str, tuple[StatType, Any]]]


@dataclass(frozen=True)
class ProcessStats(Event):
    query_id: str
    stats: dict[str, tuple[StatType, Any]]


@dataclass(frozen=True)
class ResultProduced(Event):
    query_id: str
    num_rows: int
