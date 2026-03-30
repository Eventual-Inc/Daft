from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from daft.daft import StatType


class Event(ABC):
    """Base class for subscriber execution events."""


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
