from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from daft.daft import StatType


@dataclass(frozen=True)
class OperatorStarted:
    query_id: str
    node_id: int
    name: str


@dataclass(frozen=True)
class OperatorFinished:
    query_id: str
    node_id: int
    name: str


@dataclass(frozen=True)
class Stats:
    query_id: str
    stats: dict[int, dict[str, tuple[StatType, Any]]]


Event = OperatorStarted | OperatorFinished | Stats
