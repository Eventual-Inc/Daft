from __future__ import annotations

from abc import ABC 
from dataclasses import dataclass


@dataclass(frozen=True)
class Pushdowns(ABC):
    """Marker interface for pushdown objects."""
    pass
