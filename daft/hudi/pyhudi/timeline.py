from dataclasses import dataclass
from enum import Enum
from typing import List


class State(Enum):
    REQUESTED = "requested",
    INFLIGHT = "inflight",
    COMPLETED = "completed"


@dataclass
class Instant:
    state: State
    action: str
    timestamp: str
    state_transition_time: str = None


@dataclass
class Timeline:
    instants: List[Instant]

    def get_completed_instants(self) -> List[Instant]:
        return filter(lambda i: i.state == State.COMPLETED, self.instants)
