from __future__ import annotations

import dataclasses


@dataclasses.dataclass(frozen=True)
class Endpoint:
    name: str
    version: int
    addr: str
