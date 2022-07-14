from typing import Any, Callable, Dict, Literal, Union

from icebridge.client import IcebergExpression, IceBridgeClient

from daft.dataclasses import dataclass

# Node IDs in the NetworkX graph are uuid strings
NodeId = str


@dataclass
class WriteDatarepoStageOutput:
    filepath: str
