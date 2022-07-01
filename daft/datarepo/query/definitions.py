from typing import Any, Callable, Dict, Literal, Union

from daft.dataclasses import dataclass
from icebridge.client import IcebergExpression, IceBridgeClient

# Node IDs in the NetworkX graph are uuid strings
NodeId = str

@dataclass
class WriteDatarepoStageOutput:
    filepath: str
