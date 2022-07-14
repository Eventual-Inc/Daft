import datetime
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel


class NotebookState(str, Enum):
    not_started = "not_started"
    pending = "pending"
    ready = "ready"


class UserNotebookDetails(BaseModel):
    url: Optional[str]
    ready: bool
    pending: Optional[str]


class LaunchNotebookRequest(BaseModel):
    image: str = "jupyter/singleuser:latest"


class RayClusterType(str, Enum):
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"


class RayClusterTypeConfig(BaseModel):
    head_cpu: int
    head_memory: str
    worker_cpu: int
    worker_memory: str
    max_workers: int


class RayClusterState(str, Enum):
    PENDING = "pending"
    READY = "ready"


class RayCluster(BaseModel):
    name: str
    namespace: str
    type: RayClusterType
    started_at: datetime.datetime
    state: RayClusterState = RayClusterState.PENDING
    endpoint: Optional[str] = None
    workers: int = 0


class KuberayClientConfig(BaseModel):
    cluster_configs: Dict[RayClusterType, RayClusterTypeConfig]
    template: Dict[str, Any]


class LaunchRayClusterRequest(BaseModel):
    name: str
    type: RayClusterType


class DeleteRayClusterRequest(BaseModel):
    name: str
