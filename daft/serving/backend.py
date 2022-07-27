from __future__ import annotations

import logging
from typing import Any, Callable, List, Optional, Protocol

import pydantic

from daft.env import DaftEnv
from daft.serving.definitions import Endpoint

logger = logging.getLogger(__name__)


class AbstractEndpointBackend(Protocol):
    """Manages Daft Serving endpoints for a given backend"""

    @classmethod
    def from_config(cls, config: pydantic.BaseModel) -> AbstractEndpointBackend:
        """Instantiates an endpoint manager for the given backend configuration"""
        ...

    @staticmethod
    def config_type_id() -> str:
        """Returns the literal string that is used to indicate that this backend is to be used in a config file"""
        ...

    def list_endpoints(self) -> List[Endpoint]:
        """Lists all endpoints managed by this endpoint manager"""
        ...

    def deploy_endpoint(
        self,
        endpoint_name: str,
        endpoint: Callable[[Any], Any],
        custom_env: Optional[DaftEnv] = None,
    ) -> Endpoint:
        """Deploys an endpoint managed by this endpoint manager"""
        ...
