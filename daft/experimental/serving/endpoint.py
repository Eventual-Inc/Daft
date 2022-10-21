from __future__ import annotations

from daft.experimental.serving.backend import (
    AbstractEndpointBackend,
    get_serving_backend,
)
from daft.experimental.serving.definitions import Endpoint
from daft.experimental.serving.env import DaftEnv
from daft.logical.logical_plan import HTTPResponse, LogicalPlan
from daft.logical.schema import ExpressionList


class HTTPEndpoint:
    """A HTTP Endpoint that can be configured to run the same logic as a DataFrame, and be deployed onto various backends"""

    def __init__(
        self,
        request_schema: ExpressionList,
        backend: AbstractEndpointBackend | None = None,
        custom_env: DaftEnv | None = None,
    ):
        self._request_schema = request_schema
        self._plan: LogicalPlan | None = None
        self._backend = backend if backend is not None else get_serving_backend()
        self._custom_env = custom_env

    def _set_plan(self, plan: LogicalPlan) -> None:
        if self._plan is not None:
            raise ValueError("Unable to .set_plan more than once on the same HTTPEndpoint")
        self._plan = HTTPResponse(input=plan)
        return

    def deploy(
        self,
        endpoint_name: str,
    ) -> Endpoint:
        if self._plan is None:
            raise RuntimeError("Unable to deploy HTTPEndpoint without a plan")

        # TODO(jay): In the absence of a runner, we deploy whatever the ._plan is as a function. This is a hack and
        # is only meant to be used in unit tests by monkey-patching ._plan for now to test the e2e flow.
        if isinstance(self._plan, LogicalPlan):
            raise RuntimeError("HTTPEndpoint unable to deploy LogicalPlans until runners are implemented")
        endpoint_func = self._plan

        return self._backend.deploy_endpoint(endpoint_name, endpoint_func, custom_env=self._custom_env)
