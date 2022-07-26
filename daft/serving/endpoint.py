from typing import Optional

from daft.logical.logical_plan import HTTPResponse, LogicalPlan
from daft.logical.schema import ExpressionList
from daft.serving.client import ServingClient
from daft.serving.definitions import Endpoint


class HTTPEndpoint:
    """A HTTP Endpoint that can be configured to run the same logic as a DataFrame, and be deployed onto various backends"""

    def __init__(self, request_schema: ExpressionList):
        self._request_schema = request_schema
        self._plan: Optional[LogicalPlan] = None

    def _set_plan(self, plan: LogicalPlan) -> None:
        if self._plan is not None:
            raise ValueError("Unable to .set_plan more than once on the same HTTPEndpoint")
        self._plan = HTTPResponse(input=plan)
        return

    def deploy(self, endpoint_name: str, backend: str, client: ServingClient) -> Endpoint:
        print(f"Currently stubbed, but supposed to be deploying plan: {self._plan}")
        # TODO(jay): Replace with an actual runner that takes as a parameter the plan object and is able to process incoming requests
        def endpoint_func(request: str) -> str:
            return request

        return client.deploy(endpoint_name, endpoint_func, backend=backend)
