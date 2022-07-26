import pytest
import requests

from daft.dataframe import DataFrame
from daft.expressions import ColumnExpression
from daft.logical.schema import ExpressionList
from daft.serving import HTTPEndpoint, ServingClient

CONFIGS = [
    {
        "name": "test-backend",
        "config": {"type": "multiprocessing"},
    }
]

FAKE_ENDPOINT_NAME = "test-endpoint"
SCHEMA = ExpressionList([ColumnExpression("foo")])


@pytest.fixture(scope="function")
def serving_client():
    return ServingClient.from_configs(CONFIGS)


def test_identity_dataframe_serving(serving_client: ServingClient) -> None:
    endpoint = HTTPEndpoint(SCHEMA)
    df = DataFrame.from_endpoint(endpoint)
    df.write_endpoint(endpoint)

    deployed_endpoint = endpoint.deploy(FAKE_ENDPOINT_NAME, "test-backend", serving_client)

    # TODO(jay): Replace with actual logic when the endpoint is deployed with a runner
    response = requests.get(f"{deployed_endpoint.addr}?request=foo")
    assert response.text == '"foo"'
