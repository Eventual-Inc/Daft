from __future__ import annotations

import pathlib
import tempfile

import docker
import numpy as np
import pytest
import requests

from daft.dataframe import DataFrame
from daft.experimental.serving import HTTPEndpoint
from daft.experimental.serving.backend import get_serving_backend
from daft.experimental.serving.backends import (
    DockerEndpointBackend,
    MultiprocessingEndpointBackend,
)
from daft.experimental.serving.env import DaftEnv, get_docker_client
from daft.expressions import ColumnExpression
from daft.logical.schema import ExpressionList

TEST_BACKEND_CONFIG = {
    "mp": {"type": "multiprocessing"},
    "docker": {"type": "docker"},
}

FAKE_ENDPOINT_NAME = "test-endpoint"
SCHEMA = ExpressionList([ColumnExpression("foo")])


@pytest.fixture(scope="function")
def multiprocessing_backend():
    return get_serving_backend(name="mp", configs=TEST_BACKEND_CONFIG)


@pytest.fixture(scope="function")
def docker_backend():
    return get_serving_backend(name="docker", configs=TEST_BACKEND_CONFIG)


@pytest.mark.skip(reason="Serving not implemented")
def test_identity_dataframe_serving_multiprocessing(multiprocessing_backend: MultiprocessingEndpointBackend) -> None:
    endpoint = HTTPEndpoint(SCHEMA, backend=multiprocessing_backend)
    df = DataFrame.from_endpoint(endpoint)
    df.write_endpoint(endpoint)

    # HACK(jay): Override ._plan with a mock function to execute, because we don't yet have a runner that can run ._plan
    def endpoint_func(request: str) -> str:
        return request

    endpoint._plan = endpoint_func

    deployed_endpoint = endpoint.deploy(FAKE_ENDPOINT_NAME)

    # TODO(jay): Replace with actual logic when the endpoint is deployed with a runner
    response = requests.get(f"{deployed_endpoint.addr}?request=foo")
    assert response.text == '"foo"'


@pytest.mark.skip(reason="Serving not implemented")
@pytest.mark.conda
def test_identity_dataframe_serving_multiprocessing_with_pip_dependency(
    multiprocessing_backend: MultiprocessingEndpointBackend,
) -> None:
    endpoint = HTTPEndpoint(SCHEMA, backend=multiprocessing_backend, custom_env=DaftEnv(pip_packages=["numpy"]))
    df = DataFrame.from_endpoint(endpoint)
    df.write_endpoint(endpoint)

    # HACK(jay): Override ._plan with a mock function to execute, because we don't yet have a runner that can run ._plan
    def endpoint_func(request: str) -> float:
        return np.sum(np.ones(int(request)))

    endpoint._plan = endpoint_func

    deployed_endpoint = endpoint.deploy(FAKE_ENDPOINT_NAME)

    # TODO(jay): Replace with actual logic when the endpoint is deployed with a runner
    response = requests.get(f"{deployed_endpoint.addr}?request=5")
    assert response.text == "5.0"


@pytest.mark.skip(reason="Serving not implemented")
@pytest.mark.conda
def test_identity_dataframe_serving_multiprocessing_with_requirements_txt(
    multiprocessing_backend: DockerEndpointBackend,
) -> None:
    with tempfile.NamedTemporaryFile(mode="w") as requirements_txt:
        requirements_txt.write("numpy")
        requirements_txt.flush()

        endpoint = HTTPEndpoint(
            SCHEMA, backend=multiprocessing_backend, custom_env=DaftEnv(requirements_txt=requirements_txt.name)
        )
        df = DataFrame.from_endpoint(endpoint)
        df.write_endpoint(endpoint)

        # HACK(jay): Override ._plan with a mock function to execute, because we don't yet have a runner that can run ._plan
        def endpoint_func(request: str) -> float:
            return np.sum(np.ones(int(request)))

        endpoint._plan = endpoint_func
        deployed_endpoint = endpoint.deploy(FAKE_ENDPOINT_NAME)

        # TODO(jay): Replace with actual logic when the endpoint is deployed with a runner
        response = requests.get(f"{deployed_endpoint.addr}?request=5")
        assert response.text == "5.0"


@pytest.mark.skip(reason="Serving not implemented")
@pytest.mark.docker
def test_identity_dataframe_serving_docker(docker_backend: DockerEndpointBackend) -> None:
    endpoint = HTTPEndpoint(SCHEMA, backend=docker_backend)
    df = DataFrame.from_endpoint(endpoint)
    df.write_endpoint(endpoint)

    # HACK(jay): Override ._plan with a mock function to execute, because we don't yet have a runner that can run ._plan
    def endpoint_func(request: str) -> str:
        return request

    endpoint._plan = endpoint_func

    deployed_endpoint = endpoint.deploy(FAKE_ENDPOINT_NAME)

    try:
        # TODO(jay): Replace with actual logic when the endpoint is deployed with a runner
        response = requests.get(f"{deployed_endpoint.addr}?request=foo")
        assert response.text == '"foo"'
    finally:
        docker_client = get_docker_client()
        try:
            docker_client.containers.get(f"daft-endpoint-{deployed_endpoint.name}-v{deployed_endpoint.version}").kill()
        except docker.errors.NotFound:
            pass


@pytest.mark.skip(reason="Serving not implemented")
@pytest.mark.docker
def test_identity_dataframe_serving_docker_with_pip_dependency(docker_backend: DockerEndpointBackend) -> None:
    endpoint = HTTPEndpoint(SCHEMA, backend=docker_backend, custom_env=DaftEnv(pip_packages=["numpy"]))
    df = DataFrame.from_endpoint(endpoint)
    df.write_endpoint(endpoint)

    # HACK(jay): Override ._plan with a mock function to execute, because we don't yet have a runner that can run ._plan
    def endpoint_func(request: str) -> float:
        return np.sum(np.ones(int(request)))

    endpoint._plan = endpoint_func

    deployed_endpoint = endpoint.deploy(FAKE_ENDPOINT_NAME)

    try:
        # TODO(jay): Replace with actual logic when the endpoint is deployed with a runner
        response = requests.get(f"{deployed_endpoint.addr}?request=5")
        assert response.text == "5.0"
    finally:
        docker_client = get_docker_client()
        try:
            docker_client.containers.get(f"daft-endpoint-{deployed_endpoint.name}-v{deployed_endpoint.version}").kill()
        except docker.errors.NotFound:
            pass


@pytest.mark.skip(reason="Serving not implemented")
@pytest.mark.docker
def test_identity_dataframe_serving_docker_with_requirements_txt(docker_backend: DockerEndpointBackend) -> None:
    with tempfile.NamedTemporaryFile(mode="w") as requirements_txt:
        requirements_txt.write("numpy")
        requirements_txt.flush()

        endpoint = HTTPEndpoint(
            SCHEMA, backend=docker_backend, custom_env=DaftEnv(requirements_txt=requirements_txt.name)
        )
        df = DataFrame.from_endpoint(endpoint)
        df.write_endpoint(endpoint)

        # HACK(jay): Override ._plan with a mock function to execute, because we don't yet have a runner that can run ._plan
        def endpoint_func(request: str) -> float:
            return np.sum(np.ones(int(request)))

        endpoint._plan = endpoint_func
        deployed_endpoint = endpoint.deploy(FAKE_ENDPOINT_NAME)

        try:
            # TODO(jay): Replace with actual logic when the endpoint is deployed with a runner
            response = requests.get(f"{deployed_endpoint.addr}?request=5")
            assert response.text == "5.0"
        finally:
            docker_client = get_docker_client()
            try:
                docker_client.containers.get(
                    f"daft-endpoint-{deployed_endpoint.name}-v{deployed_endpoint.version}"
                ).kill()
            except docker.errors.NotFound:
                pass


@pytest.mark.docker
@pytest.mark.skip(reason="Not implemented yet")
def test_identity_dataframe_serving_docker_with_local_pkg(docker_backend: DockerEndpointBackend) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = pathlib.Path(tmpdir)
        setup_py = tmpdir / "setup.py"
        setup_py.write_text(
            "from setuptools import setup; setup(name='fake_pkg', version='0.0.1', packages=['fake_pkg'])"
        )
        pkg_dir = tmpdir / "fake_pkg"
        pkg_dir.mkdir()
        pkg_init = tmpdir / "fake_pkg" / "__init__.py"
        pkg_init.write_text("def foo(request: str) -> int: return int(request)")

        endpoint = HTTPEndpoint(SCHEMA, docker_backend, custom_env=DaftEnv(local_packages=[str(tmpdir)]))
        df = DataFrame.from_endpoint(endpoint)
        df.write_endpoint(endpoint)

        # HACK(jay): Override ._plan with a mock function to execute, because we don't yet have a runner that can run ._plan
        def endpoint_func(request: str) -> int:
            import fake_pkg

            return fake_pkg.foo(request)

        endpoint._plan = endpoint_func
        deployed_endpoint = endpoint.deploy(FAKE_ENDPOINT_NAME)

        try:
            # TODO(jay): Replace with actual logic when the endpoint is deployed with a runner
            response = requests.get(f"{deployed_endpoint.addr}?request=5")
            assert response.text == "5"
        finally:
            docker_client = get_docker_client()
            try:
                docker_client.containers.get(
                    f"daft-endpoint-{deployed_endpoint.name}-v{deployed_endpoint.version}"
                ).kill()
            except docker.errors.NotFound:
                pass
