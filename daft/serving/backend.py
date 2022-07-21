import dataclasses
import logging
import pathlib
import re
import shutil
import socket
import subprocess
import tempfile
from typing import Any, Callable, List, Protocol

import boto3
import cloudpickle
import docker

from daft.serving.docker import build_image

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class Endpoint:
    name: str
    version: int
    addr: str


class AbstractEndpointBackend(Protocol):
    """Manages Daft Serving endpoints for a given backend"""

    """Lists all endpoints managed by this endpoint manager"""

    def list_endpoints(self) -> List[Endpoint]:
        ...

    """Deploys an endpoint managed by this endpoint manager"""

    def deploy_endpoint(
        self, endpoint_name: str, endpoint: Callable[[Any], Any], pip_dependencies: List[str] = []
    ) -> Endpoint:
        ...


class AWSLambdaEndpointBackend(AbstractEndpointBackend):
    """Manages Daft Serving endpoints on AWS Lambda

    Limitations:

        1. Only deploys public endpoints and no auth is performed when requesting
        2. Only allows for 50MB packages, using images from ECR (up to 10GB) is not yet supported
    """

    DAFT_REQUIRED_DEPS = ["cloudpickle"]

    def __init__(self):
        self.docker_client = docker.from_env()
        self.lambda_client = boto3.client("lambda")
        self.role_arn = "arn:aws:iam::941892620273:role/jay-daft-serving-testrole-OKTODELETE"

    def _list_daft_serving_lambda_functions(self) -> List[dict]:
        aws_lambda_functions = []
        function_paginator = self.lambda_client.get_paginator("list_functions")
        for page in function_paginator.paginate():
            for aws_lambda_function in page["Functions"]:
                if not aws_lambda_function["FunctionName"].startswith("daft-serving-"):
                    continue

                # API does not provide tags by default on list operations, we do it manually here
                aws_lambda_function["Tags"] = self.lambda_client.list_tags(
                    Resource=self._strip_function_arn_version(aws_lambda_function["FunctionArn"])
                )["Tags"]

                aws_lambda_functions.append(aws_lambda_function)
        return aws_lambda_functions

    def _strip_function_arn_version(self, function_arn: str) -> str:
        if re.match(
            r"arn:(aws[a-zA-Z-]*)?:lambda:[a-z]{2}(-gov)?-[a-z]+-\d{1}:\d{12}:function:[a-zA-Z0-9-_]+:(\$LATEST|[a-zA-Z0-9-_]+)",
            function_arn,
        ):
            return function_arn.rsplit(":", 1)[0]
        return function_arn

    def list_endpoints(self) -> List[Endpoint]:
        aws_lambda_functions = self._list_daft_serving_lambda_functions()

        # Each function should have been created with a corresponding URL config, but if it hasn't we will
        # return None for the URL instead.
        aws_lambda_url_configs = []
        for f in aws_lambda_functions:
            try:
                aws_lambda_url_configs.append(
                    self.lambda_client.get_function_url_config(FunctionName=f["FunctionName"])
                )
            except self.lambda_client.exceptions.ResourceNotFoundException:
                aws_lambda_url_configs.append(None)

        return [
            Endpoint(
                name=f["FunctionName"],
                version=f["Tags"]["endpoint_version"],
                addr=url_config["FunctionUrl"] if url_config else None,
            )
            for f, url_config in zip(aws_lambda_functions, aws_lambda_url_configs)
        ]

    def deploy_endpoint(
        self, endpoint_name: str, endpoint: Callable[[Any], Any], pip_dependencies: List[str] = []
    ) -> Endpoint:
        lambda_function_name = f"daft-serving-{endpoint_name}"
        lambda_function_version = 1

        # Check for existing function
        try:
            old_function = self.lambda_client.get_function(FunctionName=lambda_function_name)
            lambda_function_version = int(old_function["Tags"]["endpoint_version"]) + 1
        except self.lambda_client.exceptions.ResourceNotFoundException:
            pass

        # Build the zip file
        with tempfile.TemporaryDirectory() as td, tempfile.TemporaryDirectory() as zipfile_td:
            tmpdir = pathlib.Path(td)
            shutil.copy2(
                pathlib.Path(__file__).parent / "static" / "aws-lambda-entrypoint.py",
                tmpdir / "aws-lambda-entrypoint.py",
            )
            proc = subprocess.run(
                [
                    "pip",
                    "install",
                    "-t",
                    td,
                    "--platform=manylinux1_x86_64",
                    "--only-binary=:all:",
                    "--python-version=3.9",
                    *AWSLambdaEndpointBackend.DAFT_REQUIRED_DEPS,
                    *pip_dependencies,
                ]
            )
            proc.check_returncode()
            pickle_file = tmpdir / "endpoint.pkl"
            with open(pickle_file, "wb") as f:
                f.write(cloudpickle.dumps(endpoint))

            # Create Lambda function
            lambda_layer_path = pathlib.Path(zipfile_td) / "lambda_layer"
            shutil.make_archive(base_name=str(lambda_layer_path), format="zip", root_dir=td)
            with open(f"{lambda_layer_path}.zip", "rb") as f:
                zipfile_bytes = f.read()

                if lambda_function_version > 1:
                    response = self.lambda_client.update_function_code(
                        FunctionName=lambda_function_name,
                        ZipFile=zipfile_bytes,
                        Publish=True,
                    )
                    self.lambda_client.tag_resource(
                        Resource=self._strip_function_arn_version(response["FunctionArn"]),
                        Tags={
                            "endpoint_version": str(lambda_function_version),
                        },
                    )
                else:
                    self.lambda_client.create_function(
                        FunctionName=lambda_function_name,
                        Runtime="python3.9",
                        Handler="aws-lambda-entrypoint.lambda_handler",
                        Code={"ZipFile": zipfile_bytes},
                        Description="Daft serving endpoint",
                        Environment={
                            "Variables": {"ENDPOINT_PKL_FILEPATH": "endpoint.pkl"},
                        },
                        Architectures=["x86_64"],
                        Tags={
                            "owner": "daft-serving",
                            "endpoint_name": endpoint_name,
                            "endpoint_version": str(lambda_function_version),
                        },
                        Role=self.role_arn,
                        Publish=True,
                    )

        # Add permission for anyone to invoke the lambda function
        try:
            self.lambda_client.add_permission(
                FunctionName=lambda_function_name,
                StatementId="public-invoke",
                Action="lambda:InvokeFunctionUrl",
                Principal="*",
                FunctionUrlAuthType="NONE",
            )
        except self.lambda_client.exceptions.ResourceConflictException:
            pass

        # Create an endpoint with Lambda URL
        try:
            url_config = self.lambda_client.get_function_url_config(FunctionName=lambda_function_name)
        except self.lambda_client.exceptions.ResourceNotFoundException:
            url_config = self.lambda_client.create_function_url_config(
                FunctionName=lambda_function_name,
                AuthType="NONE",
            )

        return Endpoint(
            name=endpoint_name,
            version=lambda_function_version,
            addr=url_config["FunctionUrl"],
        )


class DockerEndpointBackend(AbstractEndpointBackend):
    """Manages Daft Serving endpoints for a Docker backend"""

    REQUIREMENTS_TXT_FILENAME = "requirements.txt"
    ENDPOINT_PKL_FILENAME = "endpoint.pkl"

    DAFT_ENDPOINT_VERSION_LABEL = "DAFT_ENDPOINT_VERSION"
    DAFT_ENDPOINT_NAME_LABEL = "DAFT_ENDPOINT_NAME"
    DAFT_ENDPOINT_PORT_LABEL = "DAFT_ENDPOINT_PORT"

    def __init__(self):
        self.docker_client = docker.from_env()

    def _run_container(self, endpoint_name: str, img: docker.models.images.Image) -> docker.models.containers.Container:
        """Runs a Docker container from the given image"""
        version = 0
        port = self._get_free_local_port()

        # Check and remove existing container with the same name
        containers = [
            c
            for c in self._get_daft_serving_containers()
            if c.labels[DockerEndpointBackend.DAFT_ENDPOINT_NAME_LABEL] == endpoint_name
        ]
        if containers:
            assert len(containers) == 1, "Multiple endpoints with the same name are not supported"
            match = containers[0]
            old_version = int(match.labels[DockerEndpointBackend.DAFT_ENDPOINT_VERSION_LABEL])
            version = old_version + 1
            port = match.labels[DockerEndpointBackend.DAFT_ENDPOINT_PORT_LABEL]
            print(f"Tearing down existing endpoint {endpoint_name}/v{old_version}")
            match.stop()

        try:
            container = self.docker_client.containers.run(
                img,
                name=f"daft-endpoint-{endpoint_name}",
                ports={f"8000/tcp": str(port)},
                detach=True,
                auto_remove=True,
                labels={
                    DockerEndpointBackend.DAFT_ENDPOINT_NAME_LABEL: endpoint_name,
                    DockerEndpointBackend.DAFT_ENDPOINT_PORT_LABEL: str(port),
                    DockerEndpointBackend.DAFT_ENDPOINT_VERSION_LABEL: str(version),
                },
            )
            print(f"Started endpoint {endpoint_name}/v{version} at http://localhost:{port}")
            return container
        except docker.errors.APIError as e:
            raise RuntimeError(e)

    def _get_free_local_port(self) -> int:
        """Returns a free local port"""
        sock = socket.socket()
        sock.bind(("", 0))
        return int(sock.getsockname()[1])

    def _get_daft_serving_containers(self) -> List[docker.models.containers.Container]:
        """Returns all Daft Serving containers"""
        return [
            container
            for container in self.docker_client.containers.list()
            if container.labels.get(DockerEndpointBackend.DAFT_ENDPOINT_NAME_LABEL)
        ]

    def list_endpoints(self) -> List[Endpoint]:
        """Lists all endpoints managed by this endpoint manager"""
        containers = self._get_daft_serving_containers()
        return [
            Endpoint(
                name=c.labels[DockerEndpointBackend.DAFT_ENDPOINT_NAME_LABEL],
                addr=f"http://localhost:{c.labels['DAFT_ENDPOINT_PORT']}",
                version=int(c.labels[DockerEndpointBackend.DAFT_ENDPOINT_VERSION_LABEL]),
            )
            for c in containers
        ]

    def deploy_endpoint(
        self, endpoint_name: str, endpoint: Callable[[Any], Any], pip_dependencies: List[str] = []
    ) -> Endpoint:
        img = build_image(self.docker_client, endpoint_name, endpoint, pip_dependencies=pip_dependencies)
        container = self._run_container(endpoint_name, img)
        return Endpoint(
            name=endpoint_name,
            addr=f"http://localhost:{container.labels['DAFT_ENDPOINT_PORT']}",
            version=int(container.labels[DockerEndpointBackend.DAFT_ENDPOINT_VERSION_LABEL]),
        )
