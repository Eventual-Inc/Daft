import pathlib
import re
import shutil
import subprocess
import tempfile
from typing import Any, Callable, List, Literal

import boto3
import cloudpickle
import pydantic

from daft.serving.backend import AbstractEndpointBackend
from daft.serving.endpoints import Endpoint

CONFIG_TYPE_ID = Literal["aws_lambda"]


class AWSLambdaBackendConfig(pydantic.BaseModel):
    type: CONFIG_TYPE_ID
    execution_role_arn: str


class AWSLambdaEndpointBackend(AbstractEndpointBackend):
    """Manages Daft Serving endpoints on AWS Lambda

    Limitations:

        1. Only deploys public endpoints and no auth is performed when requesting
        2. Only allows for 50MB packages, using images from ECR (up to 10GB) is not yet supported
    """

    DAFT_REQUIRED_DEPS = ["cloudpickle"]

    ENDPOINT_VERSION_TAG = "endpoint_version"
    FUNCTION_NAME_PREFIX = "daft-serving-"

    def __init__(self, config: AWSLambdaBackendConfig):
        self.lambda_client = boto3.client("lambda")
        self.role_arn = config.execution_role_arn

    @staticmethod
    def config_type_id() -> str:
        return "aws_lambda"

    @classmethod
    def from_config(cls, config: AWSLambdaBackendConfig) -> AbstractEndpointBackend:
        return cls(config)

    def _list_daft_serving_lambda_functions(self) -> List[dict]:
        aws_lambda_functions = []
        function_paginator = self.lambda_client.get_paginator("list_functions")
        for page in function_paginator.paginate():
            for aws_lambda_function in page["Functions"]:
                if not aws_lambda_function["FunctionName"].startswith(AWSLambdaEndpointBackend.FUNCTION_NAME_PREFIX):
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
                version=f["Tags"][AWSLambdaEndpointBackend.ENDPOINT_VERSION_TAG],
                addr=url_config["FunctionUrl"] if url_config else None,
            )
            for f, url_config in zip(aws_lambda_functions, aws_lambda_url_configs)
        ]

    def deploy_endpoint(
        self, endpoint_name: str, endpoint: Callable[[Any], Any], pip_dependencies: List[str] = []
    ) -> Endpoint:
        lambda_function_name = f"{AWSLambdaEndpointBackend.FUNCTION_NAME_PREFIX}{endpoint_name}"
        lambda_function_version = 1

        # Check for existing function
        try:
            old_function = self.lambda_client.get_function(FunctionName=lambda_function_name)
            lambda_function_version = int(old_function["Tags"][AWSLambdaEndpointBackend.ENDPOINT_VERSION_TAG]) + 1
        except self.lambda_client.exceptions.ResourceNotFoundException:
            pass

        # Build the zip file
        with tempfile.TemporaryDirectory() as td, tempfile.TemporaryDirectory() as zipfile_td:
            tmpdir = pathlib.Path(td)
            shutil.copy2(
                pathlib.Path(__file__).parent.parent / "static" / "aws-lambda-entrypoint.py",
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
                            AWSLambdaEndpointBackend.ENDPOINT_VERSION_TAG: str(lambda_function_version),
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
                            AWSLambdaEndpointBackend.ENDPOINT_VERSION_TAG: str(lambda_function_version),
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
