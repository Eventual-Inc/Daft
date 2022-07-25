from typing import Union

import pydantic

from daft.serving.aws_lambda import AWSLambdaBackendConfig
from daft.serving.docker import DockerBackendConfig


class BackendConfig(pydantic.BaseModel):
    name: str
    config: Union[DockerBackendConfig, AWSLambdaBackendConfig] = pydantic.Field(discriminator="type")
