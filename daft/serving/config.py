from typing import Union

import pydantic

from daft.serving.backends.aws_lambda import AWSLambdaBackendConfig
from daft.serving.backends.docker import DockerBackendConfig


class BackendConfig(pydantic.BaseModel):
    name: str
    config: Union[DockerBackendConfig, AWSLambdaBackendConfig] = pydantic.Field(discriminator="type")
