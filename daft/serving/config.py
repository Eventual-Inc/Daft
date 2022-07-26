from typing import Union

import pydantic

from daft.serving.backends.aws_lambda import AWSLambdaBackendConfig
from daft.serving.backends.docker import DockerBackendConfig
from daft.serving.backends.multiprocessing import MultiprocessingBackendConfig


class BackendConfig(pydantic.BaseModel):
    name: str
    config: Union[DockerBackendConfig, AWSLambdaBackendConfig, MultiprocessingBackendConfig] = pydantic.Field(
        discriminator="type"
    )
