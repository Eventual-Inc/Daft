from typing import Any, Callable, Dict, List, Type

from daft.serving import backend, config
from daft.serving.backends.aws_lambda import AWSLambdaEndpointBackend
from daft.serving.backends.docker import DockerEndpointBackend
from daft.serving.backends.multiprocessing import MultiprocessingEndpointBackend
from daft.serving.definitions import Endpoint

CONFIG_BACKEND_CLASSES: List[Type[backend.AbstractEndpointBackend]] = [
    DockerEndpointBackend,
    AWSLambdaEndpointBackend,
    MultiprocessingEndpointBackend,
]
CONFIG_TYPE_ID_TO_BACKEND_CLASS_MAP = {
    backend_class.config_type_id(): backend_class for backend_class in CONFIG_BACKEND_CLASSES
}


class ServingClient:
    def __init__(self, endpoint_backends: Dict[str, backend.AbstractEndpointBackend]):
        self.endpoint_backends = endpoint_backends

    @classmethod
    def from_configs(cls, configs: List[Dict[str, Any]]):
        parsed_configs = [config.BackendConfig.parse_obj(c) for c in configs]
        instantiated_backends = {
            c.name: CONFIG_TYPE_ID_TO_BACKEND_CLASS_MAP[c.config.type].from_config(c.config) for c in parsed_configs
        }
        return cls(instantiated_backends)

    def list_backends(self):
        return self.endpoint_backends

    def deploy(
        self,
        endpoint_name: str,
        endpoint: Callable[[Any], Any],
        backend: str = "default",
        pip_dependencies: List[str] = [],
    ) -> Endpoint:
        if backend not in self.endpoint_backends:
            raise ValueError(f"Unknown backend {backend}, expected one of: {list(self.endpoint_backends.keys())}")
        endpoint_backend = self.endpoint_backends[backend]
        return endpoint_backend.deploy_endpoint(endpoint_name, endpoint, pip_dependencies=pip_dependencies)

    def list_endpoints(self):
        for backend_name, endpoint_backend in self.endpoint_backends.items():
            print(f"Backend: {backend_name}")
            for endpoint in endpoint_backend.list_endpoints():
                print(f"  {endpoint}")
