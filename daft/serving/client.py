from typing import Any, Callable, Dict, List

from daft.serving import backend


class ServingClient:
    def __init__(self):
        self.endpoint_backends: Dict[str, backend.AbstractEndpointBackend] = {
            "default": backend.DockerEndpointBackend(),
            "aws_lambda": backend.AWSLambdaEndpointBackend(),
        }

    def list_backends(self):
        return self.endpoint_backends

    def deploy(
        self,
        endpoint_name: str,
        endpoint: Callable[[Any], Any],
        backend: str = "default",
        pip_dependencies: List[str] = [],
    ):
        if backend not in self.endpoint_backends:
            raise ValueError(f"Unknown backend {backend}, expected one of: {list(self.endpoint_backends.keys())}")
        endpoint_backend = self.endpoint_backends[backend]
        return endpoint_backend.deploy_endpoint(endpoint_name, endpoint, pip_dependencies=pip_dependencies)

    def list_endpoints(self):
        for backend_name, endpoint_backend in self.endpoint_backends.items():
            print(f"Backend: {backend_name}")
            for endpoint in endpoint_backend.list_endpoints():
                print(f"  {endpoint}")
