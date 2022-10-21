from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
from dataclasses import dataclass, field
from typing import Any

import docker


def get_docker_client() -> docker.Client:
    client = docker.from_env()
    try:
        client.info()
    except docker.errors.APIError as e:
        raise RuntimeError("Unable to run Docker, please make sure Docker is installed and running.")
    return client


def get_conda_executable() -> pathlib.Path:
    conda_info_proc = subprocess.run(["conda", "info", "--json"], capture_output=True)
    if conda_info_proc.returncode != 0:
        raise RuntimeError(
            "Unable to find Conda installation, please ensure that Conda is installed and available on your $PATH"
        )
    conda_info = json.loads(conda_info_proc.stdout)

    expected_conda_executable_path = pathlib.Path(conda_info["root_prefix"]) / "condabin" / "conda"
    if not expected_conda_executable_path.is_file():
        raise RuntimeError(f"Unable to find Conda executable at: {expected_conda_executable_path}")
    if not os.access(str(expected_conda_executable_path), os.X_OK):
        raise RuntimeError(f"Conda executable at {expected_conda_executable_path} is not executable")
    return expected_conda_executable_path


@dataclass(frozen=True)
class DaftEnv:
    python_version: str = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    requirements_txt: str | None = None
    pip_packages: list[str] = field(default_factory=list)
    # local_packages: List[str] = field(default_factory=list)
    # platform: Union[Literal["linux"], Literal["darwin"]] = sys.platform
    # arch: Union[Literal[32], Literal[64]] = 64 if sys.maxsize > 2**32 else 32

    def get_conda_environment(self) -> dict[str, Any]:
        dependencies: list[Any] = [
            f"python=={self.python_version}",
        ]

        # Construct pip dependencies
        pip = []
        if self.requirements_txt is not None:
            split = pathlib.Path(self.requirements_txt).read_text().splitlines()
            pip.extend(split)
        if self.pip_packages:
            pip.extend(self.pip_packages)
        # if self.local_packages:
        #     pip.extend(self.local_packages)
        if pip:
            dependencies.append("pip")
            dependencies.append({"pip": pip})

        conda_environment = {
            "name": "daft_env",
            "channels": ["conda-forge", "defaults"],
            "dependencies": dependencies,
        }

        return conda_environment
