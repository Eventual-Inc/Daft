from __future__ import annotations

import pathlib
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class DaftEnv:
    python_version: str = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    requirements_txt: Optional[str] = None
    pip_packages: List[str] = field(default_factory=list)
    # local_packages: List[str] = field(default_factory=list)
    # platform: Union[Literal["linux"], Literal["darwin"]] = sys.platform
    # arch: Union[Literal[32], Literal[64]] = 64 if sys.maxsize > 2**32 else 32

    def get_conda_environment(self) -> Dict[str, Any]:
        dependencies: List[Any] = [
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
