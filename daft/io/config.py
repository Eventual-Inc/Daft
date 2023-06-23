from __future__ import annotations

from daft.daft import PyIoConfig


class IoConfig:
    _config: PyIoConfig

    def __init__(self) -> None:
        self._config = PyIoConfig()

    def __repr__(self) -> str:
        return repr(self._config)
