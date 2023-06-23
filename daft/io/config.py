from __future__ import annotations

from daft.daft import PyS3Config


class S3Config:
    _config: PyS3Config

    def __init__(self) -> None:
        self._config = PyS3Config()

    def __repr__(self) -> str:
        return repr(self._config)
