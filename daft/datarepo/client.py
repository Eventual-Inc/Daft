from __future__ import annotations
import dataclasses
import logging


from typing import List, Optional
from os import path
import re
import fsspec
import pyarrow as pa
import ray
from ray.data.impl.arrow_block import ArrowRow

from typing import List, Optional, Type, TypeVar

from daft import config
from daft.dataset import Dataset


logger = logging.getLogger(__name__)

Dataclass = TypeVar("Dataclass")


class DatarepoClient:
    def __init__(self, path: str):
        pathsplit = path.split("://")
        if len(pathsplit) != 2:
            raise ValueError(f"Expected path in format <protocol>://<path> but received: {path}")
        self._protocol, self._prefix = pathsplit
        self._fs = fsspec.filesystem(self._protocol)

    def list_ids(self) -> List[str]:
        """List the IDs of all datarepos

        Returns:
            List[str]: IDs of datarepos
        """
        pattern_to_search = path.join(self._prefix, f'**/_log')
        dirs_with_log = self._fs.glob(pattern_to_search)
        id_pattern = f'{self._prefix}/(.*)/_log'
        lexer = re.compile(id_pattern)
        dirs_to_return = []
        for dir in dirs_with_log:
            result = lexer.search(dir)
            if result is not None:
                dirs_to_return.append(result.group(1))
        return dirs_to_return


def get_client(datarepo_path: Optional[str] = None) -> DatarepoClient:
    """Return the appropriate DatarepoClient as configured by the environment

    Returns:
        DatarepoClient: DatarepoClient to access Datarepos
    """
    if datarepo_path is None:
        daft_settings = config.DaftSettings()
        datarepo_path = f"s3://{daft_settings.DAFT_DATAREPOS_BUCKET}/{daft_settings.DAFT_DATAREPOS_PREFIX}"
    return DatarepoClient(datarepo_path)
