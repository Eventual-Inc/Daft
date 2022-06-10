from __future__ import annotations

import logging
import re
from os import path
from typing import List, Optional, Type, TypeVar

import fsspec

from daft import config
from daft.datarepo.datarepo import DataRepo
from daft.datarepo.log import DaftLakeLog

logger = logging.getLogger(__name__)

Dataclass = TypeVar("Dataclass")


class DatarepoClient:
    def __init__(self, path: str):
        pathsplit = path.split("://")
        if len(pathsplit) != 2:
            raise ValueError(f"Expected path in format <protocol>://<path> but received: {path}")
        self._protocol, self._prefix = pathsplit
        self._fs: fsspec.AbstractFileSystem = fsspec.filesystem(self._protocol)

    def list_ids(self) -> List[str]:
        """List the IDs of all datarepos

        Returns:
            List[str]: IDs of datarepos
        """
        pattern_to_search = path.join(self._prefix, f"**/_log")
        dirs_with_log = self._fs.glob(pattern_to_search)
        id_pattern = f"{self._prefix}/(.*)/_log"
        lexer = re.compile(id_pattern)
        dirs_to_return = []
        for dir in dirs_with_log:
            result = lexer.search(dir)
            if result is not None:
                dirs_to_return.append(result.group(1))
        return dirs_to_return

    def get_path(self, name: str) -> str:
        return f"{self._protocol}://{self._prefix}/{name}"

    def from_id(self, repo_id: str) -> DataRepo:
        full_path = self.get_path(repo_id)
        exists = self._fs.exists(full_path)
        if not exists:
            raise ValueError(f"{repo_id} does not exist")
        daft_log = DaftLakeLog(full_path)
        return DataRepo(daft_log)

    def create(self, repo_id: str, dtype: Type, exists_ok=False) -> DataRepo:
        full_path = self.get_path(repo_id)
        exists = self._fs.exists(full_path)
        if exists:
            if exists_ok:
                data_repo = self.from_id(repo_id)
                new_schema = getattr(dtype, "_daft_schema")
                if data_repo.schema() != new_schema.arrow_schema():
                    logger.warning("New Schema and Data Repo Schema differs")
                    raise ValueError("New Schema does not match old")
                else:
                    return self.from_id(repo_id)
            else:
                raise ValueError(f"{repo_id} already exists")
        return DataRepo.create(full_path, repo_id, dtype)

    def delete(self, repo_id: str) -> None:
        self._fs.rmdir(self.get_path(repo_id))


def get_client(datarepo_path: Optional[str] = None) -> DatarepoClient:
    """Return the appropriate DatarepoClient as configured by the environment

    Returns:
        DatarepoClient: DatarepoClient to access Datarepos
    """
    if datarepo_path is None:
        daft_settings = config.DaftSettings()
        datarepo_path = f"s3://{daft_settings.DAFT_DATAREPOS_BUCKET}/{daft_settings.DAFT_DATAREPOS_PREFIX}"
    return DatarepoClient(datarepo_path)
