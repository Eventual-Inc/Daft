from __future__ import annotations

import logging
from typing import List, Type, TypeVar

from icebridge.client import IcebergCatalog, IceBridgeClient

from daft.experimental.datarepo.datarepo import DataRepo

logger = logging.getLogger(__name__)

Dataclass = TypeVar("Dataclass")


class DatarepoClient:
    def __init__(self, path: str):
        pathsplit = path.split("://")
        if len(pathsplit) != 2:
            raise ValueError(f"Expected path in format <protocol>://<path> but received: {path}")
        self._client = IceBridgeClient()
        self._iceberg_catalog = IcebergCatalog.from_hadoop_catalog(self._client, path)

    def list_ids(self) -> List[str]:
        """List the IDs of all datarepos

        Returns:
            List[str]: IDs of datarepos
        """
        return self._iceberg_catalog.list_tables()  # type: ignore

    def from_id(self, repo_id: str) -> DataRepo:
        table = self._iceberg_catalog.load_table(repo_id)
        return DataRepo(table=table)

    def create(self, repo_id: str, dtype: Type, exists_ok=False) -> DataRepo:
        return DataRepo.create(self._iceberg_catalog, repo_id, dtype)

    def delete(self, repo_id: str) -> bool:
        return self._iceberg_catalog.drop_table(repo_id, True)  # type: ignore


def get_client(datarepo_path: str) -> DatarepoClient:
    """Return the appropriate DatarepoClient as configured by the environment

    Returns:
        DatarepoClient: DatarepoClient to access Datarepos
    """
    return DatarepoClient(datarepo_path)
