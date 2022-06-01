from __future__ import annotations

from typing import List, Optional

import fsspec
from daft import config


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
        return [path.replace(self._prefix, "").lstrip("/") for path in self._fs.ls(self._prefix)]

    def get_path(self, datarepo_id: str) -> str:
        """Returns the path to a Datarepo's underlying storage

        Returns:
            str: path to Datarepo's underlying storage
        """
        return f"{self._protocol}://{self._prefix}/{datarepo_id}"

    def get_parquet_filepaths(self, datarepo_id: str) -> List[str]:
        """Returns a sorted list of filepaths to the Parquet files that make up this datarepo

        Args:
            datarepo_id (str): ID of the datarepo

        Returns:
            List[str]: list of paths to Parquet files
        """
        return sorted(
            [
                f"{self._protocol}://{path}"
                for path in self._fs.ls(f"{self._prefix}/{datarepo_id}")
                if path.endswith(".parquet")
            ]
        )


def get_client(datarepo_path: Optional[str] = None) -> DatarepoClient:
    """Return the appropriate DatarepoClient as configured by the environment

    Returns:
        DatarepoClient: DatarepoClient to access Datarepos
    """
    if datarepo_path is None:
        daft_settings = config.DaftSettings()
        datarepo_path = f"s3://{daft_settings.DAFT_DATAREPOS_BUCKET}/{daft_settings.DAFT_DATAREPOS_PREFIX}"
    return DatarepoClient(datarepo_path)
