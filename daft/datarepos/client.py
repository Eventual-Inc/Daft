from __future__ import annotations

import os
from typing import List, Protocol

from daft import config
from daft.datarepos import metadata_service


class DatarepoClient:

    def __init__(self, metadata_service: metadata_service._DatarepoMetadataService):
        self._metadata_service = metadata_service

    def list_ids(self) -> List[str]:
        """List the IDs of all datarepos

        Returns:
            List[str]: IDs of datarepos
        """
        return self._metadata_service.list_ids()

    def get_path(self, datarepo_id: str) -> str:
        """Returns the path to a Datarepo's underlying storage

        Returns:
            str: path to Datarepo's underlying storage
        """
        return self._metadata_service.get_path(datarepo_id)


def get_client() -> DatarepoClient:
    """Return the appropriate DatarepoClient as configured by the environment

    Returns:
        DatarepoClient: DatarepoClient to access Datarepos
    """
    daft_settings = config.DaftSettings()
    return DatarepoClient(
        metadata_service=metadata_service._S3DatarepoMetadataService(
            bucket=daft_settings.DAFT_DATAREPOS_BUCKET,
            prefix=daft_settings.DAFT_DATAREPOS_PREFIX,
        ),
    )
