from __future__ import annotations

import os
from typing import List, Protocol

from daft.datarepo import config


def get_metadata_service() -> _DatarepoMetadataService:
    """Return the appropriate _DatarepoMetadataService as configured by the environment

    Returns:
        _DatarepoMetadataService: _DatarepoMetadataService to access Datarepo metadata
    """
    return _S3DatarepoMetadataService(
        bucket=config.Settings.DATAREPOS_BUCKET,
        prefix=config.Settings.DATAREPOS_PREFIX,
    )


class _DatarepoMetadataService(Protocol):
    """The DatarepoMetadataService provides access to metadata about Datarepos, such as
    IDs, schemas and paths for initializing Datarepos.
    """

    def list_ids(self) -> List[str]:
        """List the IDs of all available Datarepos

        Returns:
            List[str]: IDs of available Datarepos
        """
        ...

    def get_path(self, datarepo_id: str) -> str:
        """Returns the path to a Datarepo's underlying storage

        Returns:
            str: path to Datarepo's underlying storage
        """
        ...


class _S3DatarepoMetadataService(_DatarepoMetadataService):
    """Implementation of DatarepoMetadataService using S3 as the backing store"""

    def __init__(self, bucket: str, prefix: str) -> None:
        self._bucket = bucket.rstrip("/")
        self._prefix = prefix.rstrip("/") + "/"

    def _get_s3_client(self):
        import boto3

        return boto3.client("s3")

    def list_ids(self) -> List[str]:
        s3 = self._get_s3_client()
        response = s3.list_objects_v2(Bucket=self._bucket, Prefix=self._prefix, Delimiter="/")
        return [
            content.get("Prefix").rstrip("/").replace(self._prefix, "")
            for content in response.get("CommonPrefixes", [])
        ]

    def get_path(self, datarepo_id: str) -> str:
        """Returns the path to a Datarepo's underlying storage

        Returns:
            str: path to Datarepo's underlying storage
        """
        return f"s3://{self._bucket}/{self._prefix.rstrip('/')}/{datarepo_id}"


class _LocalDatarepoMetadataService(_DatarepoMetadataService):
    """Implementation of DatarepoMetadataService using a local tmpdir as the backing store"""

    def __init__(self, tmpdir: str) -> None:
        self._tmpdir = tmpdir

    def list_ids(self) -> List[str]:
        return os.listdir(self._tmpdir)

    def get_path(self, datarepo_id: str) -> str:
        """Returns the path to a Datarepo's underlying storage

        Returns:
            str: path to Datarepo's underlying storage
        """
        return os.path.join(self._tmpdir, datarepo_id)
