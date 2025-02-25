from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

import boto3

from daft.catalog import Catalog, Identifier, Table
from daft.daft import IOConfig, S3Config
from daft.io import read_iceberg

if TYPE_CHECKING:
    from daft.dataframe import DataFrame


class Path(Sequence):
    #
    _parts: tuple[str]

    def __init__(self, *parts: str):
        self._parts = tuple(parts)

    @staticmethod
    def from_ident(ident: Identifier | str) -> Path:
        path = Path.__new__(Path)
        if isinstance(ident, Identifier):
            path._parts = tuple(ident)
        elif isinstance(ident, str):
            path._parts = ident.split(".")
        else:
            raise ValueError("expected Identifier or str")
        return path

    @staticmethod
    def from_str(input: str) -> Path:
        return Path(*input.split("."))

    @property
    def parent(self) -> Path:
        return Path(*self._parts[:-1])

    @property
    def name(self) -> str:
        return self._parts[-1]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Path):
            return False
        return self._parts == other._parts

    def __getitem__(self, index: int | slice) -> str | Sequence[str]:
        return self._parts.__getitem__(index)

    def __len__(self) -> int:
        return self._parts.__len__()

    def __repr__(self) -> str:
        return f"Path('{self!s}')"

    def __str__(self) -> str:
        return ".".join(self)


class S3Catalog(Catalog):
    #
    _client: object
    _table_bucket_arn: str
    _io_config: IOConfig

    def __init__(self):
        raise ValueError("Not supported!")

    ###
    # from_*
    ###

    @staticmethod
    def from_arn(table_bucket_arn: str, s3_config: S3Config | None = None) -> S3Catalog:
        """Creates an S3Catalog from the table bucket ARN."""
        region = table_bucket_arn.split(":")[3]  # ARN format: arn:aws:s3tables:region:account:bucket/name
        c = S3Catalog.__new__(S3Catalog)
        c._client = boto3.client("s3tables", region_name=region)
        c._table_bucket_arn = table_bucket_arn
        c._io_config = IOConfig(s3=s3_config)
        return c

    @staticmethod
    def from_options(table_bucket_arn: str, **options) -> S3Catalog:
        """Creates an S3Catalog using the default credential provider region and account."""
        c = S3Catalog.__new__(S3Catalog)
        c._table_bucket_arn = table_bucket_arn
        c._client = boto3.client("s3tables", **options)
        return c

    @staticmethod
    def from_client(table_bucket_arn: str, client: object) -> S3Catalog:
        """Creates an S3Catalog using the given boto3 client."""
        c = S3Catalog.__new__(S3Catalog)
        c._table_bucket_arn = table_bucket_arn
        c._client = client
        return c

    @staticmethod
    def from_session(table_bucket_arn: str, session: object) -> S3Catalog:
        """Creates an S3Catalog using the boto3 session."""
        c = S3Catalog.__new__(S3Catalog)
        c._table_bucket_arn = table_bucket_arn
        c._client = session.create_client("s3tables")
        return c

    ###
    # get_*
    ###

    def get_table(self, identifier: Identifier | str) -> S3Table:
        path = Path.from_ident(identifier)
        res = self._client.get_table(
            name=path.name,
            namespace=str(path.parent),
            tableBucketARN=self._table_bucket_arn,
        )
        return S3Table(self, path, res["metadataLocation"])

    ###
    # list_*
    ###

    def list_tables(self, pattern: str | None = None) -> list[str]:
        raise NotImplementedError("list_tables")

    ###
    # private methods
    ###

    def _read_iceberg(self, table: S3Table) -> DataFrame:
        return read_iceberg(table=table.metadata_location, io_config=self._io_config)


class S3Table(Table):
    #
    _catalog: S3Catalog
    _path: Path
    #
    metadata_location: str

    def __init__(
        self,
        catalog: S3Catalog,
        path: Path,
        metadata_location: str,
    ):
        self._catalog = catalog
        self._path = path
        self.metadata_location = metadata_location

    @property
    def path(self) -> Path:
        return self._path

    @property
    def name(self) -> str:
        return self._path.name

    @property
    def namespace(self) -> Path:
        return self._path.parent

    def read(self) -> DataFrame:
        return self._catalog._read_iceberg(self)
