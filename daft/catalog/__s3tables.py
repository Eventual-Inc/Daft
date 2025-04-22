"""WARNING! THIS PACKAGE IS INTERNAL AND IS SUBJECT TO CHANGE."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

from daft.catalog import Catalog, Identifier, NotFoundError, Properties, Schema, Table
from daft.catalog.__iceberg import IcebergCatalog
from daft.dataframe import DataFrame
from daft.io import read_iceberg

if TYPE_CHECKING:
    from boto3 import Session
    from mypy_boto3_s3tables import S3TablesClient

    from daft.daft import IOConfig
    from daft.dependencies import pa
else:
    S3TablesClient = object


class S3Path(Sequence):
    _parts: tuple[str, ...]

    def __init__(self, *parts: str):
        self._parts = tuple(parts)

    @staticmethod
    def from_ident(ident: Identifier | str) -> S3Path:
        path = S3Path.__new__(S3Path)
        if isinstance(ident, Identifier):
            path._parts = tuple(ident)
        elif isinstance(ident, str):
            path._parts = tuple(ident.split("."))
        else:
            raise ValueError("expected Identifier or str")
        return path

    @staticmethod
    def from_str(input: str) -> S3Path:
        return S3Path(*input.split("."))

    @property
    def parent(self) -> S3Path:
        return S3Path(*self._parts[:-1])

    @property
    def name(self) -> str:
        return self._parts[-1]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, S3Path):
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
    _client: S3TablesClient
    _table_bucket_arn: str
    _io_config: IOConfig

    def __init__(self):
        raise ValueError("Not supported!")

    @property
    def name(self) -> str:
        return self._table_bucket_arn.split("/")[-1]

    ###
    # from_*
    ###

    @staticmethod
    def from_arn(table_bucket_arn: str) -> IcebergCatalog:
        """Creates a Catalog from the S3 table bucket ARN using the Iceberg REST endpoint."""
        # arn:aws:s3tables:region:account:bucket/name
        arn_parts = table_bucket_arn.split(":")
        region = arn_parts[3]
        bucket = arn_parts[5][7:]
        return IcebergCatalog._load_catalog(
            bucket,
            **{
                "type": "rest",
                "warehouse": table_bucket_arn,
                "uri": f"https://s3tables.{region}.amazonaws.com/iceberg",
                "rest.sigv4-enabled": "true",
                "rest.signing-name": "s3tables",
                "rest.signing-region": region,
            },
        )

    @staticmethod
    def from_client(table_bucket_arn: str, client: S3TablesClient) -> S3Catalog:
        """Creates an S3Catalog using the given boto3 client."""
        c = S3Catalog.__new__(S3Catalog)
        c._table_bucket_arn = table_bucket_arn
        c._client = client
        return c

    @staticmethod
    def from_session(table_bucket_arn: str, session: Session) -> S3Catalog:
        """Creates an S3Catalog using the boto3 session."""
        c = S3Catalog.__new__(S3Catalog)
        c._table_bucket_arn = table_bucket_arn
        c._client = session.create_client("s3tables")
        return c

    ###
    # create_*
    ###

    def create_namespace(self, identifier: Identifier | str):
        """Creates a namespace in the S3 Tables catalog."""
        try:
            path = S3Path.from_ident(identifier)
            self._client.create_namespace(
                tableBucketARN=self._table_bucket_arn,
                namespace=path._parts,
            )
        except Exception as e:
            raise ValueError(f"Failed to create namespace: {e}") from e

    def create_table(
        self,
        identifier: Identifier | str,
        source: Schema | DataFrame,
        properties: Properties | None = None,
    ) -> Table:
        if isinstance(source, Schema):
            return self._create_table_from_schema(identifier, source)
        elif isinstance(source, DataFrame):
            raise ValueError("S3 Tables create table from DataFrame not yet supported.")
        else:
            raise Exception(f"Unknown table source: {source}")

    def _create_table_from_schema(self, ident: Identifier | str, source: Schema) -> Table:
        path = S3Path.from_ident(ident)
        if len(path) < 2:
            raise ValueError(f"Table identifier is missing a namespace, {path!s}")

        try:
            _ = self._client.create_table(
                tableBucketARN=self._table_bucket_arn,
                namespace=str(path.parent),
                name=path.name,
                format="ICEBERG",  # <-- only supported value
                metadata=_to_metadata(source),
            )
            return self.get_table(ident)
        except Exception as e:
            raise ValueError(f"Failed to create table: {e}") from e

    ###
    # has_*
    ###

    def has_namespace(self, identifier: Identifier | str):
        try:
            _ = self._client.get_namespace(
                namespace=str(identifier),
                tableBucketARN=self._table_bucket_arn,
            )
            return True
        except ClientError as ex:
            if ex.response["Error"]["Code"] == "NotFoundException":
                return False
            else:
                raise ex

    ###
    # drop_*
    ###

    def drop_namespace(self, identifier: Identifier | str):
        """Drops a namespace from the S3 Tables catalog."""
        path = S3Path.from_ident(identifier)
        try:
            self._client.delete_namespace(
                tableBucketARN=self._table_bucket_arn,
                namespace=str(path),
            )
        except Exception as e:
            raise ValueError(f"Failed to drop namespace: {e}") from e

    def drop_table(self, identifier: Identifier | str):
        """Drops a table from the S3 Tables catalog."""
        path = S3Path.from_ident(identifier)
        try:
            self._client.delete_table(
                tableBucketARN=self._table_bucket_arn,
                namespace=str(path.parent),
                name=path.name,
            )
        except Exception as e:
            raise ValueError(f"Failed to drop table: {e}") from e

    ###
    # get_*
    ###

    def get_table(self, identifier: Identifier | str) -> S3Table:
        path = S3Path.from_ident(identifier)
        try:
            res = self._client.get_table(
                name=path.name,
                namespace=str(path.parent),
                tableBucketARN=self._table_bucket_arn,
            )
            return S3Table(self, path, res.get("metadataLocation"))
        except ClientError as ex:
            if ex.response["Error"]["Code"] == "NotFoundException":
                raise NotFoundError(f"Table {identifier} not found")
            else:
                raise ex

    ###
    # list_*
    ###

    def list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        """Lists namespaces in the S3 Tables catalog."""
        # base request
        req = {
            "tableBucketARN": self._table_bucket_arn,
            "maxNamespaces": 1000,
        }

        # pattern here just represents the namespace prefix
        if pattern:
            req["prefix"] = pattern

        try:
            namespaces = []
            while True:
                res = self._client.list_namespaces(**req)
                for namespace in res["namespaces"]:
                    namespaces.append(Identifier(*namespace["namespace"]))
                if cont_token := res.get("continuationToken"):
                    req["continuationToken"] = cont_token
                else:
                    break
            return namespaces
        except Exception as e:
            raise ValueError(f"Failed to list namespaces: {e}")

    def list_tables(self, pattern: str | None = None) -> list[str]:
        """Lists tables in the S3 Tables catalog."""
        # base request
        req = {
            "tableBucketARN": self._table_bucket_arn,
            "maxTables": 1000,
        }

        # need to return qualified names, so stitch the parts (str for now).
        def to_ident(table_summary) -> str:
            parts = []
            parts.extend(table_summary["namespace"])
            parts.append(table_summary["name"])
            return ".".join(parts)

        # we must split the pattern and use the last part as the table preefix.
        if pattern:
            parts = pattern.split(".")
            if len(parts) == 1:
                req["namespace"] = parts[0]
            else:
                req["namespace"] = ".".join(parts[:-1])
                req["prefix"] = parts[-1]

        # loop each page
        try:
            tables = []
            while True:
                res = self._client.list_tables(**req)
                for table in res["tables"]:
                    tables.append(to_ident(table))
                if cont_token := res.get("continuationToken"):
                    req["continuationToken"] = cont_token
                else:
                    break
            return tables
        except Exception as e:
            raise ValueError(f"Failed to list tables: {e}") from e

    ###
    # private methods
    ###

    def _read_iceberg(self, table: S3Table) -> DataFrame:
        return read_iceberg(table=table.metadata_location, io_config=self._io_config)


class S3Table(Table):
    #
    _catalog: S3Catalog
    _path: S3Path
    #
    metadata_location: str

    def __init__(
        self,
        catalog: S3Catalog,
        path: S3Path,
        metadata_location: str,
    ):
        self._catalog = catalog
        self._path = path
        self.metadata_location = metadata_location

    @property
    def path(self) -> S3Path:
        return self._path

    @property
    def name(self) -> str:
        return self._path.name

    @property
    def namespace(self) -> S3Path:
        return self._path.parent

    def read(self, **options) -> DataFrame:
        return self._catalog._read_iceberg(self)

    def write(self):
        raise ValueError("S3 Table writes require using Iceberg REST.")


###
# PRIVATE HELPERS
###


def _to_metadata(schema: Schema) -> dict:
    from pyiceberg.io.pyarrow import _ConvertToIcebergWithoutIDs, visit_pyarrow

    # We must stringify the iceberg schema.
    # Conversion: daft schema -> pyarrow schema -> iceberg schema
    pa_schema = schema.to_pyarrow_schema()
    ic_schema = visit_pyarrow(pa_schema, _ConvertToIcebergWithoutIDs())

    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3TableBuckets_TableMetadata.html
    return {
        "iceberg": {
            "schema": {
                "fields": [_to_field(f) for f in ic_schema.fields],
            }
        }
    }


def _to_field(field: pa.Field) -> dict:
    return {
        "name": field.name,
        "type": str(field.field_type),
    }
