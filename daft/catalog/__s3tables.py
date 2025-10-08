"""WARNING! THIS PACKAGE IS INTERNAL AND IS SUBJECT TO CHANGE."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, overload

import boto3
import botocore
from botocore.exceptions import ClientError

from daft.catalog import Catalog, Identifier, NotFoundError, Properties, Schema, Table
from daft.catalog.__iceberg import IcebergCatalog
from daft.io import read_iceberg

if TYPE_CHECKING:
    from boto3 import Session
    from mypy_boto3_s3tables import S3TablesClient
    from mypy_boto3_s3tables.type_defs import SchemaFieldTypeDef, TableMetadataTypeDef, TableSummaryTypeDef

    from daft.daft import IOConfig
    from daft.dataframe import DataFrame
    from daft.io.partitioning import PartitionField

else:
    S3TablesClient = object
    TableMetadataTypeDef = dict
    SchemaFieldTypeDef = dict
    TableSummaryTypeDef = dict


class S3Path(Sequence[str]):
    _parts: tuple[str, ...]

    def __init__(self, *parts: str):
        self._parts = tuple(parts)

    @staticmethod
    def from_ident(ident: Identifier) -> S3Path:
        path = S3Path.__new__(S3Path)
        path._parts = tuple(ident)
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

    @overload
    def __getitem__(self, index: int, /) -> str: ...

    @overload
    def __getitem__(self, index: slice, /) -> Sequence[str]: ...

    def __getitem__(self, index: int | slice, /) -> str | Sequence[str]:
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

    def __init__(self) -> None:
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
        if isinstance(session, boto3.Session):
            c._client = session.client("s3tables")
        elif isinstance(session, botocore.session.Session):
            c._client = session.create_client("s3tables")
        else:
            raise TypeError(f"Expected boto3.Session or botocore.session.Session, got {type(session).__name__}")
        return c

    ###
    # create_*
    ###

    def _create_namespace(self, identifier: Identifier) -> None:
        try:
            path = S3Path.from_ident(identifier)
            self._client.create_namespace(
                tableBucketARN=self._table_bucket_arn,
                namespace=path._parts,
            )
        except Exception as e:
            raise ValueError(f"Failed to create namespace: {e}") from e

    def _create_table(
        self,
        ident: Identifier,
        schema: Schema,
        properties: Properties | None = None,
        partition_fields: list[PartitionField] | None = None,
    ) -> Table:
        path = S3Path.from_ident(ident)
        if len(path) < 2:
            raise ValueError(f"Table identifier is missing a namespace, {path!s}")

        try:
            _ = self._client.create_table(
                tableBucketARN=self._table_bucket_arn,
                namespace=str(path.parent),
                name=path.name,
                format="ICEBERG",  # <-- only supported value
                metadata=_to_metadata(schema),
            )
            return self.get_table(ident)
        except Exception as e:
            raise ValueError(f"Failed to create table: {e}") from e

    ###
    # has_*
    ###

    def _has_namespace(self, identifier: Identifier) -> bool:
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

    def _has_table(self, identifier: Identifier) -> bool:
        try:
            self._get_table(identifier)
            return True
        except Exception:
            return False

    ###
    # drop_*
    ###

    def _drop_namespace(self, identifier: Identifier) -> None:
        path = S3Path.from_ident(identifier)
        try:
            self._client.delete_namespace(
                tableBucketARN=self._table_bucket_arn,
                namespace=str(path),
            )
        except Exception as e:
            raise ValueError(f"Failed to drop namespace: {e}") from e

    def _drop_table(self, identifier: Identifier) -> None:
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

    def _get_table(self, identifier: Identifier) -> S3Table:
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

    def _list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
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

    def _list_tables(self, pattern: str | None = None) -> list[Identifier]:
        # base request
        req = {
            "tableBucketARN": self._table_bucket_arn,
            "maxTables": 1000,
        }

        def to_ident(table_summary: TableSummaryTypeDef) -> Identifier:
            return Identifier(*table_summary["namespace"], table_summary["name"])

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
        if table.metadata_location is None:
            raise ValueError("Cannot read S3Table without a metadata_location")
        return read_iceberg(table=table.metadata_location, io_config=self._io_config)


class S3Table(Table):
    #
    _catalog: S3Catalog
    _path: S3Path
    #
    metadata_location: str | None

    def __init__(
        self,
        catalog: S3Catalog,
        path: S3Path,
        metadata_location: str | None,
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

    def schema(self) -> Schema:
        return self.read().schema()

    @property
    def namespace(self) -> S3Path:
        return self._path.parent

    def read(self, **options: Any) -> DataFrame:
        return self._catalog._read_iceberg(self)

    def append(self, df: DataFrame, **options: Any) -> None:
        raise ValueError("S3 Table writes require using Iceberg REST.")

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        raise ValueError("S3 Table writes require using Iceberg REST.")


###
# PRIVATE HELPERS
###


def _to_metadata(schema: Schema) -> TableMetadataTypeDef:
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


def _to_field(field) -> SchemaFieldTypeDef:  # type: ignore
    return {
        "name": field.name,
        "type": str(field.field_type),
    }
