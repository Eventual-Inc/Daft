"""WARNING! THIS PACKAGE IS INTERNAL AND IS SUBJECT TO CHANGE."""

from __future__ import annotations

import boto3

from botocore.exceptions import ClientError

from typing import TYPE_CHECKING, Literal, Optional
from enum import Enum

from daft.catalog import Catalog, Identifier, NotFoundError, Table, TableSource
from daft.datatype import DataType
from daft.dataframe import DataFrame
from daft.logical.schema import Schema, Field

if TYPE_CHECKING:
    from daft.daft import IOConfig


class GlueCatalog(Catalog):
    """The GlueCatalog maps to an AWS Glue Database using a boto3 client."""

    _client: object
    _database_name: str
    _io_config: IOConfig

    def __init__(self):
        raise ValueError("GlueCatalog.__init__() not supported!")

    @staticmethod
    def from_database(database_name: str, **options) -> GlueCatalog:
        """Creates a GlueCatalog from the database name and optional catalog id.

        Parameters:
            - database_name (str): The name of the database to retrieve. For Hive compatibility, this should be all lowercase.
            - catlog_id (Optional[str]): The ID of the Data Catalog in which the database resides. If none is provided, the AWS account ID is used by default.
            - **options: The boto3 client options (like 'region_name')

        Returns:
            GlueCatalog: Catalog instance backed by an AWS Glue Database.
        """
        c = GlueCatalog.__new__(GlueCatalog)
        c._database_name = database_name
        c._client = boto3.client('glue', **options)
        return c

    @staticmethod
    def from_client(database_name: str, client: object) -> GlueCatalog:
        """Creates a GlueCatalog using the given boto3 client."""
        c = GlueCatalog.__new__(GlueCatalog)
        c._database_name = database_name
        c._client = client
        return c

    @staticmethod
    def from_session(database_name: str, session: object) -> GlueCatalog:
        """Creates a GlueCatalog using the boto3 session."""
        c = GlueCatalog.__new__(GlueCatalog)
        c._database_name = database_name
        c._client = session.create_client("glue")
        return c

    @property
    def name(self) -> str:
        return self._database_name

    def create_namespace(self, identifier: Identifier | str):
        raise NotImplementedError

    def create_table(self, identifier: Identifier | str, source: TableSource) -> Table:
        raise NotImplementedError

    def drop_namespace(self, identifier: Identifier | str):
        raise NotImplementedError

    def drop_table(self, identifier: Identifier | str):
        raise NotImplementedError

    def get_table(self, identifier: Identifier | str) -> Table:
        req = {
            "DatabaseName": self._database_name,
            "Name": str(identifier),
        }
        try:
            descriptor = self._client.get_table(**req)["Table"]
        except ClientError as ex:
            if ex.response["Error"]["Code"] == "EntityNotFoundException":
                raise NotFoundError(f"Table {identifier} not found")
            else:
                raise ex
        return GlueTable._from_descriptor(self, descriptor)

    def list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        raise NotImplementedError

    def list_tables(self, pattern: str | None = None) -> list[str]:
        req = { 
            "DatabaseName": self._database_name,
        }
        try:
            tables = []
            while True:
                res = self._client.get_tables(**req)
                for table in res["TableList"]:
                    tables.append(table["Name"])
                if next_token := res.get("NextToken"):
                    req["NextToken"] = next_token
                else:
                    break
            return tables
        except ClientError as e:
            raise e # just re-throw for now


class GlueTable(Table):
    _descriptor: object
    _catalog: GlueCatalog
    _name: str
    _storage: GlueStorageDescriptor

    def __init__(self):
        raise ValueError("GlueTable.__init__() not supported!")

    @staticmethod
    def _from_descriptor(catalog: GlueCatalog, descriptor: object) -> GlueTable:
        t = GlueTable.__new__(GlueTable)
        t._descriptor = descriptor
        t._catalog = catalog
        t._name = descriptor["Name"]
        t._storage = GlueStorageDescriptor._from_descriptor(descriptor["StorageDescriptor"])
        return t

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def schema(self) -> Schema:
        return self._storage.schema

    def read(self, **options) -> DataFrame:
        # VERY BASIC...
        impl = self._storage._format
        if impl == GlueFormat.PARQUET:
            return self._read_parquet(**options)
        if impl == GlueFormat.CSV:
            return self._read_csv(**options)
        raise ValueError(f"Unsupport GlueFormat {impl}")

    def _read_parquet(self, **options) -> DataFrame:
        from daft.io import read_parquet

        return read_parquet(
            path=self._storage._location,
            infer_schema=False,
            schema={ f.name : f.dtype for f in self._storage.schema },
        )

    def _read_csv(self, **options) -> DataFrame:
        from daft.io import read_csv

        return read_csv(
            path=self._storage._location,   
            infer_schema=False,
            schema={ f.name : f.dtype for f in self._storage.schema },
            delimiter=options.get("delimiter", ",")
        )

    def write(self, df: DataFrame, mode: Literal['append'] | Literal['overwrite'] = "append", **options) -> None:
        raise NotImplementedError

    def __repr__(self) -> str:
        import json 

        return json.dumps(self._descriptor, indent=2, default=str)


class GlueStorageDescriptor:
    """See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-StorageDescriptor"""
    _descriptor: object
    _schema: Schema
    _format: GlueFormat
    _location: str

    def __init__(self):
        raise ValueError("GlueStorageDescriptor.__init__() not supported!")

    @staticmethod
    def _from_descriptor(descriptor: object) -> GlueStorageDescriptor:
        d = GlueStorageDescriptor.__new__(GlueStorageDescriptor)
        d._descriptor = descriptor
        d._schema = d._to_schema(descriptor["Columns"])
        d._format = GlueFormat._from_descriptor(descriptor)
        d._location = descriptor["Location"]
        return d

    @property
    def schema(self) -> Schema:
        return self._schema

    def __repr__(self) -> str:
        import json 

        return json.dumps(self._descriptor, indent=2, default=str)

    @staticmethod
    def _to_schema(columns: list[object]) -> Schema:
        return Schema._from_fields([GlueStorageDescriptor._to_field(column) for column in columns])

    @staticmethod
    def _to_field(column: object) -> Field:
        return Field.create(column["Name"], GlueStorageDescriptor._to_type(column["Type"]))

    @staticmethod
    def _to_type(type: str) -> DataType:
        type = type.lower()
        if type == "boolean":
            return DataType.bool()
        elif type == "byte":
            return DataType.int8()
        elif type == "short":
            return DataType.int16()
        elif type == "integer":
            return DataType.int32()
        elif type == "long" or type == "bigint":
            return DataType.int64()
        elif type == "float":
            return DataType.float32()
        elif type == "double":
            return DataType.float64()
        elif type == "decimal":
            return DataType.decimal128(precision=38, scale=18)
        elif type == "string":
            return DataType.string()
        elif type == "timestamp":
            return DataType.timestamp(timeunit="us", timezone="UTC")
        elif type == "date":
            return DataType.date()
        else:
            raise ValueError(f"Unsupported Glue type: {type}")


class GlueFormat(Enum):
    """Enum representing supported Glue table formats."""
    CSV = "csv"
    PARQUET = "parquet"


    @staticmethod
    def _from_descriptor(descriptor: object) -> GlueFormat:
        if (parameters := descriptor.get("Parameters")) is None:
            raise ValueError("StorageDescriptor missing Parameters")
        if (classification := parameters.get("classification")) is None:
            raise ValueError("StorageDescriptor Parameters missing classification.")
        fmt = classification.lower()
        if fmt == "csv":
            return GlueFormat.CSV
        if fmt == "parquet":
            return GlueFormat.PARQUET
        raise ValueError(f"Unsupported Glue format: {fmt}")
