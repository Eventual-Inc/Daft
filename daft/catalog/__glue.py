"""WARNING! THIS PACKAGE IS INTERNAL AND IS SUBJECT TO CHANGE."""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, Literal

import boto3
from botocore.exceptions import ClientError

from daft.catalog import Catalog, Identifier, NotFoundError, Table, TableSource
from daft.datatype import DataType
from daft.logical.schema import Field, Schema

if TYPE_CHECKING:
    from boto3 import Session
    from mypy_boto3_glue import GlueClient
    from mypy_boto3_glue.type_defs import ColumnOutputTypeDef as GlueColumnInfo
    from mypy_boto3_glue.type_defs import TableTypeDef as GlueTableInfo

    from daft.daft import IOConfig
    from daft.dataframe import DataFrame

    from daft.io.source import GlobSourcePushdowns, GlobSource

else:
    GlueClient = object


Properties = dict[str, Any]


class GlueCatalog(Catalog):
    """The GlueCatalog maps to an AWS Glue Database using a boto3 client."""

    _client: GlueClient
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
        c._client = boto3.client("glue", **options)
        return c

    @staticmethod
    def from_client(database_name: str, client: GlueClient) -> GlueCatalog:
        """Creates a GlueCatalog using the given boto3 client."""
        c = GlueCatalog.__new__(GlueCatalog)
        c._database_name = database_name
        c._client = client
        return c

    @staticmethod
    def from_session(database_name: str, session: Session) -> GlueCatalog:
        """Creates a GlueCatalog using the boto3 session."""
        c = GlueCatalog.__new__(GlueCatalog)
        c._database_name = database_name
        c._client = session.client("glue")
        return c

    @property
    def name(self) -> str:
        return self._database_name

    def create_namespace(self, identifier: Identifier | str):
        raise NotImplementedError()

    def create_table(self, identifier: Identifier | str, source: TableSource | object) -> Table:
        raise NotImplementedError()

    def drop_namespace(self, identifier: Identifier | str):
        raise NotImplementedError()

    def drop_table(self, identifier: Identifier | str):
        raise NotImplementedError()

    def get_table(self, identifier: Identifier | str) -> Table:
        try:
            res = self._client.get_table(DatabaseName=self._database_name, Name=str(identifier))
            return GlueTable._from_table_info(self, res["Table"])
        except self._client.exceptions.EntityNotFoundException:
            raise NotFoundError(f"Table {identifier} not found")

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
            raise e  # just re-throw for now


class GlueTable(Table):
    _catalog: GlueCatalog
    _table: GlueTableInfo

    def __init__(self, catalog: GlueCatalog, table: GlueTableInfo):
        self._catalog = catalog
        self._table = table

    @staticmethod
    def _from_table_info(catalog: GlueCatalog, table: GlueTableInfo) -> GlueTable:
        for factory in (GlueGlobTable._from_table_info,):
            try:
                return factory(catalog, table)
            except ValueError:
                pass
        raise ValueError(f"Not able to determine the GlueTable implementation for response {table}")

    @property
    def name(self) -> str:
        return self._table["Name"]

    def __repr__(self) -> str:
        import json

        return json.dumps(self._table, indent=4, default=str)


class GlueGlobTable(GlueTable):
    """GlueTable implemented by scanning files by glob prefix."""

    _location: str
    _format: GlueGlobTableFormat
    _schema: Schema

    def __init__(self):
        raise ValueError("GlueGlobTable.__init__() not supported!")

    @staticmethod
    def _from_table_info(catalog: GlueCatalog, table: GlueTableInfo) -> GlueGlobTable:
        """Creates a GlueGlobTable with the specified format type.

        Parameters:
            catalog (GlueCatalog): The catalog this table belongs to
            table (GlueTableInfo): The table information from Glue

        Returns:
            GlueGlobTable: A new table instance with the specified format.
        """
        t = GlueGlobTable.__new__(GlueGlobTable)
        t._catalog = catalog
        t._table = table
        t._location = table["StorageDescriptor"]["Location"]
        t._schema = _convert_glue_schema(table["StorageDescriptor"]["Columns"])

        # check properties for necessary glob path information
        properties: Properties = table["Parameters"]

        # csv or parquet files should not have a table_type
        if table_type := properties.get("table_type"):
            raise ValueError(f"GlueTable information had table type {table_type} but should be none for a glob table.")

        # we'll use the "classification" property to figure out which io method to use.
        if classification := properties.get("classification"):
            t._format = GlueGlobTableFormat.from_str(classification)
        else:
            raise ValueError(
                "GlueTable information is missing the 'classification' property which is required for glob tables."
            )

        return t

    def read(self, **options) -> DataFrame:
        from daft.io.__glob import GlobSource
        source = GlobSource(
            paths=[self._location],
            schema=self._schema,
        )
        return source.to_df()

    def write(self, df: DataFrame, mode: Literal["append", "overwrite"] = "append", **options) -> None:
        raise NotImplementedError()


class GlueGlobTableFormat(Enum):
    """Enum representing supported GlueGlobTable formats."""
    CSV = "csv"
    PARQUET = "parquet"

    @classmethod
    def from_str(cls, value: str) -> GlueGlobTableFormat:
        try:
            return cls(value.lower())
        except ValueError:
            raise ValueError(
                f"Unknown glob table format: {value}. Expected one of: {', '.join(item.value for item in cls)}"
            )


def _convert_glue_schema(columns: list[GlueColumnInfo]) -> Schema:
    return Schema._from_fields([_convert_glue_column(column) for column in columns])


def _convert_glue_column(column: GlueColumnInfo) -> Field:
    return Field.create(column["Name"], _convert_glue_type(column["Type"]))


def _convert_glue_type(type: str) -> DataType:
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
