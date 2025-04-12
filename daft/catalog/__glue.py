"""WARNING! THIS PACKAGE IS INTERNAL AND IS SUBJECT TO CHANGE."""

from __future__ import annotations

from abc import ABC, abstractmethod
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
    from daft.logical.builder import LogicalPlanBuilder
else:
    GlueClient = Any
    GlueColumnInfo = Any
    GlueTableInfo = Any

Parameters = dict[str, Any]


def load_glue(
    name: str,
    region_name: str | None = None,
    api_version: str | None = None,
    use_ssl: bool | None = None,
    verify: bool | str | None = None,
    endpoint_url: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_session_token: str | None = None,
) -> GlueCatalog:
    """Creates a GlueCatalog with some give name and optional boto3 client configuration.

    Args:
        name (str): The name of the database to retrieve. For Hive compatibility, this should be all lowercase.
        region_name (str, optional): The AWS region to connect to.
        api_version (str, optional): The API version to use for the AWS Glue service.
        use_ssl (bool, optional): Whether to use SSL when connecting to AWS.
        verify (bool | str, optional): Whether to verify SSL certificates or a path to a CA bundle.
        endpoint_url (str, optional): Alternative endpoint URL to connect to.
        aws_access_key_id (str, optional): AWS access key ID for authentication.
        aws_secret_access_key (str, optional): AWS secret access key for authentication.
        aws_session_token (str, optional): AWS session token for temporary credentials.

    Returns:
        GlueCatalog: Catalog instance backed by an AWS Glue Database.
    """
    c = GlueCatalog.__new__(GlueCatalog)
    c._name = name
    options = {}
    if region_name is not None:
        options["region_name"] = region_name
    if api_version is not None:
        options["api_version"] = api_version
    if use_ssl is not None:
        options["use_ssl"] = use_ssl
    if verify is not None:
        options["verify"] = verify
    if endpoint_url is not None:
        options["endpoint_url"] = endpoint_url
    if aws_access_key_id is not None:
        options["aws_access_key_id"] = aws_access_key_id
    if aws_secret_access_key is not None:
        options["aws_secret_access_key"] = aws_secret_access_key
    if aws_session_token is not None:
        options["aws_session_token"] = aws_session_token
    c._client = boto3.client("glue", **options)
    return c


class GlueCatalog(Catalog):
    """The GlueCatalog maps to an AWS Glue Database using a boto3 client."""
    _client: GlueClient
    _name: str

    # !! PATCH HERE TO PROVIDE CUSTOM GLUE TABLE IMPLEMENTATIONS !!
    _table_impls: list[type[GlueTable]] = []

    def __new__(cls, *args, **kwargs):
        if cls is GlueCatalog:
            cls._table_impls = [GlueGlobTable]
        return super().__new__(cls)

    def __init__(self):
        raise ValueError("GlueCatalog.__init__() not supported!")

    @staticmethod
    def from_client(name: str, client: GlueClient) -> GlueCatalog:
        """Creates a GlueCatalog using the given boto3 client."""
        c = GlueCatalog.__new__(GlueCatalog)
        c._name = name
        c._client = client
        return c

    @staticmethod
    def from_session(name: str, session: Session) -> GlueCatalog:
        """Creates a GlueCatalog using the boto3 session to get a glue client."""
        c = GlueCatalog.__new__(GlueCatalog)
        c._name = name
        c._client = session.client("glue")
        return c

    @property
    def name(self) -> str:
        return self._name

    def create_namespace(self, identifier: Identifier | str):
        """Creates a namespace (database) in AWS Glue.
        
        Args:
            identifier (Identifier | str): The name of the database to create.

        Returns:
            None
        """
        try:
            self._client.create_database(
                DatabaseInput={
                    'Name': str(identifier),
                }
            )
        except self._client.exceptions.AlreadyExistsException:
            raise ValueError(f"Namespace {identifier} already exists")

    def create_table(self, identifier: Identifier | str, source: TableSource | object) -> Table:
        """Creates a table in AWS Glue.
        
        Args:
            identifier (Identifier | str): The name of the table to create.
            source (TableSource | object): The source data for the table.

        Returns:
            Table: The created table.
        """
        # Table creation implementation will be added later
        raise NotImplementedError("Table creation not yet implemented")

    def drop_namespace(self, identifier: Identifier | str):
        """Drops a namespace (database) from AWS Glue.
        
        Args:
            identifier (Identifier | str): The name of the database to drop.

        Returns:
            None
        """
        try:
            self._client.delete_database(
                Name=str(identifier)
            )
        except self._client.exceptions.EntityNotFoundException:
            raise NotFoundError(f"Namespace {identifier} not found")

    def drop_table(self, identifier: Identifier | str):
        """Drops a table from AWS Glue.
        
        Args:
            identifier (Identifier | str): The name of the table to drop.

        Returns:    
            None
        """

        if isinstance(identifier, str):
            identifier = Identifier.from_str(identifier)
        if len(identifier) != 2:
            raise ValueError(f"Expected identifier with form `<database_name>.<table_name>` but found {identifier}")

        try:
            self._client.delete_table(
                DatabaseName=identifier[0],
                Name=identifier[1]
            )
        except self._client.exceptions.EntityNotFoundException:
            raise NotFoundError(f"Table {identifier} not found")


    def get_table(self, identifier: Identifier | str) -> Table:
        """Gets a table by its identifier (<database_name>.<table_name>).
        
        Args:
            identifier (Identifier | str): 
        """

        if isinstance(identifier, str):
            identifier = Identifier.from_str(identifier)
        if len(identifier) != 2:
            raise ValueError(f"Expected identifier with form `<database_name>.<table_name>` but found {identifier}")

        try:
            res = self._client.get_table(
                DatabaseName=identifier[0],
                Name=identifier[1]
            )
            for impl in self._table_impls:
                try:
                    return impl.from_table_info(self, res["Table"])
                except ValueError:
                    pass
            raise ValueError(f"No supported table implementation for Table {res['Table']}.")
        except self._client.exceptions.EntityNotFoundException:
            raise NotFoundError(f"Table {identifier} not found")

    def has_namespace(self, identifier: Identifier | str) -> bool:
        """Checks if a namespace (database) exists in AWS Glue.
        
        Args:
            identifier (Identifier | str): The name of the database to check.
            
        Returns:
            bool: True if the namespace (database) exists, False otherwise.
        """
        try:
            self._client.get_database(Name=str(identifier))
            return True
        except self._client.exceptions.EntityNotFoundException:
            return False

    def list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        """Lists namespaces (databases) in AWS Glue.

        Args:
            pattern (str | None): Pattern is NOT supported by Glue.
            
        Returns:
            list[Identifier]: List of namespace identifiers.
        """

        if pattern is not None:
            # Glue may add some kind of pattern to get_databases, then we can use their scheme.
            # Rather than making something up now which could later be broken.
            raise ValueError("GlueCatalog does not support using pattern filters with get_databases.")

        try:
            req = {}
            namespaces = []
            while True:
                res = self._client.get_databases(**req)
                for database in res["DatabaseList"]:
                    namespaces.append(Identifier(database["Name"]))
                if next_token := res.get("NextToken"):
                    req["NextToken"] = next_token
                else:
                    break
            return namespaces
        except ClientError as e:
            raise ValueError(f"Failed to list namespaces: {e}")

    def list_tables(self, pattern: str | None = None) -> list[str]:
        if pattern is None:
            raise ValueError("GlueCatalog requires the pattern to contain a namespace.")

        req = {}
        if "." in pattern:
            database_name, expression = pattern.split(".", 1)
            req["DatabaseName"] = database_name
            req["Expression"] = expression
        else:
            req["DatabaseName"] = pattern

        try:
            tables = []
            while True:
                res = self._client.get_tables(**req)
                for table in res["TableList"]:
                    tables.append(f'{table["DatabaseName"]}.{table["Name"]}')
                if next_token := res.get("NextToken"):
                    req["NextToken"] = next_token
                else:
                    break
            return tables
        except ClientError as e:
            raise e  # just re-throw for now


class GlueTable(Table, ABC):
    """"""
    _catalog: GlueCatalog
    _table: GlueTableInfo

    def __init__(self, catalog: GlueCatalog, table: GlueTableInfo):
        self._catalog = catalog
        self._table = table

    @classmethod
    @abstractmethod
    def from_table_info(cls, catalog: GlueCatalog, table: GlueTableInfo) -> GlueTable:
        """Returns a GlueTable if the GlueTableInfo matches this implementation's requirements, otherwise raise a ValueError."""

    @property
    def name(self) -> str:
        return self._table["Name"]

    def __repr__(self) -> str:
        import json

        return json.dumps(self._table, indent=4, default=str)

class GlueGlobTable(GlueTable):
    """GlueTable implemented by scanning files by glob prefix."""
    _location: str
    _format: str
    _schema: Schema
    _builder: LogicalPlanBuilder

    def __init__(self):
        raise ValueError("GlueGlobTable.__init__() not supported!")

    @classmethod
    def from_table_info(cls, catalog: GlueCatalog, table: GlueTableInfo) -> GlueGlobTable:
        # validate parameters information
        parameters: Parameters = table.get("Parameters", {})
        if table_type := parameters.get("table_type"):
            raise ValueError(f"GlueTableInfo had table type {table_type}, but should be none for a glob table.")
        if "classification" not in parameters:
            raise ValueError("GlueTableInfo is missing the required parameeter 'classification for glob table.")

        t = GlueGlobTable.__new__(GlueGlobTable)
        t._builder = cls._convert_classification(parameters["classification"])
        t._catalog = catalog
        t._table = table
        t._location = table["StorageDescriptor"]["Location"]
        t._schema = _convert_glue_schema(table["StorageDescriptor"]["Columns"])
        return t

    def read(self, **options) -> DataFrame:
        return DataFrame(self._builder)

    def write(self, df: DataFrame, mode: Literal["append", "overwrite"] = "append", **options) -> None:
        # I think we should be saving parsed format info rather than a builder
        raise NotImplementedError()

    @classmethod
    def _convert_classification(cls, classification: str, table: GlueTableInfo) -> LogicalPlanBuilder:
        classifications = {
            "csv": cls._from_csv,
            "parquet": cls._from_parquet,
        }
        if factory := classifications[classification.lower()]:
            return factory(table)
        else:
            raise ValueError(f"Unknown classification {classification}")

    @classmethod
    def _from_csv(cls, table: GlueTableInfo) -> LogicalPlanBuilder:
        raise NotImplementedError("_from_csv")

    @classmethod
    def _from_parquet(cls, table: GlueTableInfo) -> LogicalPlanBuilder:
        raise NotImplementedError("_from_parquet")


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
