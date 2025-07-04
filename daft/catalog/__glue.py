"""WARNING! THIS PACKAGE IS INTERNAL AND IS SUBJECT TO CHANGE."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

import boto3
import botocore
from botocore.exceptions import ClientError

from daft.catalog import Catalog, Identifier, NotFoundError, Properties, Table
from daft.datatype import DataType
from daft.logical.schema import Field, Schema

if TYPE_CHECKING:
    from mypy_boto3_glue import GlueClient
    from mypy_boto3_glue.type_defs import ColumnOutputTypeDef as GlueColumnInfo
    from mypy_boto3_glue.type_defs import TableTypeDef as GlueTableInfo
    from pyiceberg.table import Table as PyIcebergTable

    from daft.daft import IOConfig
    from daft.dataframe import DataFrame
    from daft.unity_catalog import UnityCatalogTable
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
    options: dict[str, Any] = {}
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

    _name: str
    _client: GlueClient

    # !! PATCH HERE TO PROVIDE CUSTOM GLUE TABLE IMPLEMENTATIONS !!
    _table_impls: list[type[GlueTable]] = []

    def __new__(cls) -> GlueCatalog:
        if cls is GlueCatalog:
            cls._table_impls = [
                GlueCsvTable,
                GlueParquetTable,
                GlueIcebergTable,
                GlueDeltaTable,
            ]
        return super().__new__(cls)

    def __init__(self) -> None:
        raise ValueError("GlueCatalog.__init__() not supported!")

    @staticmethod
    def from_client(name: str, client: GlueClient) -> GlueCatalog:
        """Creates a GlueCatalog using the given boto3 client."""
        c = GlueCatalog.__new__(GlueCatalog)
        c._name = name
        c._client = client
        return c

    @staticmethod
    def from_session(name: str, session: boto3.Session | botocore.session.Session) -> GlueCatalog:
        """Creates a GlueCatalog using the provided boto3 or botocore session to get a glue client.

        Args:
            name: Name of the catalog
            session: A boto3.Session or botocore.session.Session object

        Returns:
            A configured GlueCatalog instance
        """
        c = GlueCatalog.__new__(GlueCatalog)
        c._name = name
        if isinstance(session, boto3.Session):
            c._client = session.client("glue")
        elif isinstance(session, botocore.session.Session):
            c._client = session.create_client("glue")
        else:
            raise TypeError(f"Expected boto3.Session or botocore.session.Session, got {type(session).__name__}")

        return c

    @property
    def name(self) -> str:
        return self._name

    def _create_namespace(self, identifier: Identifier) -> None:
        try:
            self._client.create_database(
                DatabaseInput={
                    "Name": str(identifier),
                }
            )
        except self._client.exceptions.AlreadyExistsException:
            raise ValueError(f"Namespace {identifier} already exists")

    def _create_table(
        self,
        identifier: Identifier,
        schema: Schema,
        properties: Properties | None = None,
    ) -> Table:
        # Table creation implementation will be added later
        raise NotImplementedError("Table creation not yet implemented")

    def _drop_namespace(self, identifier: Identifier) -> None:
        try:
            self._client.delete_database(Name=str(identifier))
        except self._client.exceptions.EntityNotFoundException:
            raise NotFoundError(f"Namespace {identifier} not found")

    def _drop_table(self, identifier: Identifier) -> None:
        if len(identifier) != 2:
            raise ValueError(f"Expected identifier with form `<database_name>.<table_name>` but found {identifier}")
        database_name: str = str(identifier[0])
        name: str = str(identifier[1])

        try:
            self._client.delete_table(DatabaseName=database_name, Name=name)
        except self._client.exceptions.EntityNotFoundException:
            raise NotFoundError(f"Table {identifier} not found")

    def _get_table(self, identifier: Identifier) -> Table:
        if len(identifier) != 2:
            raise ValueError(f"Expected identifier with form `<database_name>.<table_name>` but found {identifier}")
        database_name: str = str(identifier[0])
        name: str = str(identifier[1])

        try:
            res = self._client.get_table(DatabaseName=database_name, Name=name)
            tbl = res["Table"]
            for impl in self._table_impls:
                try:
                    return impl.from_table_info(self, tbl)
                except ValueError:
                    pass
            classification = tbl["Parameters"].get("classification", "unknown")
            table_type = tbl["Parameters"].get("table_type", "unknown")
            raise ValueError(
                f"No supported table implementation for Table with classification='{classification}' and table_type='{table_type}'."
            )
        except self._client.exceptions.EntityNotFoundException:
            raise NotFoundError(f"Table {identifier} not found")

    def _has_table(self, identifier: Identifier) -> bool:
        try:
            self._get_table(identifier)
            return True
        except self._client.exceptions.EntityNotFoundException:
            return False

    def _has_namespace(self, identifier: Identifier) -> bool:
        try:
            self._client.get_database(Name=str(identifier))
            return True
        except self._client.exceptions.EntityNotFoundException:
            return False

    def _list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        if pattern is not None:
            # Glue may add some kind of pattern to get_databases, then we can use their scheme.
            # Rather than making something up now which could later be broken.
            raise ValueError("GlueCatalog does not support using pattern filters with get_databases.")

        try:
            req = {}  # type: ignore
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
            raise ValueError("Failed to list namespaces.") from e

    def _list_tables(self, pattern: str | None = None) -> list[Identifier]:
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
                    tables.append(Identifier(table["DatabaseName"], table["Name"]))
                if next_token := res.get("NextToken"):
                    req["NextToken"] = next_token
                else:
                    break
            return tables
        except ClientError as e:
            raise ValueError("Failed to list tables.") from e


class GlueTable(Table, ABC):
    """GlueTable is the base class for various Glue table formats."""

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

    def schema(self) -> Schema:
        return self.read().schema()

    def __repr__(self) -> str:
        import json

        return json.dumps(self._table, indent=4, default=str)


class GlueCsvTable(GlueTable):
    """GlueCsvTable is for Glue classification='CSV' where we have delimited files under a common S3 prefix."""

    _path: str
    _schema: Schema
    _has_headers: bool = True
    _delimiter: str = ","
    _io_config: IOConfig | None = None
    _hive_partitioning: bool = False
    _hive_partitioning_cols: list[str] = []

    def __init__(self) -> None:
        raise ValueError("GlueCsvTable.__init__() not supported!")

    @classmethod
    def from_table_info(cls, catalog: GlueCatalog, table: GlueTableInfo) -> GlueTable:
        # validate parameters information
        parameters: Parameters = table.get("Parameters", {})

        # check 'csv'
        classification = parameters.get("classification")
        if classification is None:
            raise ValueError("GlueTableInfo is missing the required parameter 'classification'.")
        if classification.lower() != "csv":
            raise ValueError(f"GlueTableInfo had classification {classification}, but expected 'CSV'")

        t = GlueCsvTable.__new__(GlueCsvTable)
        t._catalog = catalog
        t._table = table
        t._io_config = None  # todo

        # parse csv format information
        t._schema = _convert_glue_schema(table["StorageDescriptor"]["Columns"])
        t._path = table["StorageDescriptor"]["Location"]
        t._has_headers = parameters.get("skip.header.line.count", "0") == "1"
        t._delimiter = parameters.get("delimiter", ",")
        t._hive_partitioning = False
        t._hive_partitioning_cols = []

        return t

    def read(self, **options: Any) -> DataFrame:
        from daft.io._csv import read_csv

        return read_csv(
            path=self._path,
            infer_schema=False,
            schema={c.name: c.dtype for c in self._schema},
            has_headers=self._has_headers,
            delimiter=self._delimiter,
            double_quote=True,
            quote=None,
            escape_char=None,
            comment=None,
            allow_variable_columns=False,
            io_config=self._io_config,
            file_path_column=None,
            hive_partitioning=self._hive_partitioning,
        )

    def append(self, df: DataFrame, **options: Any) -> None:
        df.write_csv(
            root_dir=self._path,
            write_mode="append",
            partition_cols=(self._hive_partitioning_cols if self._hive_partitioning else None),  # type: ignore
        )

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        df.write_csv(
            root_dir=self._path,
            write_mode="overwrite",
            partition_cols=(self._hive_partitioning_cols if self._hive_partitioning else None),  # type: ignore
        )


class GlueParquetTable(GlueTable):
    """GlueParquetTable is for Glue classification='parquet' where we have parquet files under a common S3 prefix."""

    _path: str
    _schema: Schema
    _io_config: IOConfig | None = None
    _hive_partitioning: bool = False
    _hive_partitioning_cols: list[str] = []

    def __init__(self) -> None:
        raise ValueError("GlueParquetTable.__init__() not supported!")

    @classmethod
    def from_table_info(cls, catalog: GlueCatalog, table: GlueTableInfo) -> GlueTable:
        # validate parameters information
        parameters: Parameters = table.get("Parameters", {})

        # check 'parquet'
        classification = parameters.get("classification")
        if classification is None:
            raise ValueError("GlueTableInfo is missing the required parameter 'classification'.")
        if classification.lower() != "parquet":
            raise ValueError(f"GlueTableInfo had classification {classification}, but expected 'parquet'")

        t = GlueParquetTable.__new__(GlueParquetTable)
        t._catalog = catalog
        t._table = table
        t._io_config = None  # todo

        # parse parquet format information
        t._schema = _convert_glue_schema(table["StorageDescriptor"]["Columns"])
        t._path = table["StorageDescriptor"]["Location"]

        return t

    def read(self, **options: Any) -> DataFrame:
        from daft.io._parquet import read_parquet

        return read_parquet(
            path=self._path,
            row_groups=None,
            infer_schema=False,
            schema={c.name: c.dtype for c in self._schema},
            io_config=self._io_config,
            file_path_column=None,
            hive_partitioning=self._hive_partitioning,
            coerce_int96_timestamp_unit=None,
            schema_hints=None,
        )

    def append(self, df: DataFrame, **options: Any) -> None:
        df.write_parquet(
            root_dir=self._path,
            compression="snappy",
            write_mode="append",
            partition_cols=(self._hive_partitioning_cols if self._hive_partitioning else None),  # type: ignore
            io_config=self._io_config,
        )

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        df.write_parquet(
            root_dir=self._path,
            compression="snappy",
            write_mode="overwrite",
            partition_cols=(self._hive_partitioning_cols if self._hive_partitioning else None),  # type: ignore
            io_config=self._io_config,
        )


class GlueIcebergTable(GlueTable):
    """GlueIcebergTable is for Glue table_type='ICEBERG'."""

    _io_config: IOConfig | None
    _pyiceberg_table: PyIcebergTable

    def __init__(self) -> None:
        raise ValueError("GlueIcebergTable.__init__() not supported!")

    @classmethod
    def from_table_info(cls, catalog: GlueCatalog, table: GlueTableInfo) -> GlueTable:
        parameters: Parameters = table.get("Parameters", {})

        # verify we have table_type = "ICEBERG"
        table_type = parameters.get("table_type")
        if table_type is None:
            raise ValueError("GlueIcebegTable is missing the required 'table_type' parameter.")
        if table_type.lower() != "iceberg":
            raise ValueError(f"GlueIcebergTable had table_type {table_type}, but expected 'ICEBERG'")

        # create a pyiceberg catalog so we can write
        t = GlueIcebergTable.__new__(GlueIcebergTable)
        t._catalog = catalog
        t._table = table
        t._io_config = None  # todo
        t._pyiceberg_table = cls._create_pyiceberg_table(catalog, table)

        return t

    @classmethod
    def _create_pyiceberg_table(cls, catalog: GlueCatalog, table: GlueTableInfo) -> PyIcebergTable:
        from pyiceberg.catalog.glue import GlueCatalog as PyIcebergGlueCatalog

        # cannot create a pyiceberg GlueCatalog with custom client directly
        gc = PyIcebergGlueCatalog.__new__(PyIcebergGlueCatalog)
        gc.name = catalog.name
        gc.properties = {}
        gc.glue = catalog._client

        # the pyiceberg table will hold a ref to gc
        return gc._convert_glue_to_iceberg(table)

    def read(self, **options: Any) -> DataFrame:
        from daft.io.iceberg._iceberg import read_iceberg

        return read_iceberg(
            table=self._pyiceberg_table, snapshot_id=options.get("snapshot_id"), io_config=self._io_config
        )

    def append(self, df: DataFrame, **options: Any) -> None:
        df.write_iceberg(self._pyiceberg_table, mode="append")

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        df.write_iceberg(self._pyiceberg_table, mode="overwrite")


class GlueDeltaTable(GlueTable):
    """GlueDeltaTable is for Glue table_type='ICEBERG'."""

    _io_config: IOConfig | None
    _unity_catalog_table: UnityCatalogTable

    def __init__(self) -> None:
        raise ValueError("GlueDeltaTable.__init__() not supported!")

    @classmethod
    def from_table_info(cls, catalog: GlueCatalog, table: GlueTableInfo) -> GlueTable:
        parameters: Parameters = table.get("Parameters", {})

        # verify we have table_type = "delta"
        table_type = parameters.get("table_type")
        if table_type is None:
            raise ValueError("GlueDeltaTable is missing the required 'table_type' parameter.")
        if table_type.lower() != "delta":
            raise ValueError(f"GlueDeltaTable had table_type {table_type}, but expected 'delta'")

        t = GlueDeltaTable.__new__(GlueDeltaTable)
        t._catalog = catalog
        t._table = table
        t._io_config = None  # todo
        t._unity_catalog_table = cls._create_unity_catalog_table(table, None)

        return t

    @classmethod
    def _create_unity_catalog_table(cls, table: GlueTableInfo, io_config: IOConfig | None) -> UnityCatalogTable:
        from unitycatalog.types import TableInfo

        from daft.unity_catalog import UnityCatalogTable

        table_info = TableInfo(
            # catalog_name=
            # columns=[],
            comment=None,
            created_at=int(table["CreateTime"].timestamp() * 1000),
            data_source_format="DELTA",
            schema_name=table["DatabaseName"],
        )

        table_uri = table["StorageDescriptor"]["Location"]

        return UnityCatalogTable(table_info, table_uri, io_config)

    def read(self, **options: Any) -> DataFrame:
        from daft.io.delta_lake._deltalake import read_deltalake

        return read_deltalake(
            table=self._unity_catalog_table,
            version=options.get("version"),
            io_config=self._io_config,
        )

    def append(self, df: DataFrame, **options: Any) -> None:
        raise NotImplementedError

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        raise NotImplementedError


def _convert_glue_schema(columns: list[GlueColumnInfo]) -> Schema:
    return Schema._from_fields([_convert_glue_column(column) for column in columns])


def _convert_glue_column(column: GlueColumnInfo) -> Field:
    return Field.create(column["Name"], _convert_glue_type(column["Type"]))


# TODO update to support more Hive types.
# https://cwiki.apache.org/confluence/display/hive/languagemanual+types
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
