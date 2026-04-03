"""WARNING! These APIs are internal; please use load_gravitino() or Catalog.from_gravitino()."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Literal

from daft.catalog import Catalog, Function, Identifier, NotFoundError, Properties, Schema, Table
from daft.catalog.__gravitino._client import GravitinoClient as InnerCatalog
from daft.catalog.__gravitino._client import GravitinoTable as InnerTable
from daft.io._parquet import read_parquet
from daft.io.iceberg._iceberg import read_iceberg

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyiceberg.table import Table as PyIcebergTable

    from daft.dataframe import DataFrame
    from daft.io import IOConfig
    from daft.io.partitioning import PartitionField


class GravitinoCatalog(Catalog):
    _inner: InnerCatalog

    def __init__(self) -> None:
        raise RuntimeError("GravitinoCatalog.__init__ is not supported, please use `Catalog.from_gravitino` instead.")

    @property
    def name(self) -> str:
        return f"gravitino_{self._inner._metalake_name}"

    @staticmethod
    def _from_obj(obj: object) -> GravitinoCatalog:
        """Returns a GravitinoCatalog instance if the given object can be adapted so."""
        if isinstance(obj, InnerCatalog):
            catalog = GravitinoCatalog.__new__(GravitinoCatalog)
            catalog._inner = obj
            return catalog
        raise ValueError(f"Unsupported gravitino catalog type: {type(obj)}")

    ###
    # create_*
    ###

    def _create_function(self, ident: Identifier, function: Function | Callable[..., Any]) -> None:
        raise NotImplementedError("Gravitino does not support function registration.")

    def _create_namespace(self, identifier: Identifier) -> None:
        raise NotImplementedError("Gravitino create_namespace not yet supported.")

    def _create_table(
        self,
        identifier: Identifier,
        source: Schema,
        properties: Properties | None = None,
        partition_fields: list[PartitionField] | None = None,
    ) -> Table:
        raise NotImplementedError("Gravitino create_table not yet supported.")

    ###
    # drop_*
    ###

    def _drop_namespace(self, identifier: Identifier) -> None:
        raise NotImplementedError("Gravitino drop_namespace not yet supported.")

    def _drop_table(self, identifier: Identifier) -> None:
        raise NotImplementedError("Gravitino drop_table not yet supported.")

    ###
    # get_*
    ###

    def _get_function(self, ident: Identifier) -> Function:
        raise NotFoundError(f"Function '{ident}' not found in catalog '{self.name}'")

    def _get_table(self, ident: Identifier) -> GravitinoTable:
        try:
            return GravitinoTable._from_obj(self._inner.load_table(str(ident)))
        except Exception as e:
            if "not found" in str(e).lower():
                raise NotFoundError(f"Table {ident} not found!")
            raise

    ###
    # list_.*
    ###

    def _list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        if not pattern:
            return [
                Identifier.from_str(namespace)
                for cat in self._inner.list_catalogs()
                for namespace in self._inner.list_namespaces(cat)
            ]
        return [Identifier.from_str(namespace) for namespace in self._inner.list_namespaces(pattern)]

    def _list_tables(self, pattern: str | None = None) -> list[Identifier]:
        if not pattern:
            return [
                Identifier.from_str(tbl)
                for cat in self._inner.list_catalogs()
                for namespace in self._inner.list_namespaces(cat)
                for tbl in self._inner.list_tables(namespace)
            ]
        dot_count = pattern.count(".")
        if dot_count == 0:
            return [
                Identifier.from_str(tbl)
                for namespace in self._inner.list_namespaces(pattern)
                for tbl in self._inner.list_tables(namespace)
            ]
        elif dot_count == 1:
            return [Identifier.from_str(tbl) for tbl in self._inner.list_tables(pattern)]
        else:
            raise ValueError(
                f"Unrecognized catalog name or namespace name, expected a '.'-separated namespace but received: {pattern}"
            )

    ###
    # has_.*
    ###

    def _has_namespace(self, ident: Identifier) -> bool:
        if len(ident) != 2:
            return False
        catalog_name = ident[0]
        namespaces = self._inner.list_namespaces(catalog_name)
        return str(ident) in namespaces

    def _has_table(self, ident: Identifier) -> bool:
        try:
            self._inner.load_table(str(ident))
            return True
        except Exception:
            return False


class GravitinoTable(Table, ABC):
    """Base class for Gravitino table formats."""

    _inner: InnerTable

    def __init__(self) -> None:
        raise RuntimeError("GravitinoTable.__init__ is not supported, please use `Table.from_gravitino` instead.")

    @classmethod
    @abstractmethod
    def _from_inner(cls, inner: InnerTable) -> GravitinoTable:
        """Returns a table wrapping the given InnerTable, or raises ValueError if the format is not supported."""

    @staticmethod
    def _from_obj(obj: object) -> GravitinoTable:
        """Returns a GravitinoTable if the given object can be adapted so."""
        if isinstance(obj, InnerTable):
            for impl in (GravitinoIcebergTable, GravitinoParquetTable):
                try:
                    return impl._from_inner(obj)
                except ValueError:
                    pass
            raise ValueError(
                f"Unsupported Gravitino table format: {obj.table_info.format!r} "
                f"(table_type={obj.table_info.table_type!r})"
            )
        raise ValueError(f"Unsupported gravitino table type: {type(obj)}")

    @property
    def name(self) -> str:
        return self._inner.table_info.name

    def schema(self) -> Schema:
        return self.read().schema()


class GravitinoIcebergTable(GravitinoTable):
    """GravitinoIcebergTable is for Gravitino tables with format starting with 'ICEBERG'."""

    _pyiceberg_table: PyIcebergTable
    _read_options: set[str] = {"snapshot_id"}
    _write_options: set[str] = set()

    @classmethod
    def _from_inner(cls, inner: InnerTable) -> GravitinoIcebergTable:
        if not inner.table_info.format.upper().startswith("ICEBERG"):
            raise ValueError(f"Expected ICEBERG format, got {inner.table_info.format!r}")
        t = GravitinoIcebergTable.__new__(GravitinoIcebergTable)
        t._inner = inner
        t._pyiceberg_table = _open_iceberg_table(inner.table_info.storage_location, inner.io_config)
        return t

    def read(self, **options: Any) -> DataFrame:
        Table._validate_options("Gravitino read", options, self._read_options)
        return read_iceberg(
            table=self._pyiceberg_table,
            snapshot_id=options.get("snapshot_id"),
            io_config=self._inner.io_config,
        )

    def append(self, df: DataFrame, **options: Any) -> None:
        self._validate_options("Gravitino write", options, self._write_options)
        raise NotImplementedError("Writing to Iceberg tables through Gravitino is not yet supported")

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        self._validate_options("Gravitino write", options, self._write_options)
        raise NotImplementedError("Writing to Iceberg tables through Gravitino is not yet supported")


class GravitinoParquetTable(GravitinoTable):
    """GravitinoParquetTable is for Gravitino Hive tables with PARQUET format."""

    _read_options: set[str] = set()
    _write_options: set[str] = set()

    @classmethod
    def _from_inner(cls, inner: InnerTable) -> GravitinoParquetTable:
        if not (inner.table_info.format.upper() == "PARQUET" and "HIVE" in inner.table_info.table_type.upper()):
            raise ValueError(
                f"Expected PARQUET/Hive table, got format={inner.table_info.format!r} "
                f"table_type={inner.table_info.table_type!r}"
            )
        t = GravitinoParquetTable.__new__(GravitinoParquetTable)
        t._inner = inner
        return t

    def read(self, **options: Any) -> DataFrame:
        Table._validate_options("Gravitino read", options, self._read_options)
        return read_parquet(
            path=self._inner.table_info.storage_location,
            io_config=self._inner.io_config,
            hive_partitioning=True,
        )

    def append(self, df: DataFrame, **options: Any) -> None:
        self._validate_options("Gravitino write", options, self._write_options)
        raise NotImplementedError("Writing PARQUET format tables is not yet supported")

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        self._validate_options("Gravitino write", options, self._write_options)
        raise NotImplementedError("Writing PARQUET format tables is not yet supported")


def _open_iceberg_table(storage_location: str, io_config: IOConfig | None) -> PyIcebergTable:
    """Load a PyIceberg Table from a storage directory via HadoopCatalog."""
    from pyiceberg.catalog.hadoop import HadoopCatalog

    parent, table_dir = storage_location.rstrip("/").rsplit("/", 1)
    props: dict[str, str] = {"warehouse": parent}
    props.update(_io_config_to_pyiceberg_props(io_config))
    return HadoopCatalog("gravitino_reader", props).load_table(table_dir)


def _io_config_to_pyiceberg_props(io_config: IOConfig | None) -> dict[str, str]:
    """Convert a Daft IOConfig to PyIceberg FileIO properties."""
    if io_config is None or io_config.s3 is None:
        return {}
    s3 = io_config.s3
    props: dict[str, str] = {}
    if s3.key_id:
        props["s3.access-key-id"] = s3.key_id
    if s3.access_key:
        props["s3.secret-access-key"] = s3.access_key
    if s3.endpoint_url:
        props["s3.endpoint"] = s3.endpoint_url
    if s3.session_token:
        props["s3.session-token"] = s3.session_token
    if s3.region_name:
        props["s3.region"] = s3.region_name
    return props


def load_gravitino(
    endpoint: str,
    metalake_name: str,
    auth_type: Literal["simple", "oauth2"] = "simple",
    username: str | None = None,
    password: str | None = None,
    token: str | None = None,
) -> GravitinoCatalog:
    """Creates a GravitinoCatalog for the given Gravitino metalake.

    Args:
        endpoint (str): Gravitino server endpoint URL.
        metalake_name (str): Name of the metalake to connect to.
        auth_type (str): Authentication type, either ``"simple"`` or ``"oauth2"``. Defaults to ``"simple"``.
        username (str, optional): Username for simple authentication.
        password (str, optional): Password for simple authentication.
        token (str, optional): Bearer token for OAuth2 authentication.

    Returns:
        GravitinoCatalog: Catalog instance backed by the given Gravitino metalake.

    Examples:
        >>> from daft.catalog.__gravitino import load_gravitino
        >>> catalog = load_gravitino(
        ...     endpoint="http://localhost:8090",
        ...     metalake_name="my_metalake",
        ...     username="admin",
        ... )
        >>> catalog.list_tables("my_catalog.my_schema")
    """
    client = InnerCatalog(
        endpoint=endpoint,
        metalake_name=metalake_name,
        auth_type=auth_type,
        username=username,
        password=password,
        token=token,
    )
    return GravitinoCatalog._from_obj(client)
