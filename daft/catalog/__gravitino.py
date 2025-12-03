"""WARNING! These APIs are internal; please use Catalog.from_gravitino() and Table.from_gravitino()."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.catalog import Catalog, Identifier, NotFoundError, Properties, Schema, Table
from daft.gravitino import GravitinoClient as InnerCatalog
from daft.gravitino import GravitinoTable as InnerTable
from daft.io.iceberg._iceberg import read_iceberg

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
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
        if pattern is None or pattern == "":
            return [
                Identifier.from_str(namespace)
                for cat in self._inner.list_catalogs()
                for namespace in self._inner.list_namespaces(cat)
            ]
        # If pattern is provided, treat it as a catalog name
        catalog_name = pattern
        return [Identifier.from_str(namespace) for namespace in self._inner.list_namespaces(catalog_name)]

    def _list_tables(self, pattern: str | None = None) -> list[Identifier]:
        if pattern is None or pattern == "":
            return [
                Identifier.from_str(tbl)
                for cat in self._inner.list_catalogs()
                for namespace in self._inner.list_namespaces(cat)
                for tbl in self._inner.list_tables(namespace)
            ]
        num_namespaces = pattern.count(".")
        if num_namespaces == 0:
            catalog_name = pattern
            return [
                Identifier.from_str(tbl)
                for namespace in self._inner.list_namespaces(catalog_name)
                for tbl in self._inner.list_tables(namespace)
            ]
        elif num_namespaces == 1:
            namespace_name = pattern
            return [Identifier.from_str(tbl) for tbl in self._inner.list_tables(namespace_name)]
        else:
            raise ValueError(
                f"Unrecognized catalog name or namespace name, expected a '.'-separated namespace but received: {pattern}"
            )

    ###
    # has_.*
    ###
    def _has_namespace(self, ident: Identifier) -> bool:
        # Namespace should be in format "catalog.schema"
        if len(ident) != 2:
            return False
        catalog_name = ident[0]
        # List namespaces for the catalog and check if our identifier is in the list
        namespaces = self._inner.list_namespaces(catalog_name)
        return str(ident) in namespaces

    def _has_table(self, ident: Identifier) -> bool:
        try:
            self._inner.load_table(str(ident))
            return True
        except Exception:
            return False


class GravitinoTable(Table):
    _inner: InnerTable

    _read_options: set[str] = {"snapshot_id"}
    _write_options: set[str] = set()

    def __init__(self) -> None:
        raise RuntimeError("GravitinoTable.__init__ is not supported, please use `Table.from_gravitino` instead.")

    @property
    def name(self) -> str:
        return self._inner.table_info.name

    def schema(self) -> Schema:
        return self.read().schema()

    @staticmethod
    def _from_obj(obj: object) -> GravitinoTable:
        """Returns a GravitinoTable if the given object can be adapted so."""
        if isinstance(obj, InnerTable):
            t = GravitinoTable.__new__(GravitinoTable)
            t._inner = obj
            return t
        raise ValueError(f"Unsupported gravitino table type: {type(obj)}")

    ###
    # read methods
    ###

    def read(self, **options: Any) -> DataFrame:
        Table._validate_options("Gravitino read", options, GravitinoTable._read_options)

        # For Iceberg tables, use the storage location (metadata path)
        if self._inner.table_info.format.upper().startswith("ICEBERG"):
            try:
                return read_iceberg(
                    table=self._inner.table_uri,
                    snapshot_id=options.get("snapshot_id"),
                    io_config=self._inner.io_config,
                )
            except ImportError as e:
                if "pyiceberg" in str(e):
                    raise ImportError(
                        "PyIceberg is required to read Iceberg tables. "
                        "Install it with: pip install 'daft[iceberg]' or pip install pyiceberg"
                    ) from e
                raise
        else:
            # For other formats, we might need different handling
            raise NotImplementedError(
                f"Reading {self._inner.table_info.format} format tables is not yet supported. "
                f"Currently only ICEBERG format (and variants like ICEBERG/PARQUET) are supported."
            )

    ###
    # write methods
    ###

    def append(self, df: DataFrame, **options: Any) -> None:
        self._validate_options("Gravitino write", options, GravitinoTable._write_options)

        if self._inner.table_info.format.upper().startswith("ICEBERG"):
            # For Iceberg tables, we need to create a PyIceberg table object
            # This is more complex and may require additional Gravitino integration
            raise NotImplementedError("Writing to Iceberg tables through Gravitino is not yet supported")
        else:
            raise NotImplementedError(f"Writing {self._inner.table_info.format} format tables is not yet supported")

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        self._validate_options("Gravitino write", options, GravitinoTable._write_options)

        if self._inner.table_info.format.upper().startswith("ICEBERG"):
            # For Iceberg tables, we need to create a PyIceberg table object
            # This is more complex and may require additional Gravitino integration
            raise NotImplementedError("Writing to Iceberg tables through Gravitino is not yet supported")
        else:
            raise NotImplementedError(f"Writing {self._inner.table_info.format} format tables is not yet supported")
