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

    def _detect_format(self) -> str:
        """Detect table format from metadata.

        Returns:
            Format string: "ICEBERG" or "PARQUET"

        Raises:
            ValueError: If format cannot be determined or is unsupported
        """
        format_str = self._inner.table_info.format.upper()

        # Iceberg tables: format is "ICEBERG" or "ICEBERG/PARQUET"
        if format_str.startswith("ICEBERG"):
            return "ICEBERG"

        # Parquet tables: format is "PARQUET" with Hive table_type
        if format_str == "PARQUET":
            table_type = self._inner.table_info.table_type.upper()
            if "HIVE" in table_type or table_type == "":
                return "PARQUET"

        # Unsupported format
        supported = ["ICEBERG", "PARQUET (Hive)"]
        raise ValueError(
            f"Unsupported table format: {format_str} (table_type: {self._inner.table_info.table_type}). "
            f"Supported formats: {', '.join(supported)}"
        )

    def _read_iceberg(self, **options: Any) -> DataFrame:
        """Read an Iceberg table.

        Args:
            snapshot_id: Optional snapshot ID to read from

        Returns:
            DataFrame containing table data

        Raises:
            ImportError: If pyiceberg is not installed
        """
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

    def _read_parquet(self, **options: Any) -> DataFrame:
        """Read a Hive/Parquet table.

        The storage_location points to a directory containing Parquet files.
        Uses Daft's read_parquet with hive_partitioning enabled to handle
        Hive-style partitioned tables.

        Returns:
            DataFrame containing table data
        """
        from daft.io._parquet import read_parquet

        # For Hive tables, storage_location is the data directory
        path = self._inner.table_info.storage_location

        # Enable hive partitioning to handle partitioned tables
        # This will automatically detect partition columns from directory structure
        return read_parquet(
            path=path,
            io_config=self._inner.io_config,
            hive_partitioning=True,
        )

    ###
    # read methods
    ###

    def read(self, **options: Any) -> DataFrame:
        """Read table data into a DataFrame.

        Automatically detects the table format and uses the appropriate
        read method. Supports format-specific options.

        Args:
            **options: Format-specific options
                For Iceberg:
                    - snapshot_id: Read from a specific snapshot

        Returns:
            DataFrame containing table data

        Raises:
            ValueError: If table format is unsupported
            ImportError: If required dependencies are missing
        """
        Table._validate_options("Gravitino read", options, GravitinoTable._read_options)

        format_type = self._detect_format()

        if format_type == "ICEBERG":
            return self._read_iceberg(**options)
        elif format_type == "PARQUET":
            return self._read_parquet(**options)
        else:
            # This should never happen if _detect_format() is correct
            raise ValueError(f"Unsupported format: {format_type}")

    ###
    # write methods
    ###

    def append(self, df: DataFrame, **options: Any) -> None:
        """Append data to the table.

        Args:
            df: DataFrame to append
            **options: Format-specific write options

        Raises:
            NotImplementedError: Write operations are not yet supported
        """
        Table._validate_options("Gravitino write", options, GravitinoTable._write_options)

        format_type = self._detect_format()

        if format_type == "ICEBERG":
            raise NotImplementedError(
                "Writing to Iceberg tables through Gravitino requires PyIceberg catalog integration. "
                "This will be implemented in a future update."
            )
        elif format_type == "PARQUET":
            raise NotImplementedError(
                "Writing to Hive/Parquet tables through Gravitino is not yet supported. "
                "Note: This would not update the Hive metastore."
            )
        else:
            raise NotImplementedError(f"Writing {format_type} format tables is not yet supported")

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        """Overwrite the table with new data.

        Args:
            df: DataFrame to overwrite with
            **options: Format-specific write options

        Raises:
            NotImplementedError: Write operations are not yet supported
        """
        Table._validate_options("Gravitino write", options, GravitinoTable._write_options)

        format_type = self._detect_format()

        if format_type == "ICEBERG":
            raise NotImplementedError(
                "Writing to Iceberg tables through Gravitino requires PyIceberg catalog integration. "
                "This will be implemented in a future update."
            )
        elif format_type == "PARQUET":
            raise NotImplementedError(
                "Writing to Hive/Parquet tables through Gravitino is not yet supported. "
                "Note: This would not update the Hive metastore."
            )
        else:
            raise NotImplementedError(f"Writing {format_type} format tables is not yet supported")
