"""WARNING! These APIs are internal; please use Catalog.from_iceberg() and Table.from_iceberg()."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyiceberg.catalog import Catalog as InnerCatalog
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.partitioning import PartitionField as PyIcebergPartitionField
from pyiceberg.partitioning import PartitionSpec as PyIcebergPartitionSpec
from pyiceberg.schema import Schema as PyIcebergSchema
from pyiceberg.schema import assign_fresh_schema_ids
from pyiceberg.table import Table as InnerTable
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    TruncateTransform,
    YearTransform,
)

from daft.catalog import Catalog, Identifier, NotFoundError, Properties, Schema, Table
from daft.io.iceberg._iceberg import read_iceberg

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.io.partitioning import PartitionField


class IcebergCatalog(Catalog):
    _inner: InnerCatalog

    def __init__(self) -> None:
        raise RuntimeError("IcebergCatalog.__init__ is not supported, please use `Catalog.from_iceberg` instead.")

    @staticmethod
    def _from_obj(obj: object) -> IcebergCatalog:
        """Returns an IcebergCatalog instance if the given object can be adapted so."""
        if isinstance(obj, InnerCatalog):
            c = IcebergCatalog.__new__(IcebergCatalog)
            c._inner = obj
            return c
        raise ValueError(f"Unsupported iceberg catalog type: {type(obj)}")

    @staticmethod
    def _load_catalog(name: str, **options: str | None) -> IcebergCatalog:
        c = IcebergCatalog.__new__(IcebergCatalog)
        c._inner = load_catalog(name, **options)
        return c

    @property
    def name(self) -> str:
        return self._inner.name

    @staticmethod
    def _partition_fields_to_pyiceberg_spec(
        iceberg_schema: PyIcebergSchema, partition_fields: list[PartitionField] | None
    ) -> PyIcebergPartitionSpec | None:
        """Converts Daft partition fields to a PyIceberg PartitionSpec."""
        if not partition_fields:
            return None

        # Convert Daft schema → PyArrow schema → PyIceberg schema (with IDs)
        iceberg_partition_fields = []
        for idx, pf in enumerate(partition_fields):
            if pf.transform is None or pf.transform.is_identity():
                transform = IdentityTransform()
            elif pf.transform.is_year():
                transform = YearTransform()
            elif pf.transform.is_month():
                transform = MonthTransform()
            elif pf.transform.is_day():
                transform = DayTransform()
            elif pf.transform.is_hour():
                transform = HourTransform()
            elif pf.transform.is_iceberg_bucket():
                transform = BucketTransform(num_buckets=pf.transform.num_buckets)
            elif pf.transform.is_iceberg_truncate():
                transform = TruncateTransform(width=pf.transform.width)
            else:
                raise NotImplementedError(f"Unsupported partition transform: {pf.transform}")

            source_field = iceberg_schema.find_field(pf.field.name)
            iceberg_partition_fields.append(
                PyIcebergPartitionField(
                    source_id=source_field.field_id,
                    field_id=1000 + idx,
                    transform=transform,
                    name=pf.field.name,
                )
            )
        return PyIcebergPartitionSpec(*iceberg_partition_fields)

    ###
    # create_*
    ###

    def _create_namespace(self, identifier: Identifier) -> None:
        ident = _to_pyiceberg_ident(identifier)
        self._inner.create_namespace(ident)

    def _create_table(
        self,
        identifier: Identifier,
        schema: Schema,
        properties: Properties | None = None,
        partition_fields: list[PartitionField] | None = None,
    ) -> Table:
        i = _to_pyiceberg_ident(identifier)
        pa_schema = schema.to_pyarrow_schema()
        iceberg_schema = assign_fresh_schema_ids(_pyarrow_to_schema_without_ids(pa_schema))
        partition_spec = self._partition_fields_to_pyiceberg_spec(iceberg_schema, partition_fields)
        t = IcebergTable.__new__(IcebergTable)
        if partition_spec is not None:
            t._inner = self._inner.create_table(
                i,
                schema=iceberg_schema,
                partition_spec=partition_spec,
            )
        else:
            t._inner = self._inner.create_table(
                i,
                schema=iceberg_schema,
            )
        return t

    ###
    # drop_*
    ###

    def _drop_namespace(self, identifier: Identifier) -> None:
        ident = _to_pyiceberg_ident(identifier)
        self._inner.drop_namespace(ident)

    def _drop_table(self, identifier: Identifier) -> None:
        ident = _to_pyiceberg_ident(identifier)
        self._inner.drop_table(ident)

    ###
    # has_*
    ###

    def _has_namespace(self, identifier: Identifier) -> bool:
        ident = _to_pyiceberg_ident(identifier)
        try:
            _ = self._inner.list_namespaces(ident)
            return True
        except NoSuchNamespaceError:
            return False

    def _has_table(self, identifier: Identifier) -> bool:
        ident = _to_pyiceberg_ident(identifier)
        try:
            # using load_table instead of table_exists because table_exists does not work with an instance of the `tabulario/iceberg-rest` Docker image
            self._inner.load_table(ident)
            return True
        except NoSuchTableError:
            return False

    ###
    # get_*
    ###

    def _get_table(self, identifier: Identifier) -> IcebergTable:
        ident = _to_pyiceberg_ident(identifier)
        try:
            return IcebergTable._from_obj(self._inner.load_table(ident))
        except NoSuchTableError as ex:
            # convert to not found because we want to (sometimes) ignore it internally
            raise NotFoundError() from ex
        except Exception as ex:
            # wrap original exceptions
            raise Exception("pyiceberg raised an exception while calling get_table") from ex

    ###
    # list_*
    ###

    def _list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        prefix = () if pattern is None else _to_pyiceberg_ident(pattern)
        return [Identifier(*tup) for tup in self._inner.list_namespaces(prefix)]

    def _list_tables(self, pattern: str | None = None) -> list[Identifier]:
        if pattern is None:
            tables = []
            for ns in self.list_namespaces():
                tables.extend(self._inner.list_tables(str(ns)))
        else:
            tables = self._inner.list_tables(pattern)
        return [Identifier(*tup) for tup in tables]


class IcebergTable(Table):
    _inner: InnerTable

    _read_options = {"snapshot_id"}
    _write_options: set[str] = set()

    def __init__(self) -> None:
        raise RuntimeError("IcebergTable.__init__ is not supported, please use `Table.from_iceberg` instead.")

    @property
    def name(self) -> str:
        return self._inner.name()[-1]

    def schema(self) -> Schema:
        return self.read().schema()

    @staticmethod
    def _from_obj(obj: object) -> IcebergTable:
        """Returns an IcebergTable if the given object can be adapted so."""
        if isinstance(obj, InnerTable):
            t = IcebergTable.__new__(IcebergTable)
            t._inner = obj
            return t
        raise ValueError(f"Unsupported iceberg table type: {type(obj)}")

    def read(self, **options: Any | None) -> DataFrame:
        Table._validate_options("Iceberg read", options, IcebergTable._read_options)
        return read_iceberg(self._inner, snapshot_id=options.get("snapshot_id"))

    def append(self, df: DataFrame, **options: Any) -> None:
        self._validate_options("Iceberg write", options, IcebergTable._write_options)

        df.write_iceberg(self._inner, mode="append")

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        self._validate_options("Iceberg write", options, IcebergTable._write_options)

        df.write_iceberg(self._inner, mode="overwrite")


def _to_pyiceberg_ident(ident: Identifier | str) -> tuple[str, ...] | str:
    return tuple(ident) if isinstance(ident, Identifier) else ident
