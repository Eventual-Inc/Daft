from daft.catalog import Catalog, Identifier, Table
from daft.daft import PyCatalog as _PyCatalog
from daft.daft import PyTable as _PyTable
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder


class _RustCatalog(Catalog):
    """Shim to wrap PyCatalog and subclass Catalog.

    This should not be used directly, but instead be subclassed for each Rust catalog.
    """

    def __init__(self, inner: _PyCatalog):
        self.inner = inner

    @property
    def name(self):
        return self.inner.name()

    def _create_namespace(self, ident):
        self.inner.create_namespace(ident._ident)

    def _has_namespace(self, ident):
        return self.inner.has_namespace(ident._ident)

    def _drop_namespace(self, ident):
        self.inner.drop_namespace(ident._ident)

    def _list_namespaces(self, pattern=None):
        return [Identifier._from_pyidentifier(ident) for ident in self.inner.list_namespaces(pattern)]

    def _create_table(self, ident):
        return self.inner.create_table(ident._ident)

    def _has_table(self, ident):
        return self.inner.has_table(ident._ident)

    def _drop_table(self, ident):
        self.inner.drop_table(ident._ident)

    def _list_tables(self, pattern=None):
        return [Identifier._from_pyidentifier(ident) for ident in self.inner.list_tables(pattern)]

    def _get_table(self, ident):
        return self.inner.get_table(ident._ident)


class _RustTable(Table):
    """Shim to wrap PyTable and subclass Table.

    This should not be used directly, but instead be subclassed for each Rust table.
    """

    def __init__(self, inner: _PyTable):
        self.inner = inner

    @property
    def name(self):
        return self.inner.name()

    def read(self, **options):
        return DataFrame(LogicalPlanBuilder(self.inner.to_logical_plan()))

    def write(self, df, mode="append", **options):
        self.inner.write(df._builder._builder, mode)


class View(_RustTable):
    pass
