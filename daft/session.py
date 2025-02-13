from __future__ import annotations

from daft import DataFrame
from daft.catalog import Catalog, Namespace
from daft.daft import PySession
from daft.table import Source, Table

class Session:

    def __init__():
        raise NotImplementedError("We do not support creating a Session via __init__ ")

    ###
    # factory methods
    ###

    @staticmethod
    def empty() -> Session:
        s = Session.__new__(Session)
        s._session = PySession.empty()
        return s

    @staticmethod
    def _from_pysession(session: PySession) -> Session:
        s = Session.__new__(Session)
        s._session = session
        return s

    ###
    # exec
    ###

    def exec(self, input: str) -> DataFrame:
        return self._session.exec(input)

    ###
    # attach & detach
    ###

    def attach(self, name: str, catalog: Catalog):
        """Attaches the catalog to this session."""
        return self._session.attach(name, catalog)

    def detach(self, name: str):
        """Detaches the catalog from this session."""
        return self._session.detach(name)

    ###
    # create_*
    ###

    def create_catalog(self, name: str) -> Catalog:
        """Create a new catalog scoped to this session."""
        return self._session.create_catalog(name)

    def create_namespace(self, name: str) -> Namespace:
        """Create a new namespace scope to this session's current catalog."""
        return self._session.create_namespace(name)

    def create_table(self, name: str, source: Source = None) -> Table:
        """Creates a new table scoped to this session's current catalog and namespace."""
        return self._session.create_table(name, source)

    ###
    # session state
    ###

    def current_catalog(self) -> Catalog:
        """Returns the session's current catalog."""
        return self._session.current_catalog()

    def current_namespace(self) -> Namespace:
        """Returns the session's current namespace."""
        return self._session.current_namespace()

    ###
    # get_*
    ###

    def get_catalog(self, name: str) -> Catalog:
        """Returns the catalog from the global session, err if not exists."""
        return self._session.get_catalog(name)

    def get_namespace(self, name: str) -> Namespace:
        """Returns the namespace from the global session, err if not exists."""
        return self._session.get_namespace(name)

    def get_table(self, name: str) -> Table:
        """Returns the table from the global session, err if not exists."""
        return self._session.get_table(name)

    ###
    # list_*
    ###

    def list_catalogs(self, pattern: None | str = None) -> list[Catalog]:
        """Returns a list of available catalogs."""
        return self._session.list_catalogs(pattern)

    def list_namespaces(self, pattern: None | str = None) -> list[Namespace]:
        """Returns a list of available namespaces."""
        return self._session.list_namespaces(pattern)

    def list_tables(self, pattern: None | str = None) -> list[Table]:
        """Returns a list of tables matching the pattern."""
        return self._session.list_tables(pattern)

    ###
    # set_*
    ###

    def set_catalog(self, name: str):
        """Set the given catalog as current_catalog or err if not exists."""
        self._session.set_catalog(name)

    def set_namespace(self, name: str):
        """Set the given namespace as current_namespace or err if not exists."""
        self._session.set_namespace(name)
