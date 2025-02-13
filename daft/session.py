from __future__ import annotations

from daft.context import DaftContext, get_context
from daft.dataframe import DataFrame
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

    def attach(self, catalog: Catalog, alias: str | None = None) -> Catalog:
        """Attaches the catalog to this session."""
        # once more catalogs are supported, then require explicit aliases
        if alias is None:
            alias = catalog.name()
        return self._session.attach(catalog, alias)

    def detach(self, catalog: str):
        """Detaches the catalog from this session."""
        return self._session.detach(catalog)

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
        """Returns the catalog or raises an exception if it does not exist."""
        return self._session.get_catalog(name)

    def get_namespace(self, name: str) -> Namespace:
        """Returns the namespace or raises an exception if it does not exist."""
        return self._session.get_namespace(name)

    def get_table(self, name: str) -> Table:
        """Returns the table ."""
        return self._session.get_table(name)

    ###
    # list_*
    ###

    def list_catalogs(self, pattern: None | str = None) -> list[Catalog]:
        """Returns a list of available catalogs."""
        return self._session.list_catalogs(pattern)

    ###
    # set_*
    ###

    def set_catalog(self, name: str):
        """Set the given catalog as current_catalog or err if not exists."""
        self._session.set_catalog(name)

    def set_namespace(self, name: str):
        """Set the given namespace as current_namespace or err if not exists."""
        self._session.set_namespace(name)

###
# global active session
###

_SESSION: Session | None = None

def _session() -> Session:
    # Consider registering into the global context
    # ```
    # ctx = get_context()
    # if not ctx._session
    #     set_session(Session.from_env())
    # return ctx._session
    # ```
    if not _SESSION:
        set_session(Session.empty())
    return _SESSION

###
# session state
###

def current_session() -> Session:
    """Returns the global context's current session."""
    return _session()

def current_catalog() -> Catalog:
    """Returns the global session's current catalog."""
    return _session().current_catalog()

###
# create_*
##

def create_catalog(name: str) -> Catalog:
    """Creates a catalog scoped to the global session."""
    return _session().create_catalog(name)

###
# set_.* (session management)
###

def set_session(session: Session):
    """Sets the global context's current session."""
    # Consider registering into the global context.
    # ```
    # ctx = get_context()
    # with ctx._lock:
    #     ctx._session = session
    # ```
    global _SESSION
    _SESSION = session

def set_catalog(name: str):
    """Sets the global session's current catalog."""
    _session().set_catalog(name)
