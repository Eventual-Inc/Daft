from __future__ import annotations

from typing import TYPE_CHECKING

from daft.catalog import Catalog, Identifier, Table, TableSource
from daft.daft import PySession

if TYPE_CHECKING:
    from daft.dataframe import DataFrame


__all__ = [
    "Session",
    "attach_catalog",
    "attach_table",
    "create_temp_table",
    "current_catalog",
    "current_session",
    "detach_catalog",
    "detach_table",
    "get_catalog",
    "get_table",
    "has_catalog",
    "has_table",
    "list_catalogs",
    "list_tables",
    "read_table",
    "set_catalog",
    "set_session",
]


class Session:
    """Session holds a connection's state and orchestrates execution of DataFrame and SQL queries against catalogs."""

    _session: PySession

    def __init__(self):
        self._session = PySession.empty()

    ###
    # factory methods
    ###

    @staticmethod
    def _from_pysession(session: PySession) -> Session:
        """Creates a session from a rust session wrapper."""
        s = Session.__new__(Session)
        s._session = session
        return s

    @staticmethod
    def _from_env() -> Session:
        """Creates a session from the environment's configuration."""
        # todo session builders, raise if DAFT_SESSION=0
        return Session()

    ###
    # attach & detach
    ###

    def attach_catalog(self, catalog: object | Catalog, alias: str | None = None) -> Catalog:
        """Attaches an external catalog to this session."""
        if alias is None:
            raise ValueError("implicit catalog aliases are not yet supported")
        c = catalog if isinstance(catalog, Catalog) else Catalog._from_obj(catalog)
        return self._session.attach_catalog(c, alias)

    def attach_table(self, table: object | Table, alias: str | None = None) -> Table:
        """Attaches an external table to this session."""
        if alias is None:
            raise ValueError("implicit table aliases are not yet supported")
        t = table if isinstance(table, Table) else Table._from_obj(table)
        return self._session.attach_table(t, alias)

    def detach_catalog(self, alias: str):
        """Detaches the catalog from this session."""
        return self._session.detach_catalog(alias)

    def detach_table(self, alias: str):
        """Detaches the table from this session."""
        return self._session.detach_table(alias)

    ###
    # create_*
    ###

    def create_temp_table(self, name: str, source: object | TableSource = None) -> Table:
        """Creates a temp table scoped to this session's lifetime."""
        s = source if isinstance(source, TableSource) else TableSource._from_obj(source)
        return self._session.create_temp_table(name, s._source, replace=True)

    ###
    # session state
    ###

    def current_catalog(self) -> Catalog:
        """Returns the session's current catalog."""
        return self._session.current_catalog()

    ###
    # get_*
    ###

    def get_catalog(self, name: str) -> Catalog:
        """Returns the catalog or raises an exception if it does not exist."""
        return self._session.get_catalog(name)

    def get_table(self, name: str | Identifier) -> Table:
        """Returns the table or raises an exception if it does not exist."""
        if isinstance(name, str):
            name = Identifier(*name.split("."))
        return self._session.get_table(name._identifier)

    ###
    # has_*
    ###

    def has_catalog(self, name: str) -> bool:
        """Returns true if a catalog with the given name exists."""
        return self._session.has_catalog(name)

    def has_table(self, name: str | Identifier) -> bool:
        """Returns true if a table with the given name exists."""
        if isinstance(name, str):
            name = Identifier.from_str(name)
        return self._session.has_table(name._identifier)

    ###
    # list_*
    ###

    def list_catalogs(self, pattern: None | str = None) -> list[str]:
        """Returns a list of available catalogs."""
        return self._session.list_catalogs(pattern)

    def list_tables(self, pattern: None | str = None) -> list[Identifier]:
        """Returns a list of available tables."""
        return [Identifier._from_pyidentifier(i) for i in self._session.list_tables(pattern)]

    ###
    # read_*
    ###

    def read_table(self, name: str | Identifier) -> DataFrame:
        """Returns the table as a DataFrame or raises an exception if it does not exist."""
        return self.get_table(name).read()

    ###
    # set_*
    ###

    def set_catalog(self, name: str):
        """Set the given catalog as current_catalog or err if not exists."""
        self._session.set_catalog(name)


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
    global _SESSION
    if not _SESSION:
        _SESSION = Session._from_env()
    return _SESSION


###
# attach & detach
###


def attach_catalog(catalog: object | Catalog, alias: str | None = None) -> Catalog:
    """Attaches an external catalog to the current session."""
    return _session().attach_catalog(catalog, alias)


def attach_table(table: object | Table, alias: str | None = None) -> Table:
    """Attaches an external table to the current session."""
    return _session().attach_table(table, alias)


def detach_catalog(alias: str):
    """Detaches the catalog from the current session."""
    return _session().detach_catalog(alias)


def detach_table(alias: str):
    """Detaches the table from the current session."""
    return _session().detach_table(alias)


###
# create_*
###


def create_temp_table(self, name: str, source: object | TableSource = None) -> Table:
    """Creates a temp table scoped to current session's lifetime."""
    return _session().create_temp_table(name, source)


###
# session state
###


def current_catalog() -> Catalog:
    """Returns the session's current catalog."""
    return _session().current_catalog()


def current_session() -> Session:
    """Returns the global context's current session."""
    return _session()


###
# get_*
###


def get_catalog(name: str) -> Catalog:
    """Returns the catalog from the current session or raises an exception if it does not exist."""
    return _session().get_catalog(name)


def get_table(name: str | Identifier) -> Table:
    """Returns the table from the current session or raises an exception if it does not exist."""
    return _session().get_table(name)


###
# has_*
###


def has_catalog(name: str) -> bool:
    """Returns true if a catalog with the given name exists in the current session."""
    return _session().has_catalog(name)


def has_table(name: str | Identifier) -> bool:
    """Returns true if a table with the given name exists in the current session."""
    return _session().has_table(name)


###
# list_*
###


def list_catalogs(pattern: None | str = None) -> list[str]:
    """Returns a list of available catalogs in the current session."""
    return _session().list_catalogs(pattern)


def list_tables(pattern: None | str = None) -> list[Identifier]:
    """Returns a list of available tables in the current session."""
    return _session().list_tables(pattern)


###
# read_*
###


def read_table(name: str | Identifier) -> DataFrame:
    """Returns the table as a DataFrame or raises an exception if it does not exist."""
    return _session().get_table(name).read()


###
# set_*
###


def set_catalog(name: str):
    """Set the given catalog as current_catalog for the current session or err if not exists."""
    _session().set_catalog(name)


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
