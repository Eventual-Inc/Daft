from __future__ import annotations

from daft import DataFrame

from denv.catalog import Catalog, Identifier, Schema
from denv.catalog import (
    create_catalog as _create_catalog,
)
from denv.table import Source, Table

# env variables
DAFT_SESSION = "default" # default | empty | none
DAFT_SESSION_USER = "daft"
DAFT_SESSION_DEFAULT_CATALOG = "daft"
DAFT_SESSION_DEFAULT_SCHEMA = "default"
DAFT_SESSION_TEMP_DIR = "/tmp"

# names are lvalue identifiers
Name = Identifier | str

class Session:

    # _instance: ClassVar[Session | None] = None
    # _lock: ClassVar[threading.Lock] = threading.Lock()

    # def __new__(cls):
    #     if cls._instance is None:
    #         with cls._lock:
    #             # check _instance is still None
    #             if not cls._instance:
    #                 cls._instance = super().__new__(cls)
    #     return cls._instance

    def __init__(self) -> None:
        # TODO raise NotImplementedError("Session __init__ is not supported, use create, default, or empty")
        self._catalogs: dict[str, Catalog] = dict()
        self._curr_catalog: str | None = None
        self._curr_schema: str | None = None
        self._path: str | None = None
        self._catalogs = dict()

    def __repr__(self) -> str:
        props = dict()
        props["current_catalog"] = self._curr_catalog
        props["current_schema"] = self._curr_schema
        return f"Session({props!r})"

    ###
    # factory methods
    ###

    @staticmethod
    def default():
        sess = Session.empty()
        sess.create_catalog(DAFT_SESSION_DEFAULT_CATALOG).create_schema(DAFT_SESSION_DEFAULT_SCHEMA)
        sess.set_catalog(DAFT_SESSION_DEFAULT_CATALOG)
        sess.set_schema(DAFT_SESSION_DEFAULT_SCHEMA)
        return sess

    @staticmethod
    def empty() -> Session:
        sess = Session.__new__(Session)
        sess.__init__()
        return sess

    ###
    # exec
    ###

    def exec(self, input: str) -> DataFrame:
        raise NotImplementedError("exec")

    ###
    # attach & detach
    ###

    def attach(self, name: str, catalog: Catalog):
        """Attaches the catalog to this session.
        
        Notes:
            - TODO iceberg support
            - TODO unity support
        """
        if name in self._catalogs:
            raise Exception(f"Catalog '{name}' already exists in this session")
        self._catalogs[name] = catalog

    def detach(self, name: str):
        """Detaches the named catalog from this session."""
        if name not in self._catalogs:
            raise Exception(f"Catalog '{name}' is not attached to this session")
        del self._catalogs[name]

    ###
    # create_*
    ###

    def create_catalog(self, name: str) -> Catalog:
        """Create a new catalog scoped to this session."""
        if name in self._catalogs:
            raise Exception(f"Catalog '{name}' already exists in this session")
        catalog = _create_catalog(name)
        if not self._catalogs:
            self._curr_catalog = name
        self._catalogs[name] = catalog
        return catalog

    def create_schema(self, name: str) -> Schema:
        return self.current_catalog().create_schema(name)

    def create_table(self, name: str, source: Source = None) -> Table:
        """Creates a new table scoped to this session.

        Notes:
            - TODO: support Name + Namespace, for now assume in default namespace
            - TODO: curr_namespc = session.current_namespace()

        SQL:
        ```sql
        CREATE TEMP TABLE [IF NOT EXISTS] <table> ( <schema>? );
        CREATE TEMP TABLE [IF NOT EXISTS] <table> AS <dataframe>;
        ```
        """
        return self.current_catalog().create_table(name, source)
    
    ###
    # session state
    ###

    def current_catalog(self) -> Catalog:
        """Returns the session's current catalog."""
        if not self._curr_catalog:
            raise Exception("Session has no current_catalog")
        return self._catalogs[self._curr_catalog]

    def current_schema(self) -> Schema:
        """Returns the session's current schema."""
        return self.current_catalog().get_schema(self._curr_schema)

    ###
    # get_*
    ###

    def get_catalog(self, name: str) -> Catalog:
        """Returns the catalog from the global session, err if not exists."""
        if name not in self._catalogs:
            raise Exception(f"Catalog '{name}' does not exist in this session.")
        return self._catalogs[name]

    def get_schema(self, name: str) -> Schema:
        """Returns the schema from the global session, err if not exists."""
        return self.current_catalog().get_schema(name)

    def get_table(self, name: str) -> Table:
        """Returns the table from the global session, err if not exists."""
        return self.current_schema().get_table(name)

    ###
    # list_*
    ###

    def list_catalogs(self, pattern: None | str = None) -> list[Catalog]:
        """Returns a list of available catalogs."""
        if pattern:
            raise NotImplementedError("list_catalogs pattern not supported")
        return list(self._catalogs.values())

    def list_schemas(self, pattern: None | str = None) -> list[Schema]:
        """Returns a list of available schemas."""
        if pattern:
            raise NotImplementedError("list_schemas pattern not supported")
        schemas = []
        for catalog in self._catalogs.values():
            schemas.extend(catalog.list_schemas(pattern))
        return schemas

    def list_tables(self, pattern: None | str = None) -> list[Table]:
        if pattern:
            raise NotImplementedError("list_tables pattern not supported")
        tables = []
        for schema in self.list_schemas(pattern):
            tables.extend(schema.list_tables(pattern))
        return tables

    ###
    # use_*
    ###

    def set_catalog(self, name: str):
        """Use the given catalog as current_catalog or err if not exists."""
        if name not in self._catalogs:
            raise Exception(f"Catalog '{name}' does not exist in this session.")
        self._curr_catalog = name

    def set_schema(self, name: str):
        """Use the given schema as current_schema or err if not exists."""
        if not self.current_catalog().has_schema(name):
            raise Exception(f"Schema '{name}' does not exist in catalog '${self._curr_catalog}'.")
        self._curr_schema = name

# global session variable, access via _session()
_SESSION: Session | None = Session.default() if DAFT_SESSION else None

# global session or err if not exists
def _session() -> Session:
    if not _SESSION:
        raise Exception("This feature requires DAFT_SESSION=true")
    return _SESSION

def current_session() -> Session:
    """Returns the global session."""
    return _session()

def create_session(impl: str, **options) -> Session:
    """Creates a new session from the given options."""
    raise NotImplementedError("create_session")

###
# attach & detach
##

def attach(name: str, catalog: Catalog):
    _session().attach(name, catalog)

def detach(name: str):
    _session().detach()

###
# create_*
##

def create_catalog(name: str) -> Catalog:
    """Creates a catalog scoped to the global session."""
    return _session().create_catalog(name)

def create_schema(name: str) -> Schema:
    """Creates a schema scoped to the global session."""
    return _session().create_schema(name)

def create_table(name: str, source: Schema | DataFrame | str | None = None) -> Table:
    """Creates a table scoped to the global session."""
    return _session().create_table(name, source)

###
# session state
###

def current_catalog() -> Catalog:
    """Returns the global session's current catalog."""
    return _session().current_catalog()

def current_schema() -> Schema:
    """Returns the global session's current schema."""
    return _session().current_schema()

###
# get_.*
###

def get_catalog(name: str) -> Catalog:
    """Returns the catalog from the global session, err if not exists."""
    return _session().get_catalog(name)

def get_schema(name: str) -> Schema:
    """Returns the schema from the global session, err if not exists."""
    return _session().get_schema(name)

def get_table(name: str) -> Table:
    """Returns the table from the global session, err if not exists."""
    return _session().get_table(name)

###
# list_.*
###

def list_catalogs(pattern: None | str = None) -> list[Catalog]:
    """Returns a list of available catalogs in the global session."""
    return _session().list_catalogs(pattern)

def list_schemas(pattern: None | str = None) -> list[Schema]:
    """Returns a list of available schema in the global session."""
    return _session().list_schemas(pattern)

def list_tables(pattern: None | str = None) -> list[Table]:
    """Returns a list of available tables in the global session."""
    return _session().list_tables(pattern)

###
# set_.* (session management)
###

def set_catalog(name: str):
    """Use the given catalog as the global session's current_catalog."""
    _session().set_catalog(name)

def set_schema(name: str):
    """Use the given schema as the global session's current_schema."""
    _session().set_schema(name)
