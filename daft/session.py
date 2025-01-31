from __future__ import annotations

import threading
from typing import ClassVar, Dict

from daft import Catalog
from daft.catalog.catalog import Table
from daft.dataframe.dataframe import DataFrame
from daft.logical.schema import Schema

"""
BACKLOG
------------------------------------------
- make environment variables
- create_table_if_not_exists
- def use(self, identifier: str):
- def use_schema(self, schema: str):
- def use_namespace(self):
"""

"""Global session for the current python environment
------------------------------------------
DAFT_SESSION_USER
DAFT_SESSION_DEFAULT_CATALOG
DAFT_SESSION_DEFAULT_NAMESPACE
DAFT_SESSION_TEMP_DIR
"""
_DAFT_DEFAULT_CATALOG: str = "default"


class Session:
    # SESSION STATE
    _catalogs: Dict[str, Catalog]
    _curr_catalog: str

    # THREAD SAFE SINGLETON
    _instance: ClassVar[Session | None] = None
    _lock: ClassVar[threading.Lock] = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                # check _instance is still None
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        self._catalogs = dict()
        self.create_catalog(_DAFT_DEFAULT_CATALOG)
        self.use_catalog(_DAFT_DEFAULT_CATALOG)

    def __repr__(self) -> str:
        props = dict()
        props["current_catalog"] = self._curr_catalog
        return f"session({props!r})"

    def create_catalog(self, name: str) -> Catalog:
        """Create a new catalog and attach to this session."""
        catalog = Catalog(name)
        self._catalogs[name] = catalog
        return catalog

    def current_catalog(self) -> Catalog:
        """Returns the session's current catalog."""
        return self._catalogs[self._curr_catalog]

    def use_catalog(self, catalog: str):
        """Use the provided catalog or err if not exists."""
        if catalog not in self._catalogs:
            raise ValueError(f"Catalog {catalog} does not exist in the session")
        self._curr_catalog = catalog


# consider tempdir and salt
_Session = Session()


def create_catalog(name: str) -> Catalog:
    """Creates a new catalog scoped to the current session."""
    return _Session.create_catalog(name)


def create_table(name: str, source: Schema | DataFrame) -> Table:
    """Creates a new table scoped to the current session.

    SQL:
        - CREATE TEMP TABLE [IF NOT EXISTS] <table> AS <dataframe>
        - CREATE TEMP TABLE [IF NOT EXISTS] <table> ( <schema> )

    TODOs:
        - support Name + Namespace, for now assume in default namespace
        - curr_namespc = session.current_namespace()

    Args:
        name: Table name
        source: Table source (schema or dataframe)

    Returns:
        Table: the newly created table
    """
    return _Session.current_catalog().create_table(name, source)


def current_catalog() -> Catalog:
    """Returns the current catalog"""
    return _Session.current_catalog()


def current_session() -> Session:
    """Returns the current session"""
    return _Session
