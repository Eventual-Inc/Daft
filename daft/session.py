from __future__ import annotations

from typing import Any, Literal

from daft.catalog import Catalog, Identifier, Table
from daft.context import get_context
from daft.daft import LogicalPlanBuilder as PyBuilder
from daft.daft import PySession, PyTableSource, sql_exec
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder
from daft.logical.schema import Schema
from daft.udf import UDF

__all__ = [
    "Session",
    "attach",
    "attach_catalog",
    "attach_function",
    "attach_table",
    "create_namespace",
    "create_namespace_if_not_exists",
    "create_table",
    "create_table_if_not_exists",
    "create_temp_table",
    "current_catalog",
    "current_namespace",
    "current_session",
    "detach_catalog",
    "detach_function",
    "detach_table",
    "drop_namespace",
    "drop_table",
    "get_catalog",
    "get_table",
    "has_catalog",
    "has_namespace",
    "has_table",
    "list_catalogs",
    "list_namespaces",
    "list_tables",
    "read_table",
    "set_catalog",
    "set_namespace",
    "set_session",
    "write_table",
]


class Session:
    """Session holds a connection's state and orchestrates execution of DataFrame and SQL queries against catalogs.

    Examples:
        >>> import daft
        >>> from daft.session import Session
        >>>
        >>> sess = Session()
        >>>
        >>> # Create a temporary table from a DataFrame
        >>> sess.create_temp_table("T", daft.from_pydict({"x": [1, 2, 3]}))
        >>>
        >>> # Read the table as a DataFrame
        >>> df = sess.read_table("T")
        >>>
        >>> # Execute an SQL query
        >>> sess.sql("SELECT * FROM T").show()
        >>>
        >>> # You can also retrieve the current session without creating a new one:
        >>> from daft.session import current_session
        >>> sess = current_session()
    """

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
    # exec
    ###

    def sql(self, sql: str) -> DataFrame | None:
        """Executes the SQL statement using this session.

        Args:
            sql (str): input SQL statement

        Returns:
            DataFrame: dataframe instance if this was a data statement (DQL, DDL, DML).
        """
        py_sess = self._session
        py_config = get_context().daft_planning_config
        py_object = sql_exec(sql, py_sess, py_config)
        if py_object is None:
            return None
        elif isinstance(py_object, PyBuilder):
            return DataFrame(LogicalPlanBuilder(py_object))
        else:
            raise ValueError(f"Unsupported return type from sql exec: {type(py_object)}")

    ###
    # attach & detach
    ###

    def attach(self, object: Catalog | Table | UDF, alias: str | None = None) -> None:
        """Attaches a known attachable object like a Catalog, Table or UDF.

        Args:
            object (Catalog|Table|UDF): object which is attachable to a session

        Returns:
            None
        """
        if isinstance(object, Catalog):
            self.attach_catalog(object, alias)
        elif isinstance(object, Table):
            self.attach_table(object, alias)
        elif isinstance(object, UDF):
            self.attach_function(object, alias)
        else:
            raise ValueError(f"Cannot attach object with type {type(object)}")

    def attach_catalog(self, catalog: Catalog | object, alias: str | None = None) -> Catalog:
        """Attaches an external catalog to this session.

        Args:
            catalog (object): catalog instance or supported catalog object
            alias (str|None): optional alias for name resolution

        Returns:
            Catalog: new daft catalog instance
        """
        c = catalog if isinstance(catalog, Catalog) else Catalog._from_obj(catalog)
        a = alias if alias else c.name
        return self._session.attach_catalog(c, a)

    def attach_table(self, table: Table | object, alias: str | None = None) -> Table:
        """Attaches an external table instance to this session.

        Args:
            table (Table | object): table instance or supported table object
            alias (str | None): optional alias for name resolution

        Returns:
            Table: new daft table instance
        """
        t = table if isinstance(table, Table) else Table._from_obj(table)
        a = alias if alias else t.name
        return self._session.attach_table(t, a)

    def detach_catalog(self, alias: str):
        """Detaches the catalog from this session or raises if the catalog does not exist.

        Args:
            alias (str): catalog alias to detach

        Returns:
            None
        """
        return self._session.detach_catalog(alias)

    def detach_table(self, alias: str):
        """Detaches the table from this session or raises if the table does not exist.

        Args:
            alias (str): catalog alias to detach

        Returns:
            None
        """
        return self._session.detach_table(alias)

    ###
    # create_*
    ###

    def create_namespace(self, identifier: Identifier | str):
        """Creates a namespace in the current catalog."""
        if not (catalog := self.current_catalog()):
            raise ValueError("Cannot create a namespace without a current catalog")
        return catalog.create_namespace(identifier)

    def create_namespace_if_not_exists(self, identifier: Identifier | str):
        """Creates a namespace in the current catalog if it does not already exist."""
        if not (catalog := self.current_catalog()):
            raise ValueError("Cannot create a namespace without a current catalog")
        return catalog.create_namespace_if_not_exists(identifier)

    def create_table(self, identifier: Identifier | str, source: Schema | DataFrame, **properties: Any) -> Table:
        """Creates a table in the current catalog.

        If no namespace is specified, the current namespace is used.

        Returns:
            Table: the newly created table instance.
        """
        if not (catalog := self.current_catalog()):
            # TODO relax this constraint by joining with the catalog name
            raise ValueError("Cannot create a table without a current catalog")

        if isinstance(identifier, str):
            identifier = Identifier.from_str(identifier)

        if len(identifier) == 1:
            if ns := self.current_namespace():
                identifier = ns + identifier

        return catalog.create_table(identifier, source, properties)

    def create_table_if_not_exists(
        self,
        identifier: Identifier | str,
        source: Schema | DataFrame,
        **properties: Any,
    ) -> Table:
        """Creates a table in the current catalog if it does not already exist.

        If no namespace is specified, the current namespace is used.

        Returns:
            Table: the newly created instance, or the existing table instance.
        """
        if not (catalog := self.current_catalog()):
            # TODO relax this constraint by joining with the catalog name
            raise ValueError("Cannot create a table without a current catalog")

        if isinstance(identifier, str):
            identifier = Identifier.from_str(identifier)

        if len(identifier) == 1:
            if ns := self.current_namespace():
                identifier = ns + identifier

        return catalog.create_table_if_not_exists(identifier, source, properties)

    def create_temp_table(self, identifier: str, source: Schema | DataFrame) -> Table:
        """Creates a temp table scoped to this session's lifetime.

        Args:
            identifier (str): table identifier (name)
            source (TableSource|object): table source like a schema or dataframe

        Returns:
            Table: new table instance

        Examples:
            >>> import daft
            >>> from daft.session import Session
            >>> sess = Session()
            >>> sess.create_temp_table("T", daft.from_pydict({"x": [1, 2, 3]}))
            >>> sess.create_temp_table("S", daft.from_pydict({"y": [4, 5, 6]}))
            >>> sess.list_tables()
            [Identifier(''T''), Identifier(''S'')]

        Args:
            identifier (str): table identifier (name)
            source (Schema | DataFrame): table source is either a Schema or Dataframe

        Returns:
            Table: new table instance
        """
        if isinstance(source, Schema):
            py_source = PyTableSource.from_pyschema(source._schema)
        elif isinstance(source, DataFrame):
            py_source = PyTableSource.from_pybuilder(source._builder._builder)
        else:
            raise ValueError(
                f"Unsupported create_temp_table source, {type(source)}, expected either Schema or DataFrame."
            )
        return self._session.create_temp_table(identifier, py_source, replace=True)

    ###
    # drop_*
    ###

    def drop_namespace(self, identifier: Identifier | str):
        """Drop the given namespace in the current catalog.

        Args:
            identifier (Identifier|str): table identifier

        Returns:
            None
        """
        if not (catalog := self.current_catalog()):
            raise ValueError("Cannot drop a namespace without a current catalog")
        return catalog.drop_namespace(identifier)

    def drop_table(self, identifier: Identifier | str):
        """Drop the given table in the current catalog.

        Args:
            identifier (Identifier|str): table identifier

        Returns:
            None
        """
        if not (catalog := self.current_catalog()):
            raise ValueError("Cannot drop a table without a current catalog")
        # TODO join the identifier with the current namespace
        return catalog.drop_table(identifier)

    ###
    # session state
    ###

    def use(self, identifier: Identifier | str | None = None):
        """Use sets the current catalog and namespace."""
        if identifier is None:
            self.set_catalog(None)
            self.set_namespace(None)
            return
        if isinstance(identifier, str):
            identifier = Identifier.from_str(identifier)
        if len(identifier) == 1:
            self.set_catalog(str(identifier[0]))
        else:
            self.set_catalog(str(identifier[0]))
            self.set_namespace(identifier.drop(1))

    def current_catalog(self) -> Catalog | None:
        """Get the session's current catalog or None.

        Args:
            None

        Returns:
            Catalog: current catalog or None if one is not set
        """
        return self._session.current_catalog()

    def current_namespace(self) -> Identifier | None:
        """Get the session's current namespace or None.

        Args:
            None

        Returns:
            Identifier: current namespace or none if one is not set
        """
        ident = self._session.current_namespace()
        return Identifier._from_pyidentifier(ident) if ident else None

    ###
    # get_*
    ###

    def get_catalog(self, identifier: str) -> Catalog:
        """Returns the catalog or raises an exception if it does not exist.

        Args:
            identifier (str): catalog identifier (name)

        Returns:
            Catalog: The catalog object.

        Raises:
            ValueError: If the catalog does not exist.
        """
        return self._session.get_catalog(identifier)

    def get_table(self, identifier: Identifier | str) -> Table:
        """Returns the table or raises an exception if it does not exist.

        Args:
            identifier (Identifier|str): table identifier or identifier string

        Returns:
            Table: The table object.

        Raises:
            ValueError: If the table does not exist.
        """
        if isinstance(identifier, str):
            identifier = Identifier.from_str(identifier)
        return self._session.get_table(identifier._ident)

    ###
    # has_*
    ###

    def has_catalog(self, identifier: str) -> bool:
        """Returns true if a catalog with the given identifier exists."""
        return self._session.has_catalog(identifier)

    def has_namespace(self, identifier: Identifier | str) -> bool:
        """Returns true if a namespace with the given identifier exists."""
        if not (catalog := self.current_catalog()):
            raise ValueError("Cannot call has_namespace without a current catalog")
        return catalog.has_namespace(identifier)

    def has_table(self, identifier: Identifier | str) -> bool:
        """Returns true if a table with the given identifier exists."""
        if isinstance(identifier, str):
            identifier = Identifier.from_str(identifier)
        return self._session.has_table(identifier._ident)

    ###
    # list_*
    ###

    def list_catalogs(self, pattern: str | None = None) -> list[str]:
        """Returns a list of available catalogs matching the pattern.

        This API currently returns a list of catalog names for backwards compatibility.
        In 0.5.0 this API will return a list of Catalog objects.

        Args:
            pattern (str): catalog name pattern

        Returns:
            list[str]: list of available catalog names
        """
        return self._session.list_catalogs(pattern)

    def list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        """Returns a list of matching namespaces in the current catalog."""
        if not (catalog := self.current_catalog()):
            raise ValueError("Cannot list namespaces without a current catalog")
        return catalog.list_namespaces(pattern)

    def list_tables(self, pattern: str | None = None) -> list[Identifier]:
        """Returns a list of available tables.

        Args:
            pattern (str): table name pattern

        Returns:
            list[Identifier]: list of available tables
        """
        return [Identifier._from_pyidentifier(i) for i in self._session.list_tables(pattern)]

    ###
    # read_*
    ###

    def read_table(self, identifier: Identifier | str, **options) -> DataFrame:
        """Returns the table as a DataFrame or raises an exception if it does not exist.

        Args:
            identifier (Identifier|str): table identifier

        Returns:
            DataFrame:

        Raises:
            ValueError: If the tables does not exist.
        """
        return self.get_table(identifier).read(**options)

    ###
    # set_*
    ###

    def set_catalog(self, identifier: str | None):
        """Set the given catalog as current_catalog or raises an err if it does not exist.

        Args:
            identifier (str): sets the current catalog

        Returns:
            None

        Raises:
            ValueError: If the catalog does not exist.
        """
        self._session.set_catalog(identifier)

    def set_namespace(self, identifier: Identifier | str | None):
        """Set the given namespace as current_namespace for table resolution.

        Args:
            identifier (Identifier | str): namespace identifier
        """
        if isinstance(identifier, str):
            identifier = Identifier.from_str(identifier)
        self._session.set_namespace(identifier._ident if identifier else None)

    ###
    # write_*
    ###

    def write_table(
        self,
        identifier: Identifier | str,
        df: DataFrame,
        mode: Literal["append", "overwrite"] = "append",
        **options,
    ):
        """Writes the DataFrame to the table specified by the identifier.

        Args:
            identifier (Identifier|str): table identifier
            df (DataFrame): dataframe to write
            mode ("append"|"overwrite"): write mode, defaults to "append"
            options**: additional, format-specific write options

        Returns:
            None
        """
        if isinstance(identifier, str):
            identifier = Identifier.from_str(identifier)

        self._session.get_table(identifier._ident).write(df, mode=mode, **options)

    ###
    # functions
    ###
    def attach_function(self, function: UDF, alias: str | None = None):
        """Attaches a Python function as a UDF in the current session."""
        self._session.attach_function(function, alias)

    def detach_function(self, alias: str):
        """Detaches a Python function as a UDF in the current session."""
        self._session.detach_function(alias)


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


def attach(object: Catalog | Table | UDF, alias: str | None = None) -> None:
    """Attaches a known attachable object like a Catalog or Table."""
    return _session().attach(object, alias)


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


def create_namespace(identifier: Identifier | str):
    """Creates a namespace in the current session's active catalog."""
    return _session().create_namespace(identifier)


def create_namespace_if_not_exists(identifier: Identifier | str):
    """Creates a namespace in the current session's active catalog if it does not already exist."""
    return _session().create_namespace_if_not_exists(identifier)


def create_table(identifier: Identifier | str, source: Schema | DataFrame, **properties: Any) -> Table:
    """Creates a table in the current session's active catalog and namespace."""
    return _session().create_table(identifier, source, **properties)


def create_table_if_not_exists(identifier: Identifier | str, source: Schema | DataFrame, **properties: Any) -> Table:
    """Creates a table in the current session's active catalog and namespace if it does not already exist."""
    return _session().create_table_if_not_exists(identifier, source, **properties)


def create_temp_table(identifier: str, source: Schema | DataFrame) -> Table:
    """Creates a temp table scoped to current session's lifetime."""
    return _session().create_temp_table(identifier, source)


###
# drop_*
###


def drop_namespace(identifier: Identifier | str):
    """Drops the namespace in the current session's active catalog."""
    return _session().drop_namespace(identifier)


def drop_table(identifier: Identifier | str):
    """Drops the table in the current session's active catalog."""
    return _session().drop_namespace(identifier)


###
# session state
###


def current_catalog() -> Catalog | None:
    """Returns the session's current catalog or None."""
    return _session().current_catalog()


def current_namespace() -> Identifier | None:
    """Returns the session's current namespace or None."""
    return _session().current_namespace()


def current_session() -> Session:
    """Returns the global context's current session."""
    return _session()


###
# get_*
###


def get_catalog(identifier: str) -> Catalog:
    """Returns the catalog from the current session or raises an exception if it does not exist."""
    return _session().get_catalog(identifier)


def get_table(identifier: Identifier | str) -> Table:
    """Returns the table from the current session or raises an exception if it does not exist."""
    return _session().get_table(identifier)


###
# has_*
###


def has_catalog(identifier: str) -> bool:
    """Returns true if a catalog with the given identifier exists in the current session."""
    return _session().has_catalog(identifier)


def has_namespace(identifier: Identifier | str) -> bool:
    """Returns true if a namespace with the given identifier exists in the current session."""
    return _session().has_namespace(identifier)


def has_table(identifier: Identifier | str) -> bool:
    """Returns true if a table with the given identifier exists in the current session."""
    return _session().has_table(identifier)


###
# list_*
###


def list_catalogs(pattern: str | None = None) -> list[str]:
    """Returns a list of available catalogs in the current session."""
    return _session().list_catalogs(pattern)


def list_namespaces(pattern: str | None = None) -> list[Identifier]:
    """Returns a list of matching namespaces in the current catalog."""
    return _session().list_namespaces(pattern)


def list_tables(pattern: str | None = None) -> list[Identifier]:
    """Returns a list of available tables in the current session."""
    return _session().list_tables(pattern)


###
# read_*
###


def read_table(identifier: Identifier | str, **options) -> DataFrame:
    """Returns the table as a DataFrame or raises an exception if it does not exist."""
    return _session().read_table(identifier, **options)


###
# write_*
###


def write_table(
    identifier: Identifier | str,
    df: DataFrame,
    mode: Literal["append", "overwrite"] = "append",
    **options,
):
    """Writes the DataFrame to the table specified with the identifier."""
    _session().write_table(identifier, df, mode, **options)


###
# set_*
###


def set_catalog(identifier: str | None):
    """Set the given catalog as current_catalog for the current session or raises an if it does not exist."""
    _session().set_catalog(identifier)


def set_namespace(identifier: Identifier | str | None):
    """Set the given namespace as current_namespace for the current session."""
    _session().set_namespace(identifier)


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


###
# functions
###


def attach_function(function: UDF, alias: str | None = None):
    """Attaches a Python function as a UDF in the current session."""
    _session().attach_function(function, alias)


def detach_function(alias: str):
    """Detaches a Python function as a UDF in the current session."""
    _session().detach_function(alias)
