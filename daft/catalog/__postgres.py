"""WARNING! These APIs are internal; please use Table.from_postgres()."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import adbc_driver_postgresql.dbapi as adbc_dbapi
import psycopg

from daft.catalog import Catalog, Identifier, NotFoundError, Properties, Schema, Table
from daft.io._sql import read_sql

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.datatype import DataType
    from daft.io.partitioning import PartitionField


def _daft_dtype_to_postgres_type(dtype: DataType) -> str:
    """Convert a Daft DataType to a PostgreSQL type string."""
    if dtype.is_int8():
        return "SMALLINT"  # PostgreSQL doesn't have TINYINT, use SMALLINT
    elif dtype.is_int16():
        return "SMALLINT"
    elif dtype.is_int32():
        return "INTEGER"
    elif dtype.is_int64():
        return "BIGINT"
    elif dtype.is_uint8():
        return "SMALLINT"  # PostgreSQL doesn't have unsigned types
    elif dtype.is_uint16():
        return "INTEGER"  # PostgreSQL doesn't have unsigned types
    elif dtype.is_uint32():
        return "BIGINT"  # PostgreSQL doesn't have unsigned types
    elif dtype.is_uint64():
        return "BIGINT"  # PostgreSQL doesn't have unsigned types, may overflow
    elif dtype.is_float32():
        return "REAL"
    elif dtype.is_float64():
        return "DOUBLE PRECISION"
    elif dtype.is_boolean():
        return "BOOLEAN"
    elif dtype.is_string():
        return "TEXT"
    elif dtype.is_binary() or dtype.is_fixed_size_binary():
        return "BYTEA"
    elif dtype.is_date():
        return "DATE"
    elif dtype.is_timestamp():
        # PostgreSQL timestamps support timezone
        try:
            timezone = dtype.timezone
            if timezone:
                return "TIMESTAMPTZ"
        except AttributeError:
            pass
        return "TIMESTAMP"
    elif dtype.is_time():
        return "TIME"
    # TODO(desmond): Add support for the following types:
    # is_duration
    # is_interval
    # is_list
    # is_fixed_size_list
    # is_struct
    # is_map
    # is_extension
    # is_image
    # is_fixed_shape_image
    # is_embedding
    # is_tensor
    # is_fixed_shape_tensor
    # is_sparse_tensor
    # is_fixed_shape_sparse_tensor
    # is_python
    elif dtype.is_decimal128():
        try:
            precision = dtype.precision
            scale = dtype.scale
            return f"DECIMAL({precision},{scale})"
        except AttributeError:
            return "DECIMAL(38,18)"  # Default precision and scale
    elif dtype.is_null():
        return "TEXT"  # Default to TEXT for null type
    else:
        return "JSONB"


def validate_connection_string(conn_string: str) -> None:
    try:
        # This will raise psycopg.ProgrammingError if invalid. See: https://www.psycopg.org/psycopg3/docs/api/conninfo.html#conninfo-manipulate-connection-strings
        psycopg.conninfo.make_conninfo(conn_string)
        return
    except psycopg.ProgrammingError as e:
        raise ValueError(f"Invalid connection string: {e}")
    except Exception as e:
        raise ValueError(f"Unexpected error: {e}")


class PostgresCatalog(Catalog):
    # TODO(desmond): For now we can create connections as needed, but in the future we can create lazy connections
    # and connection pools as an optimization.
    _inner: str  # connection string
    _create_table_options = {
        "enable_rls",
    }

    def __init__(self) -> None:
        raise RuntimeError("PostgresCatalog.__init__ is not supported, please use `Catalog.from_postgres` instead.")

    @staticmethod
    def _from_obj(obj: object) -> PostgresCatalog:
        """Returns a PostgresCatalog instance if the given object can be adapted so."""
        if isinstance(obj, str):
            # Connection string
            validate_connection_string(obj)
            c = PostgresCatalog.__new__(PostgresCatalog)
            c._inner = obj
            return c
        # TODO(desmond): We could also potentially support psycopg connection objects here.
        raise ValueError(f"Unsupported postgres catalog type: {type(obj)}")

    @staticmethod
    def _load_catalog(name: str, **options: str | None) -> PostgresCatalog:
        """Load a PostgresCatalog from a connection string."""
        c = PostgresCatalog._from_obj(name)
        # TODO(desmond): Handle options.
        return c

    @property
    def name(self) -> str:
        try:
            return psycopg.conninfo.conninfo_to_dict(self._inner)["dbname"]
        except Exception as e:
            raise ValueError(f"Error getting database name from connection string: {e}")

    ###
    # create_*
    ###

    def _create_namespace(self, identifier: Identifier) -> None:
        """Create a schema in PostgreSQL."""
        if len(identifier) != 1:
            raise ValueError(f"PostgreSQL schema identifier must be a single schema name, got {identifier}")

        quoted_schema = psycopg.sql.Identifier(identifier[0])

        with psycopg.connect(self._inner) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(psycopg.sql.SQL("CREATE SCHEMA {}").format(quoted_schema))
                    conn.commit()
                except psycopg.errors.DuplicateSchema:
                    raise ValueError(f"Schema {identifier} already exists")
                except psycopg.Error as e:
                    raise ValueError(f"Failed to create schema {identifier}: {e}") from e

    def _create_table(
        self,
        identifier: Identifier,
        schema: Schema,
        properties: Properties | None = None,
        partition_fields: list[PartitionField] | None = None,
    ) -> Table:
        """Create a table in PostgreSQL.

        Args:
            identifier (Identifier): The identifier of the table to create.
            schema (Schema): The schema of the table to create.
            properties (Properties): The properties of the table to create. One supported property is "enable_rls" (bool), which enables Row Level Security by default. See: https://www.postgresql.org/docs/current/ddl-rowsecurity.html
            partition_fields (list[PartitionField]): The partition fields of the table to create.

        Returns:
            The table that was created.
        """
        if properties:
            self._validate_options("Postgres create_table", properties, PostgresCatalog._create_table_options)
        if len(identifier) == 1:
            # When no schema is specified, PostgreSQL uses the schema search path to select the schema to use.
            # Since this is user-configurable, we simply pass along the single identifier to PostgreSQL.
            # See: https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH
            schema_name = None
            table_name = identifier[0]
        elif len(identifier) == 2:
            schema_name = identifier[0]
            table_name = identifier[1]
        else:
            raise ValueError(f"PostgreSQL table identifier must be 'schema.table' or 'table', got {identifier}")

        # Build column definitions from Daft schema.
        column_defs = []
        for field in schema:
            field_name = field.name
            field_type = _daft_dtype_to_postgres_type(field.dtype)
            quoted_name = psycopg.sql.Identifier(field_name)
            column_defs.append(psycopg.sql.SQL("{} {}").format(quoted_name, psycopg.sql.SQL(field_type)))

        quoted_schema = psycopg.sql.Identifier(schema_name) if schema_name else None
        quoted_table = psycopg.sql.Identifier(table_name)
        quoted_full_table = psycopg.sql.SQL(".").join([quoted_schema, quoted_table]) if schema_name else quoted_table

        with psycopg.connect(self._inner) as conn:
            with conn.cursor() as cur:
                try:
                    if quoted_schema:
                        cur.execute(psycopg.sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(quoted_schema))

                    cur.execute(
                        psycopg.sql.SQL("CREATE TABLE {} ({})").format(
                            quoted_full_table, psycopg.sql.SQL(", ").join(column_defs)
                        )
                    )

                    if properties and properties.get("enable_rls", False):
                        cur.execute(
                            psycopg.sql.SQL("ALTER TABLE {} ENABLE ROW LEVEL SECURITY").format(quoted_full_table)
                        )

                    conn.commit()
                except psycopg.errors.DuplicateTable:
                    raise ValueError(f"Table {identifier} already exists")
                except psycopg.Error as e:
                    raise ValueError(f"Failed to create table {identifier}: {e}") from e

        t = PostgresTable.__new__(PostgresTable)
        t._inner = (self._inner, identifier)
        return t

    ###
    # drop_*
    ###

    def _drop_namespace(self, identifier: Identifier) -> None:
        """Drop a schema from PostgreSQL."""
        if len(identifier) != 1:
            raise ValueError(f"PostgreSQL namespace identifier must be a single schema name, got {identifier}")

        quoted_schema = psycopg.sql.Identifier(identifier[0])

        with psycopg.connect(self._inner) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(psycopg.sql.SQL("DROP SCHEMA {}").format(quoted_schema))
                    conn.commit()
                except psycopg.errors.UndefinedObject:
                    raise NotFoundError(f"Namespace {identifier} not found")
                except psycopg.Error as e:
                    raise ValueError(f"Failed to drop namespace {identifier}: {e}") from e

    def _drop_table(self, identifier: Identifier) -> None:
        """Drop a table from PostgreSQL."""
        if len(identifier) == 1:
            # When no schema is specified, PostgreSQL uses the schema search path to select the schema to use.
            # Since this is user-configurable, we simply pass along the single identifier to PostgreSQL.
            # See: https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH
            table_name = identifier[0]
            quoted_full_table = psycopg.sql.Identifier(table_name)
        elif len(identifier) == 2:
            schema_name = identifier[0]
            table_name = identifier[1]

            quoted_schema = psycopg.sql.Identifier(schema_name)
            quoted_table = psycopg.sql.Identifier(table_name)
            quoted_full_table = psycopg.sql.SQL(".").join([quoted_schema, quoted_table])
        else:
            raise ValueError(f"PostgreSQL table identifier must be 'schema.table' or 'table', got {identifier}")

        with psycopg.connect(self._inner) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(psycopg.sql.SQL("DROP TABLE {}").format(quoted_full_table))
                    conn.commit()
                except psycopg.errors.UndefinedTable:
                    raise NotFoundError(f"Table {identifier} not found")
                except psycopg.Error as e:
                    raise ValueError(f"Failed to drop table {identifier}: {e}") from e

    ###
    # get_*
    ###

    def _get_table(self, identifier: Identifier) -> Table:
        """Get a table from PostgreSQL."""
        if not self._has_table(identifier):
            raise NotFoundError(f"Table {identifier} not found")

        t = PostgresTable.__new__(PostgresTable)
        t._inner = (self._inner, identifier)
        return t

    ###
    # has_*
    ###

    def _has_namespace(self, identifier: Identifier) -> bool:
        """Check if a schema exists in PostgreSQL."""
        if len(identifier) != 1:
            raise ValueError(f"PostgreSQL schema identifier must be a single schema name, got {identifier}")

        quoted_schema = psycopg.sql.Literal(identifier[0])

        with psycopg.connect(self._inner) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    psycopg.sql.SQL(
                        "select exists(SELECT 1 FROM information_schema.schemata WHERE schema_name = {})"
                    ).format(quoted_schema)
                )
                result = cur.fetchone()
                return result is not None

    def _has_table(self, identifier: Identifier) -> bool:
        """Check if a table exists in PostgreSQL."""
        if len(identifier) == 1:
            # When no schema is specified, PostgreSQL uses the schema search path to select the schema to use.
            # Since this is user-configurable, we simply pass along the single identifier to PostgreSQL.
            # See: https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH
            schema_name = None
            table_name = identifier[0]
        elif len(identifier) == 2:
            schema_name = identifier[0]
            table_name = identifier[1]
        else:
            return False

        with psycopg.connect(self._inner) as conn:
            with conn.cursor() as cur:
                if schema_name:
                    cur.execute(
                        psycopg.sql.SQL(
                            "select exists(SELECT 1 FROM information_schema.tables WHERE table_schema = {} AND table_name = {})"
                        ).format(psycopg.sql.Literal(schema_name), psycopg.sql.Literal(table_name))
                    )
                else:
                    cur.execute(
                        psycopg.sql.SQL(
                            "select exists(SELECT 1 FROM information_schema.tables WHERE table_name = {})"
                        ).format(psycopg.sql.Literal(table_name))
                    )
                result = cur.fetchone()
                return result is not None

    ###
    # list_*
    ###

    def _list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        """List schemas in PostgreSQL."""
        with psycopg.connect(self._inner) as conn:
            with conn.cursor() as cur:
                if pattern:
                    cur.execute(
                        psycopg.sql.SQL(
                            "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE {} ORDER BY schema_name"
                        ).format(psycopg.sql.Literal(pattern + "%"))
                    )
                else:
                    cur.execute(psycopg.sql.SQL("SELECT schema_name FROM information_schema.schemata"))
                results = cur.fetchall()
                return [Identifier(row[0]) for row in results]

    def _list_tables(self, pattern: str | None = None) -> list[Identifier]:
        """List tables in PostgreSQL."""
        with psycopg.connect(self._inner) as conn:
            with conn.cursor() as cur:
                if pattern:
                    cur.execute(
                        psycopg.sql.SQL(
                            "SELECT table_schema, table_name FROM information_schema.tables WHERE table_name LIKE {} ORDER BY table_schema, table_name"
                        ).format(psycopg.sql.Literal(pattern + "%"))
                    )
                else:
                    cur.execute(psycopg.sql.SQL("SELECT table_schema, table_name FROM information_schema.tables"))
                results = cur.fetchall()
                return [Identifier(row[0], row[1]) for row in results]


class PostgresTable(Table):
    _inner: tuple[str, Identifier]  # (connection_string, identifier)
    _read_options = {
        "partition_col",
        "num_partitions",
        "partition_bound_strategy",
        "disable_pushdowns_to_sql",
        "infer_schema",
        "infer_schema_length",
        "schema",
    }

    def __init__(self) -> None:
        raise RuntimeError("PostgresTable.__init__ is not supported, please use `Table.from_postgres` instead.")

    @property
    def name(self) -> str:
        """Returns the table's name."""
        _, identifier = self._inner
        if len(identifier) == 1:
            # When no schema is specified, PostgreSQL uses the schema search path to select the schema to use.
            # Since this is user-configurable, we simply pass along the single identifier to PostgreSQL.
            # See: https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH
            return identifier[0]
        elif len(identifier) == 2:
            return identifier[1]
        else:
            raise ValueError(f"Invalid table identifier: {identifier}")

    def schema(self) -> Schema:
        """Returns the table's schema."""
        return self.read().schema()

    @staticmethod
    def _from_obj(obj: object) -> PostgresTable:
        """Returns a PostgresTable if the given object can be adapted so."""
        raise ValueError(f"Unsupported postgres table type: {type(obj)}")

    def read(
        self,
        **options: Any,
    ) -> DataFrame:
        """Read the table as a DataFrame."""
        self._validate_options("Postgres read", options, PostgresTable._read_options)

        connection_string, identifier = self._inner

        if len(identifier) == 1:
            # When no schema is specified, PostgreSQL uses the schema search path to select the schema to use.
            # Since this is user-configurable, we simply pass along the single identifier to PostgreSQL.
            # See: https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH
            quoted_full_table = psycopg.sql.Identifier(identifier[0])
        elif len(identifier) == 2:
            quoted_schema = psycopg.sql.Identifier(identifier[0])
            quoted_table = psycopg.sql.Identifier(identifier[1])
            quoted_full_table = psycopg.sql.SQL(".").join([quoted_schema, quoted_table])
        else:
            raise ValueError(f"Invalid table identifier: {identifier}")

        query = psycopg.sql.SQL("SELECT * FROM {}").format(quoted_full_table)

        return read_sql(
            query.as_string(),
            connection_string,
            **options,
        )

    def append(self, df: DataFrame, **options: Any) -> None:
        """Append the DataFrame to the table."""
        connection_string, identifier = self._inner

        if len(identifier) == 1:
            # When no schema is specified, PostgreSQL uses the schema search path to select the schema to use.
            # Since this is user-configurable, we simply pass along the single identifier to PostgreSQL.
            # See: https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH
            table_name = identifier[0]
            db_schema_name = None
        elif len(identifier) == 2:
            db_schema_name = identifier[0]
            table_name = identifier[1]
        else:
            raise ValueError(f"Invalid table identifier: {identifier}")

        # TODO(desmond): This writes results sequentially on a single node. We should replace
        # this with DataFrame.write_sql() once we merge pull request #5471.
        with adbc_dbapi.connect(connection_string) as conn:
            with conn.cursor() as cur:
                for record_batch in df.to_arrow_iter():
                    cur.adbc_ingest(table_name, record_batch, mode="create_append", db_schema_name=db_schema_name)
            conn.commit()

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        """Overwrite the table with the DataFrame."""
        connection_string, identifier = self._inner

        if len(identifier) == 1:
            # When no schema is specified, PostgreSQL uses the schema search path to select the schema to use.
            # Since this is user-configurable, we simply pass along the single identifier to PostgreSQL.
            # See: https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH
            table_name = identifier[0]
            db_schema_name = None
        elif len(identifier) == 2:
            db_schema_name = identifier[0]
            table_name = identifier[1]
        else:
            raise ValueError(f"Invalid table identifier: {identifier}")

        # TODO(desmond): This writes results sequentially on a single node. We should replace
        # this with DataFrame.write_sql() once we merge pull request #5471.
        with adbc_dbapi.connect(connection_string) as conn:
            with conn.cursor() as cur:
                first_batch = True
                for record_batch in df.to_arrow_iter():
                    mode = "replace" if first_batch else "append"
                    cur.adbc_ingest(table_name, record_batch, mode=mode, db_schema_name=db_schema_name)
                    first_batch = False
            conn.commit()
