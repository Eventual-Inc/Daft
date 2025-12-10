"""WARNING! These APIs are internal; please use Table.from_postgres()."""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import psycopg
from pgvector.psycopg import register_vector

from daft.catalog import Catalog, Identifier, NotFoundError, Properties, Schema, Table
from daft.datatype import DataType
from daft.io._sql import read_sql
from daft.logical.schema import Field


@contextmanager
def postgres_connection(connection_string: str, extensions: list[str] | None) -> psycopg.Connection.connect:
    """Context manager that provides a PostgreSQL connection with specified extensions setup.

    Args:
        connection_string: PostgreSQL connection string
        extensions: List of extension names to create if they don't exist and are available.
                   For each extension, availability is checked in pg_available_extensions before
                   attempting "CREATE EXTENSION IF NOT EXISTS <extension>".
    """
    with psycopg.connect(connection_string) as conn:
        if extensions:
            with conn.cursor() as cur:
                for extension in extensions:
                    # Check if extension is available before attempting to create it
                    cur.execute(
                        psycopg.sql.SQL("SELECT EXISTS(SELECT 1 FROM pg_available_extensions WHERE name = {})").format(
                            psycopg.sql.Literal(extension)
                        )
                    )
                    result = cur.fetchone()
                    is_available = result[0] if result else False

                    if is_available:
                        cur.execute(
                            psycopg.sql.SQL("CREATE EXTENSION IF NOT EXISTS {}").format(
                                psycopg.sql.Identifier(extension)
                            )
                        )

                # Register pgvector type if it was successfully created
                if "vector" in extensions:
                    cur.execute(psycopg.sql.SQL("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'vector')"))
                    result = cur.fetchone()
                    vector_installed = result[0] if result else False

                    if vector_installed:
                        register_vector(conn)
        yield conn


if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.io.partitioning import PartitionField


def _daft_dtype_to_postgres_type(dtype: DataType, set_dimensions: bool = True) -> str:
    """Convert a Daft DataType to a PostgreSQL type string."""
    if dtype.is_int8():
        return "smallint"  # PostgreSQL doesn't have TINYINT, use SMALLINT
    elif dtype.is_int16():
        return "smallint"
    elif dtype.is_int32():
        return "integer"
    elif dtype.is_int64():
        return "bigint"
    elif dtype.is_uint8():
        return "smallint"  # PostgreSQL doesn't have unsigned types
    elif dtype.is_uint16():
        return "integer"  # PostgreSQL doesn't have unsigned types
    elif dtype.is_uint32():
        return "bigint"  # PostgreSQL doesn't have unsigned types
    elif dtype.is_uint64():
        return "bigint"  # PostgreSQL doesn't have unsigned types, may overflow
    elif dtype.is_float32():
        return "real"
    elif dtype.is_float64():
        return "double precision"
    elif dtype.is_boolean():
        return "boolean"
    elif dtype.is_string():
        return "text"
    elif dtype.is_binary() or dtype.is_fixed_size_binary():
        return "bytea"
    elif dtype.is_date():
        return "date"
    elif dtype.is_timestamp():
        # PostgreSQL timestamps support timezone
        try:
            timezone = dtype.timezone
            if timezone:
                return "timestamptz"
        except AttributeError:
            pass
        return "timestamp"
    elif dtype.is_time():
        return "time"
    # TODO(desmond): Daft is currently unable to read INTERVAL types via read_sql()
    # elif dtype.is_duration():
    #     return "interval"
    # TODO(desmond): Daft INTERVAL can't currently be casted correctly into timedelta objects.
    # elif dtype.is_interval():
    #     return "interval"
    elif dtype.is_list() or dtype.is_fixed_size_list():
        # TODO(desmond): We need to rework array types a little.
        # PostgreSQL supports multi-dimensional arrays and arrays of fixed sizes. However they're all
        # treated equally as a flat array and are simply documentation.
        try:
            inner_dtype = dtype.dtype
            inner_type = _daft_dtype_to_postgres_type(inner_dtype)
            # If inner type contains a complex type, simply use JSONB.
            if "[]" in inner_type or inner_type == "jsonb":
                return "jsonb"
            else:
                return f"{inner_type}[]"
        except AttributeError:
            pass
        return "jsonb"  # Fallback for unknown inner types
    elif dtype.is_struct():
        # TODO(desmond): PostgreSQL doesn't support ROW syntax in column definitions, we'd need to define new composite types.
        # Use JSONB for now.
        return "jsonb"
    elif dtype.is_map():
        return "jsonb"
    elif dtype.is_extension():
        return "jsonb"
    elif dtype.is_image() or dtype.is_fixed_shape_image():
        # TODO(desmond): Under the hood Daft uses structs for images. We should update this as we update the struct story.
        return "jsonb"
    elif dtype.is_embedding():
        # Use pgvector VECTOR type for embeddings.
        if set_dimensions:
            return f"vector({dtype.size})"
        else:
            return "vector"
    elif (
        dtype.is_tensor()
        or dtype.is_fixed_shape_tensor()
        or dtype.is_sparse_tensor()
        or dtype.is_fixed_shape_sparse_tensor()
    ):
        # TODO(desmond): Tensors are multidimensional arrays. We should update this as we update the story for lists.
        return "jsonb"
    elif dtype.is_python():
        return "jsonb"
    elif dtype.is_decimal128():
        # PostgreSQL supports decimal/numeric without parameters, or with precision,scale
        # But psycopg type registry may not know about parameterized versions
        return "numeric"
    elif dtype.is_null():
        return "text"  # Default to TEXT for null type
    else:
        return "jsonb"


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
    _extensions: list[str] | None  # extensions to create
    _create_table_options = {
        "enable_rls",
    }

    def __init__(self) -> None:
        raise RuntimeError("PostgresCatalog.__init__ is not supported, please use `Catalog.from_postgres` instead.")

    @staticmethod
    def from_uri(uri: str, extensions: list[str] | None, **options: str | None) -> PostgresCatalog:
        """Create a PostgresCatalog from a connection string."""
        validate_connection_string(uri)
        c = PostgresCatalog.__new__(PostgresCatalog)
        c._inner = uri
        c._extensions = extensions
        # TODO(desmond): Handle options.s
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

        with postgres_connection(self._inner, self._extensions) as conn:
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
            properties (Properties): The properties of the table to create. One supported property is "enable_rls" (bool), which enables Row Level Security. This property is set to True by default. See: https://www.postgresql.org/docs/current/ddl-rowsecurity.html
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

        with postgres_connection(self._inner, self._extensions) as conn:
            with conn.cursor() as cur:
                try:
                    if quoted_schema:
                        cur.execute(psycopg.sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(quoted_schema))

                    cur.execute(
                        psycopg.sql.SQL("CREATE TABLE {} ({})").format(
                            quoted_full_table, psycopg.sql.SQL(", ").join(column_defs)
                        )
                    )

                    if properties is None or properties.get("enable_rls", True):
                        cur.execute(
                            psycopg.sql.SQL("ALTER TABLE {} ENABLE ROW LEVEL SECURITY").format(quoted_full_table)
                        )

                    conn.commit()
                except psycopg.errors.DuplicateTable:
                    raise ValueError(f"Table {identifier} already exists")
                except psycopg.Error as e:
                    raise ValueError(f"Failed to create table {identifier}: {e}") from e

        return PostgresTable._from_catalog(self._inner, identifier, self._extensions)

    ###
    # drop_*
    ###

    def _drop_namespace(self, identifier: Identifier) -> None:
        """Drop a schema from PostgreSQL."""
        if len(identifier) != 1:
            raise ValueError(f"PostgreSQL namespace identifier must be a single schema name, got {identifier}")

        quoted_schema = psycopg.sql.Identifier(identifier[0])

        with postgres_connection(self._inner, self._extensions) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(psycopg.sql.SQL("DROP SCHEMA {}").format(quoted_schema))
                    conn.commit()
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

        with postgres_connection(self._inner, self._extensions) as conn:
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

        return PostgresTable._from_catalog(self._inner, identifier, self._extensions)

    ###
    # has_*
    ###

    def _has_namespace(self, identifier: Identifier) -> bool:
        """Check if a schema exists in PostgreSQL."""
        if len(identifier) != 1:
            raise ValueError(f"PostgreSQL schema identifier must be a single schema name, got {identifier}")

        quoted_schema = psycopg.sql.Literal(identifier[0])

        with postgres_connection(self._inner, self._extensions) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    psycopg.sql.SQL(
                        "select exists(SELECT 1 FROM information_schema.schemata WHERE schema_name = {})"
                    ).format(quoted_schema)
                )
                result = cur.fetchone()
                return result[0] if result else False

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

        with postgres_connection(self._inner, self._extensions) as conn:
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
                return result[0] if result else False

    ###
    # list_*
    ###

    def _list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        """List schemas in PostgreSQL."""
        with postgres_connection(self._inner, self._extensions) as conn:
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
        with postgres_connection(self._inner, self._extensions) as conn:
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
    _extensions: list[str] | None
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

    @classmethod
    def _from_catalog(
        cls, connection_string: str, identifier: Identifier, extensions: list[str] | None
    ) -> PostgresTable:
        """Create a PostgresTable from catalog connection details."""
        t = cls.__new__(cls)
        t._inner = (connection_string, identifier)
        t._extensions = extensions
        return t

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
        connection_string, identifier = self._inner

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
            raise ValueError(f"Invalid table identifier: {identifier}")

        # Query the database schema to get column information
        with postgres_connection(connection_string, self._extensions) as conn:
            with conn.cursor() as cur:
                if schema_name:
                    cur.execute(
                        psycopg.sql.SQL("""
                        SELECT
                            c.column_name,
                            c.data_type,
                            c.udt_name,
                            CASE
                                WHEN c.data_type = 'USER-DEFINED' AND c.udt_name = 'vector'
                                THEN a.atttypmod
                                ELSE NULL
                            END as vector_dimension
                        FROM information_schema.columns c
                        JOIN pg_class cls ON cls.relname = c.table_name
                        JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace AND nsp.nspname = c.table_schema
                        LEFT JOIN pg_attribute a ON a.attrelid = cls.oid AND a.attname = c.column_name
                        WHERE c.table_schema = {} AND c.table_name = {}
                        ORDER BY c.ordinal_position
                        """).format(psycopg.sql.Literal(schema_name), psycopg.sql.Literal(table_name)),
                    )
                else:
                    cur.execute(
                        psycopg.sql.SQL("""
                        SELECT
                            c.column_name,
                            c.data_type,
                            c.udt_name,
                            CASE
                                WHEN c.data_type = 'USER-DEFINED' AND c.udt_name = 'vector'
                                THEN a.atttypmod
                                ELSE NULL
                            END as vector_dimension
                        FROM information_schema.columns c
                        JOIN pg_class cls ON cls.relname = c.table_name
                        LEFT JOIN pg_attribute a ON a.attrelid = cls.oid AND a.attname = c.column_name
                        WHERE c.table_name = {}
                        ORDER BY c.ordinal_position
                        """).format(psycopg.sql.Literal(table_name)),
                    )

                columns = cur.fetchall()

        # If no columns found, fall back to data-based inference
        if not columns:
            return self.read().schema()

        # Build schema from database metadata
        fields = []
        for column_name, data_type, udt_name, vector_dimension in columns:
            if data_type == "USER-DEFINED" and udt_name == "vector":
                # This is a pgvector column, convert to embedding type
                # vector_dimension from atttypmod contains the dimension information
                # For pgvector, atttypmod stores the dimension directly
                dimension = vector_dimension if vector_dimension and vector_dimension > 0 else 0

                if dimension > 0:
                    fields.append(Field.create(column_name, DataType.embedding(DataType.float32(), dimension)))
                else:
                    # Fallback to list if we can't determine dimension
                    fields.append(Field.create(column_name, DataType.list(DataType.float32())))
            else:
                # For non-vector columns, try direct PostgreSQL type mapping first
                try:
                    # Attempt to map PostgreSQL type directly to Daft type
                    inferred_dtype = DataType.from_sql(data_type)
                    fields.append(Field.create(column_name, inferred_dtype))
                except Exception:
                    # Fall back to data-based inference for unmappable types
                    # This is inefficient but ensures we get the correct types
                    if schema_name:
                        single_col_query = psycopg.sql.SQL("SELECT {} FROM {}.{} LIMIT 1").format(
                            psycopg.sql.Identifier(column_name),
                            psycopg.sql.Identifier(schema_name),
                            psycopg.sql.Identifier(table_name),
                        )
                    else:
                        single_col_query = psycopg.sql.SQL("SELECT {} FROM {} LIMIT 1").format(
                            psycopg.sql.Identifier(column_name),
                            psycopg.sql.Identifier(table_name),
                        )

                    try:
                        single_col_df = read_sql(single_col_query.as_string(), connection_string)
                        inferred_dtype = single_col_df.schema()[column_name].dtype
                        fields.append(Field.create(column_name, inferred_dtype))
                    except Exception:
                        # If inference fails, use string as fallback
                        fields.append(Field.create(column_name, DataType.string()))

        return Schema._from_fields(fields)

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

        df = read_sql(
            query.as_string(),
            connection_string,
            **options,
        )

        # Cast any vector columns that were read as lists to embeddings
        schema = self.schema()  # Use our custom schema method
        for field in schema:
            if field.dtype.is_embedding():
                df = df.with_column(field.name, df[field.name].cast(field.dtype))

        return df

    def append(self, df: DataFrame, **options: Any) -> None:
        """Append the DataFrame to the table."""
        connection_string, identifier = self._inner

        if len(identifier) == 1:
            # When no schema is specified, PostgreSQL uses the schema search path to select the schema to use.
            # Since this is user-configurable, we simply pass along the single identifier to PostgreSQL.
            # See: https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH
            full_table_name = psycopg.sql.Identifier(identifier[0])
        elif len(identifier) == 2:
            quoted_schema = psycopg.sql.Identifier(identifier[0])
            quoted_table = psycopg.sql.Identifier(identifier[1])
            full_table_name = psycopg.sql.SQL(".").join([quoted_schema, quoted_table])
        else:
            raise ValueError(f"Invalid table identifier: {identifier}")

        # Build column list for COPY statement.
        columns_str = psycopg.sql.SQL(", ").join(psycopg.sql.Identifier(field.name) for field in df.schema())

        # Build types to pass into the COPY statement.
        types_to_set = [_daft_dtype_to_postgres_type(field.dtype, set_dimensions=False) for field in df.schema()]

        copy_sql = psycopg.sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT BINARY)").format(full_table_name, columns_str)

        # TODO(desmond): This writes results sequentially on a single node. We should replace
        # this with DataFrame.write_sql() once we merge pull request #5471.
        # Additionally, although ADBC currently has limited type support, it might be worth
        # exploring for bulk loading.
        with postgres_connection(connection_string, self._extensions) as conn:
            with conn.cursor() as cur:
                with cur.copy(copy_sql) as copy:
                    copy.set_types(types_to_set)

                    for batch in df.to_arrow_iter():
                        for row in batch.to_pylist():
                            values = list(row.values())
                            # Serialize complex types that are mapped to 'text' to JSON strings for binary COPY
                            serialized_values = []
                            for value, postgres_type in zip(values, types_to_set):
                                if postgres_type in ("text", "jsonb") and not isinstance(
                                    value, (str, int, float, bool, type(None))
                                ):
                                    # Complex types (lists, dicts, etc.) need to be JSON serialized.
                                    serialized_values.append(json.dumps(value))
                                else:
                                    serialized_values.append(value)
                            copy.write_row(serialized_values)
            conn.commit()

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        """Overwrite the table with the DataFrame."""
        connection_string, identifier = self._inner

        temp_catalog = PostgresCatalog.__new__(PostgresCatalog)
        temp_catalog._inner = connection_string
        temp_catalog._extensions = self._extensions

        table_exists = temp_catalog._has_table(identifier)

        if table_exists:
            temp_catalog._drop_table(identifier)

        # Extract enable_rls from options if provided, defaulting to True.
        enable_rls = options.get("enable_rls", True) if options else True
        properties = {"enable_rls": enable_rls}
        temp_catalog._create_table(identifier, df.schema(), properties=properties)
        # Now append the data (since table is empty, this effectively overwrites).
        self.append(df, **options)
        return
