"""WARNING! These APIs are internal; please use Catalog.from_hologres()."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import psycopg

from daft.catalog import (
    Catalog,
    Function,
    Identifier,
    NotFoundError,
    Properties,
    Schema,
    Table,
    TableAlreadyExistsError,
)
from daft.io._hologres._ddl import (
    build_column_defs,
    build_create_schema_sql,
    build_create_table_sql,
    build_drop_schema_sql,
    build_drop_table_sql,
    build_fulltext_index_statements,
    build_table_property_statements,
)
from daft.io._hologres._schema_loader import HologresTableSchema, load_table_schema, table_exists

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from daft.dataframe import DataFrame
    from daft.io.partitioning import PartitionField

# Schemas that belong to Hologres/PostgreSQL internals; hidden from listings.
_SYSTEM_SCHEMAS = ("information_schema", "pg_catalog", "pg_toast", "hologres", "hologres_sample", "hologres_stage")


def validate_connection_string(conn_string: str) -> None:
    """Validate a PostgreSQL connection string (Hologres uses the PG protocol)."""
    try:
        psycopg.conninfo.make_conninfo(conn_string)
        return
    except psycopg.ProgrammingError as e:
        raise ValueError(f"Invalid connection string: {e}")
    except Exception as e:
        raise ValueError(f"Unexpected error: {e}")


@contextmanager
def hologres_connection(connection_string: str) -> Generator[psycopg.Connection, None, None]:
    """Context manager yielding an autocommit Hologres connection.

    Hologres supports neither multi-statement DML transactions nor mixing DDL
    and DML in one transaction, so every connection runs in autocommit mode
    and statements commit immediately. The session timezone is pinned to UTC
    for deterministic timestamp handling.
    """
    conn = psycopg.connect(connection_string, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SET timezone TO 'UTC'")
        yield conn
    finally:
        conn.close()


def _resolve_identifier(identifier: Identifier, what: str) -> tuple[str | None, str]:
    """Split a 1- or 2-part identifier into (schema_name, table_name)."""
    if len(identifier) == 1:
        # No schema given: let the connection's search path resolve it.
        return None, identifier[0]
    elif len(identifier) == 2:
        return identifier[0], identifier[1]
    else:
        raise ValueError(f"Hologres {what} identifier must be 'schema.table' or 'table', got {identifier}")


def _resolve_table_schema(
    conn: psycopg.Connection,
    identifier: Identifier,
) -> tuple[str, str]:
    """Resolve an identifier to the concrete ``(schema, table)`` it refers to.

    Unqualified names follow the connection search path, matching PostgreSQL's
    own resolution rules. This prevents a bare ``get_table("events")`` from
    reading one schema while ``append``/``overwrite`` write to ``public``.
    """
    schema_name, table_name = _resolve_identifier(identifier, "table")
    if schema_name is not None:
        if not table_exists(conn, schema_name, table_name):
            raise NotFoundError(f"Table {identifier} not found")
        return schema_name, table_name

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.nspname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = %s
              AND c.relkind IN ('r', 'p')
              AND pg_table_is_visible(c.oid)
            ORDER BY array_position(current_schemas(false), n.nspname)
            LIMIT 1
            """,
            (table_name,),
        )
        row = cur.fetchone()
    if row is None:
        raise NotFoundError(f"Table {identifier} not found")
    return row[0], table_name


class HologresCatalog(Catalog):
    """A Hologres catalog backed by a PostgreSQL connection string."""

    _inner: str  # connection string

    _create_table_options = {
        "primary_key",
        "partition_by",
        "distribution_key",
        "clustering_key",
        "orientation",
        "time_to_live_in_days",
        "time_to_live_in_seconds",
        "bitmap_columns",
        "dictionary_encoding",
        "index",
        "proxima_vectors",
        "table_properties",
    }

    def __init__(self) -> None:
        raise RuntimeError("HologresCatalog.__init__ is not supported, please use `Catalog.from_hologres` instead.")

    @staticmethod
    def from_uri(uri: str, **options: str | None) -> HologresCatalog:
        """Create a HologresCatalog from a PostgreSQL connection string."""
        validate_connection_string(uri)
        c = HologresCatalog.__new__(HologresCatalog)
        c._inner = uri
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

    def _create_function(self, ident: Identifier, function: Function | Callable[..., Any]) -> None:
        raise NotImplementedError("Hologres does not support function registration.")

    def _create_namespace(self, identifier: Identifier) -> None:
        """Create a schema in Hologres."""
        if len(identifier) != 1:
            raise ValueError(f"Hologres schema identifier must be a single schema name, got {identifier}")

        with hologres_connection(self._inner) as conn, conn.cursor() as cur:
            try:
                cur.execute(build_create_schema_sql(identifier[0]))
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
        """Create a table in Hologres.

        Args:
            identifier (Identifier): The identifier of the table to create.
            schema (Schema): The schema of the table to create.
            properties (Properties): Table options, see
                ``HologresCatalog._create_table_options``.
            partition_fields (list[PartitionField]): Identity partition fields
                are translated to a logical partition table (``PARTITION BY
                LIST``). An explicit ``partition_by`` option takes precedence.

        Returns:
            The table that was created.
        """
        if properties:
            self._validate_options("Hologres create_table", properties, HologresCatalog._create_table_options)
        schema_name, table_name = _resolve_identifier(identifier, "table")

        options: dict[str, Any] = dict(properties) if properties else {}

        # Logical partitioning: explicit option wins over identity partition fields.
        partition_by = options.get("partition_by")
        if partition_by is None and partition_fields:
            identity = [f for f in partition_fields if f.transform is None or f.transform.is_identity()]
            if len(identity) == 1:
                partition_by = identity[0].name
            elif len(identity) > 1:
                raise ValueError(
                    "Hologres logical partition tables support a single partition column, "
                    f"got {len(identity)}: {[f.name for f in identity]}"
                )
        if partition_by is not None:
            options["partition_by"] = partition_by

        # Primary key: option or fall back to the partition column. Hologres
        # requires the partition column to be part of the primary key, so it
        # is appended when missing.
        primary_keys = _normalize_keys(options.get("primary_key"))
        if not primary_keys and partition_by:
            primary_keys = [partition_by]
        elif partition_by and partition_by not in primary_keys:
            primary_keys = [*primary_keys, partition_by]
        column_defs = build_column_defs(schema)
        column_names = [c.name for c in column_defs]
        for pk in primary_keys:
            if pk not in column_names:
                raise ValueError(f"Primary key column {pk!r} not found in schema columns {column_names}")

        with hologres_connection(self._inner) as conn, conn.cursor() as cur:
            try:
                if schema_name:
                    cur.execute(build_create_schema_sql(schema_name, if_not_exists=True))

                cur.execute(
                    build_create_table_sql(
                        schema_name,
                        table_name,
                        column_defs,
                        primary_keys=primary_keys,
                        partition_by=partition_by,
                    )
                )
                if schema_name is None:
                    # CREATE TABLE without a schema follows the search path;
                    # resolve it once so every subsequent property/index
                    # statement targets the table that was just created.
                    cur.execute("SELECT current_schema()")
                    row = cur.fetchone()
                    if row is None or row[0] is None:
                        raise ValueError("Unable to resolve the current schema for an unqualified table name")
                    effective_schema = row[0]
                else:
                    effective_schema = schema_name
                for stmt in build_table_property_statements(effective_schema, table_name, options):
                    cur.execute(stmt)
                index_columns = _normalize_index_columns(options.get("index"))
                if index_columns:
                    for stmt in build_fulltext_index_statements(effective_schema, table_name, index_columns):
                        cur.execute(stmt)
            except psycopg.errors.DuplicateTable:
                raise TableAlreadyExistsError(f"Table {identifier} already exists")
            except psycopg.Error as e:
                raise ValueError(f"Failed to create table {identifier}: {e}") from e

        return HologresTable._from_catalog(self._inner, Identifier(effective_schema, table_name))

    ###
    # drop_*
    ###

    def _drop_function(self, ident: Identifier) -> None:
        raise NotFoundError(f"Function '{ident}' not found in catalog '{self.name}'")

    def _drop_namespace(self, identifier: Identifier) -> None:
        """Drop a schema from Hologres."""
        if len(identifier) != 1:
            raise ValueError(f"Hologres namespace identifier must be a single schema name, got {identifier}")

        with hologres_connection(self._inner) as conn, conn.cursor() as cur:
            try:
                cur.execute(build_drop_schema_sql(identifier[0]))
            except psycopg.Error as e:
                raise ValueError(f"Failed to drop namespace {identifier}: {e}") from e

    def _drop_table(self, identifier: Identifier) -> None:
        """Drop a table from Hologres."""
        schema_name, table_name = _resolve_identifier(identifier, "table")

        with hologres_connection(self._inner) as conn, conn.cursor() as cur:
            try:
                cur.execute(build_drop_table_sql(schema_name, table_name))
            except psycopg.errors.UndefinedTable:
                raise NotFoundError(f"Table {identifier} not found")
            except psycopg.Error as e:
                raise ValueError(f"Failed to drop table {identifier}: {e}") from e

    ###
    # get_*
    ###

    def _get_function(self, ident: Identifier) -> Function:
        raise NotFoundError(f"Function '{ident}' not found in catalog '{self.name}'")

    def _get_table(self, identifier: Identifier) -> Table:
        """Get a table from Hologres."""
        if not self._has_table(identifier):
            raise NotFoundError(f"Table {identifier} not found")
        return HologresTable._from_catalog(self._inner, identifier)

    ###
    # has_*
    ###

    def _has_namespace(self, identifier: Identifier) -> bool:
        """Check if a schema exists in Hologres."""
        if len(identifier) != 1:
            raise ValueError(f"Hologres schema identifier must be a single schema name, got {identifier}")

        with hologres_connection(self._inner) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = %s)",
                (identifier[0],),
            )
            result = cur.fetchone()
            return result[0] if result else False

    def _has_table(self, identifier: Identifier) -> bool:
        """Check if a table exists in Hologres."""
        if len(identifier) not in (1, 2):
            return False

        with hologres_connection(self._inner) as conn, conn.cursor() as cur:
            if len(identifier) == 2:
                schema_name, table_name = identifier[0], identifier[1]
                cur.execute(
                    "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)",
                    (schema_name, table_name),
                )
            else:
                cur.execute(
                    """
                    SELECT EXISTS(
                        SELECT 1 FROM pg_class c
                        WHERE c.relname = %s
                          AND c.relkind IN ('r', 'p')
                          AND pg_table_is_visible(c.oid)
                    )
                    """,
                    (identifier[0],),
                )
            result = cur.fetchone()
            return result[0] if result else False

    ###
    # list_*
    ###

    def _list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        """List schemas in Hologres (system schemas hidden)."""
        with hologres_connection(self._inner) as conn, conn.cursor() as cur:
            if pattern:
                cur.execute(
                    "SELECT schema_name FROM information_schema.schemata "
                    "WHERE schema_name LIKE %s ORDER BY schema_name",
                    (pattern + "%",),
                )
            else:
                cur.execute("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name")
            return [Identifier(row[0]) for row in cur.fetchall() if row[0] not in _SYSTEM_SCHEMAS]

    def _list_tables(self, pattern: str | None = None) -> list[Identifier]:
        """List tables in Hologres, excluding system schemas and partition children."""
        with hologres_connection(self._inner) as conn, conn.cursor() as cur:
            query = """
                SELECT n.nspname, c.relname
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r', 'p')
                  AND n.nspname NOT IN (%s, %s, %s, %s)
                  AND NOT c.relispartition
            """
            params: list[str] = list(_SYSTEM_SCHEMAS[:4])
            if pattern:
                query += " AND c.relname LIKE %s"
                params.append(pattern + "%")
            query += " ORDER BY n.nspname, c.relname"
            cur.execute(query, params)
            return [Identifier(row[0], row[1]) for row in cur.fetchall()]


def _normalize_index_columns(value: Any) -> list[str]:
    """Normalize the full-text ``index`` option (str, sequence, or dict)."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [str(k) for k in value]
    try:
        return [str(v) for v in value]
    except TypeError:
        raise ValueError(f"Invalid full-text index option: {value!r}")


def _normalize_keys(value: Any) -> list[str]:
    """Normalize a primary key option (str, sequence of str, or None)."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    try:
        keys = [str(v) for v in value]
    except TypeError:
        raise ValueError(f"Invalid primary key option: {value!r}")
    return keys


class HologresTable(Table):
    """A Hologres table backed by a PostgreSQL connection string."""

    _inner: tuple[str, Identifier]  # (connection string, identifier)

    _read_options = {
        "read_mode",  # "arrow" (COPY TO ARROW_LZ4, default) or "select"
        "partition_col",
        "num_partitions",
        "partition_bound_strategy",
        "shard_parallelism",
        "disable_pushdowns",
    }
    _append_options = {
        "copy_mode",  # "stream" (default) or "bulk_load"
        "copy_format",  # "binary" (default) or "text"
        "on_conflict",  # "update" (default with PK), "ignore", "replace", "none"
    }
    _overwrite_options = {
        "copy_mode",
        "copy_format",
        "on_conflict",
    }

    def __init__(self) -> None:
        raise RuntimeError("HologresTable.__init__ is not supported, please use `Catalog.from_hologres` instead.")

    @classmethod
    def _from_catalog(cls, connection_string: str, identifier: Identifier) -> HologresTable:
        """Create a HologresTable from catalog connection details."""
        t = cls.__new__(cls)
        t._inner = (connection_string, identifier)
        return t

    @property
    def name(self) -> str:
        """Returns the table's name."""
        _, identifier = self._inner
        if len(identifier) == 1:
            return identifier[0]
        elif len(identifier) == 2:
            return identifier[1]
        else:
            raise ValueError(f"Invalid table identifier: {identifier}")

    @property
    def connection_string(self) -> str:
        return self._inner[0]

    @property
    def identifier(self) -> Identifier:
        return self._inner[1]

    @staticmethod
    def _from_obj(obj: object) -> HologresTable:
        """Returns a HologresTable if the given object can be adapted so."""
        raise ValueError(f"Unsupported hologres table type: {type(obj)}")

    ###
    # Table ABC methods
    ###

    def schema(self) -> Schema:
        """Returns the table's schema, read from Hologres metadata."""
        pairs = self._daft_schema_pairs()
        from daft.logical.schema import Field

        return Schema._from_fields([Field.create(name, dtype) for name, dtype in pairs])

    def _daft_schema_pairs(self) -> list[tuple[str, Any]]:
        """(name, DataType) pairs for this table; unmappable columns become string."""
        from daft.datatype import DataType
        from daft.io._hologres._schema_loader import schema_to_daft

        return [
            (name, dtype if dtype is not None else DataType.string())
            for name, dtype in schema_to_daft(self._load_hologres_schema())
        ]

    def _load_hologres_schema(self) -> HologresTableSchema:
        """Load full Hologres metadata (columns, PK, partitioning, shards)."""
        with hologres_connection(self.connection_string) as conn:
            schema_name, table_name = _resolve_table_schema(conn, self.identifier)
            return load_table_schema(conn, schema_name, table_name)

    def read(self, **options: Any) -> DataFrame:
        """Read the table as a DataFrame via the Hologres Arrow COPY export.

        Options:
            read_mode: "arrow" (default) or "select" (generic SQL path).
            partition_col: column to split parallel reads by bounds; defaults
                to the logical partition column when the table is a logical
                partition parent.
            num_partitions: number of parallel read tasks.
            partition_bound_strategy: "min-max" or "percentile".
            shard_parallelism: split reads by physical shard (default True).
            disable_pushdowns: don't push filters/limits into the query;
                shard/partition restrictions and the projection are still applied.
        """
        self._validate_options("Hologres read", options, HologresTable._read_options)

        from daft import context
        from daft.daft import ScanOperatorHandle, StorageConfig
        from daft.dataframe import DataFrame
        from daft.io._hologres.scan import HologresScanOperator
        from daft.logical.builder import LogicalPlanBuilder
        from daft.sql.sql_scan import PartitionBoundStrategy

        holo = self._load_hologres_schema()

        partition_col = options.get("partition_col")
        bound_strategy = options.get("partition_bound_strategy", "min-max")
        if partition_col is None and holo.partition_column is not None:
            # Logical partition table: align reads with the partition column.
            partition_col = holo.partition_column
            dtype = self.schema()[partition_col].dtype
            if not (dtype.is_numeric() or dtype.is_temporal()):
                bound_strategy = "percentile"

        io_config = context.get_context().daft_planning_config.default_io_config
        operator = HologresScanOperator(
            connection_uri=self.connection_string,
            holo_schema=holo,
            storage_config=StorageConfig(True, io_config),
            disable_pushdowns=options.get("disable_pushdowns", False),
            read_mode=options.get("read_mode", "arrow"),
            shard_parallelism=options.get("shard_parallelism", True),
            partition_col=partition_col,
            num_partitions=options.get("num_partitions"),
            partition_bound_strategy=PartitionBoundStrategy.from_str(bound_strategy),
        )
        handle = ScanOperatorHandle.from_python_scan_operator(operator)
        builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
        return DataFrame(builder)

    def append(self, df: DataFrame, **options: Any) -> None:
        """Append rows to the table; primary-key tables upsert (on conflict update).

        Rows are streamed with the Hologres stream COPY (fixed plan), which
        takes row-level locks and is safe for concurrent writers.

        Options:
            copy_mode: "stream" (default) or "bulk_load" (table lock,
                highest throughput; on_conflict requires Hologres >= 3.1).
            copy_format: "binary" (default) or "text".
            on_conflict: "update" (default with PK), "ignore", "replace",
                "none".
        """
        self._validate_options("Hologres append", options, HologresTable._append_options)

        from daft.io._hologres.sink import HologresDataSink

        holo = self._load_hologres_schema()
        sink = HologresDataSink(
            connection_uri=self.connection_string,
            schema_name=holo.schema_name,
            table_name=holo.table_name,
            write_mode="append",
            copy_mode=options.get("copy_mode", "stream"),
            copy_format=options.get("copy_format", "binary"),
            on_conflict=options.get("on_conflict"),
        )
        df.write_sink(sink)

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        """Overwrite the whole table (TRUNCATE first, driver-side).

        Accepts the same options as :meth:`append` (except write_mode).
        """
        self._validate_options("Hologres overwrite", options, HologresTable._overwrite_options)

        from daft.io._hologres.sink import HologresDataSink

        copy_mode = options.get("copy_mode", "stream")
        if copy_mode != "stage":
            # ``start()`` truncates before the lazy input is evaluated, so
            # ``table.overwrite(table.read().where(...))`` would erase its own
            # source. Materialize the replacement first for the non-stage
            # paths; stage mode reads all input before its INSERT OVERWRITE.
            df = df.collect()

        holo = self._load_hologres_schema()
        sink = HologresDataSink(
            connection_uri=self.connection_string,
            schema_name=holo.schema_name,
            table_name=holo.table_name,
            write_mode="overwrite",
            copy_mode=copy_mode,
            copy_format=options.get("copy_format", "binary"),
            on_conflict=options.get("on_conflict"),
        )
        df.write_sink(sink)
