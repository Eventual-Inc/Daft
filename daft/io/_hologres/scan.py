"""Hologres scan operator reading via ``COPY (...) TO STDOUT WITH (FORMAT ARROW_LZ4)``.

The Arrow COPY export is the fastest bulk-read path on Hologres: the server
emits Arrow IPC streams (LZ4 block-compressed) that are decoded directly into
RecordBatches. Pushdowns (projection/predicate/limit) are composed into the
inner query with SQLGlot; reads are parallelized either by physical shard
(``hg_shard_id IN (...)``) or by a user-specified partition column.

Types needing conversion after the Arrow decode:
- ``boolean`` arrives as uint8 -> cast to bool
- ``timestamptz`` arrives as date64[ms] (the server stores ms) -> cast to a
  UTC-aware timestamp
- ``json``/``jsonb`` arrive as raw PostgreSQL datum bytes that cannot be
  decoded reliably; queries projecting json/jsonb columns fall back to the
  generic SELECT path (``read_mode="select"``).
"""

from __future__ import annotations

import io
import logging
import math
import struct
import warnings
from typing import TYPE_CHECKING

from daft.context import get_context
from daft.daft import (
    DatabaseSourceConfig,
    PyPartitionField,
    PyPushdowns,
    ScanTask,
    StorageConfig,
)
from daft.io._hologres._schema_loader import (
    HologresTableSchema,
    pg_type_to_daft,
    qualified_name_for_sql,
    quote_identifier_if_needed,
)
from daft.io.scan import ScanOperator
from daft.recordbatch import RecordBatch
from daft.schema import DataType, Field, Schema
from daft.sql.sql_connection import SQLConnection
from daft.sql.sql_scan import PartitionBoundStrategy

if TYPE_CHECKING:
    from collections.abc import Iterator

    import pyarrow as pa

logger = logging.getLogger(__name__)

_ARROW_SCAN_SOURCE_NAME = "HologresScanOperator"
_ARROW_IPC_PREFIX = b"ARROW1"
_PG_BINARY_SIGNATURE = b"PGCOPY\n\xff\r\n\x00"


def _daft_schema_from_holo(holo_schema: HologresTableSchema) -> Schema:
    fields = []
    for col in holo_schema.columns:
        dtype = pg_type_to_daft(col.type_name, col.precision, col.scale)
        fields.append(Field.create(col.name, dtype if dtype is not None else DataType.string()))
    return Schema._from_fields(fields)


def _force_sqlalchemy_uri(uri: str) -> str:
    if uri.startswith("postgresql://"):
        return "postgresql+psycopg://" + uri[len("postgresql://") :]
    if uri.startswith("postgres://"):
        return "postgresql+psycopg://" + uri[len("postgres://") :]
    return uri


def _normalize_predicate(predicate: str) -> str:
    """Normalize a Daft-generated predicate to the postgres dialect."""
    import sqlglot

    try:
        expr = sqlglot.parse_one(predicate, dialect="postgres")
        return expr.sql(dialect="postgres")
    except Exception:
        return predicate


class HologresScanOperator(ScanOperator):
    def __init__(
        self,
        connection_uri: str,
        holo_schema: HologresTableSchema,
        storage_config: StorageConfig,
        disable_pushdowns: bool = False,
        read_mode: str = "arrow",
        shard_parallelism: bool = True,
        partition_col: str | None = None,
        num_partitions: int | None = None,
        partition_bound_strategy: PartitionBoundStrategy | None = None,
    ) -> None:
        super().__init__()
        self._connection_uri = connection_uri
        self._holo_schema = holo_schema
        self._storage_config = storage_config
        self._disable_pushdowns = disable_pushdowns
        self._read_mode = read_mode
        self._shard_parallelism = shard_parallelism
        self._partition_col = partition_col
        self._num_partitions = num_partitions
        self._partition_bound_strategy = partition_bound_strategy or PartitionBoundStrategy.MIN_MAX
        self._schema = _daft_schema_from_holo(holo_schema)
        # Aux queries and the SELECT fallback go through SQLAlchemy + psycopg:
        # connectorx cannot parse jsonb/bytea columns that Hologres tables
        # commonly carry.
        self._sql_conn = SQLConnection.from_url(_force_sqlalchemy_uri(connection_uri))

    def schema(self) -> Schema:
        return self._schema

    def name(self) -> str:
        return _ARROW_SCAN_SOURCE_NAME

    def display_name(self) -> str:
        table = f"{self._holo_schema.schema_name}.{self._holo_schema.table_name}"
        return f"{_ARROW_SCAN_SOURCE_NAME}(table={table}, read_mode={self._read_mode})"

    def partitioning_keys(self) -> list[PyPartitionField]:
        return []

    def multiline_display(self) -> list[str]:
        return [self.display_name(), f"Schema = {self._schema}"]

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return False

    ###
    # Scan task planning
    ###

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        use_arrow = self._read_mode == "arrow" and not self._projects_json_columns(pushdowns)
        if self._read_mode == "arrow" and not use_arrow:
            logger.info(
                "Projection includes json/jsonb columns; using the SELECT read path for %s",
                self.display_name(),
            )

        num_tasks, shard_lists, needs_bounds = self._plan_parallelism(pushdowns)

        if num_tasks <= 1:
            yield self._scan_task(pushdowns, use_arrow, shard_list=None, partition_bounds=None)
            return

        if needs_bounds:
            yield from self._partition_bound_scan_tasks(pushdowns, use_arrow, num_tasks)
        else:
            for shard_list in shard_lists:
                yield self._scan_task(pushdowns, use_arrow, shard_list=shard_list, partition_bounds=None)

    def _projects_json_columns(self, pushdowns: PyPushdowns) -> bool:
        columns = pushdowns.columns
        if columns is None:
            # A full scan projects every column, json/jsonb included.
            return any(c.type_name in ("json", "jsonb") for c in self._holo_schema.columns)
        for name in columns:
            idx = self._holo_schema.get_column_index(name)
            if idx is not None and self._holo_schema.columns[idx].type_name in ("json", "jsonb"):
                return True
        return False

    def _plan_parallelism(self, pushdowns: PyPushdowns) -> tuple[int, list[list[int]], bool]:
        """Plan (num_tasks, shard lists, needs partition bounds)."""
        total_rows = self._get_num_rows(pushdowns)
        if total_rows <= 0:
            return (1, [], False)

        size_bytes = total_rows * self._schema.estimate_row_size_bytes()
        target = get_context().daft_execution_config.read_sql_partition_size_bytes
        num_tasks = math.ceil(size_bytes / target) if target > 0 else 1
        if self._num_partitions is not None:
            num_tasks = self._num_partitions
        num_tasks = max(1, min(num_tasks, total_rows))

        shard_count = self._holo_schema.shard_count
        if self._partition_col is not None:
            return (num_tasks, [], True)
        if not self._shard_parallelism or shard_count <= 1:
            return (1, [], False)

        num_tasks = max(1, min(num_tasks, shard_count))
        base, extra = divmod(shard_count, num_tasks)
        shard_lists: list[list[int]] = []
        start = 0
        for i in range(num_tasks):
            n = base + (1 if i < extra else 0)
            shard_lists.append(list(range(start, start + n)))
            start += n
        return (num_tasks, shard_lists, False)

    def _partition_bound_scan_tasks(
        self, pushdowns: PyPushdowns, use_arrow: bool, num_tasks: int
    ) -> Iterator[ScanTask]:
        bounds = self._get_partition_bounds(num_tasks)
        if bounds is None or len(bounds) < 2:
            warnings.warn(
                f"Unable to partition the data by {self._partition_col}; falling back to a single scan task.",
                UserWarning,
            )
            yield self._scan_task(pushdowns, use_arrow, shard_list=None, partition_bounds=None)
            return

        size_bytes = math.ceil(
            self._get_num_rows(pushdowns) * self._schema.estimate_row_size_bytes() / max(1, len(bounds) - 1)
        )
        partition_col = quote_identifier_if_needed(self._partition_col)
        for i in range(len(bounds) - 1):
            left = f"{partition_col} >= {self._literal_sql(bounds[i])}"
            right_op = "<=" if i == len(bounds) - 2 else "<"
            right = f"{partition_col} {right_op} {self._literal_sql(bounds[i + 1])}"
            if i == 0:
                # The min/max (and string distinct-value) queries ignore nulls,
                # so no range would contain them. Include null keys in the
                # first range on both sides: the right bound is exclusive for
                # all but the last range, so nulls must satisfy both
                # comparisons to be emitted exactly once.
                left = f"({left} OR {partition_col} IS NULL)"
                right = f"({right} OR {partition_col} IS NULL)"
            stats = RecordBatch.from_pydict({self._partition_col: [bounds[i], bounds[i + 1]]})
            yield self._scan_task(
                pushdowns,
                use_arrow,
                shard_list=None,
                partition_bounds=(left, right),
                size_bytes=size_bytes,
                stats=stats._recordbatch,
            )

    def _scan_task(
        self,
        pushdowns: PyPushdowns,
        use_arrow: bool,
        shard_list: list[int] | None = None,
        partition_bounds: tuple[str, str] | None = None,
        size_bytes: int | None = None,
        stats=None,
    ) -> ScanTask:
        predicate_sql, filter_applied, limit_applied = self._plan_query_pushdowns(
            pushdowns, use_arrow=use_arrow, shard_list=shard_list
        )
        push_limit = pushdowns.limit if limit_applied else None

        # Projection is part of the materialized schema, so it is always
        # pushed into the query even when filter/limit pushdowns are disabled.
        projection = (
            self._quoted_columns(pushdowns.columns) if use_arrow else self._select_projection(pushdowns.columns)
        )
        sql = self._compose_query(
            projection=projection,
            predicate=predicate_sql,
            limit=push_limit,
            partition_bounds=partition_bounds,
        )

        if use_arrow:
            projected_schema = self._projected_schema(pushdowns)
            # The arrow query never pushes LIMIT, and filters which could not
            # be rendered as SQL must still be evaluated by the engine.
            remaining = PyPushdowns(
                filters=pushdowns.filters if not filter_applied else None,
                limit=pushdowns.limit,
            )
            return ScanTask.python_factory_func_scan_task(
                module=_hologres_arrow_scan_factory.__module__,
                func_name=_hologres_arrow_scan_factory.__name__,
                func_args=(self._connection_uri, sql, projected_schema._schema),
                schema=projected_schema._schema,
                num_rows=None,
                size_bytes=size_bytes,
                pushdowns=remaining,
                stats=stats,
                source_name=_ARROW_SCAN_SOURCE_NAME,
            )

        db_config = DatabaseSourceConfig(sql, self._sql_conn)
        remaining_pushdowns = PyPushdowns(
            filters=pushdowns.filters if not filter_applied else None,
            columns=pushdowns.columns,
            limit=pushdowns.limit if not limit_applied else None,
        )
        return ScanTask.sql_scan_task(
            url=self._sql_conn.url,
            config=db_config,
            schema=self._schema._schema,
            storage_config=self._storage_config,
            num_rows=None,
            size_bytes=size_bytes,
            pushdowns=remaining_pushdowns,
            stats=stats,
        )

    def _plan_query_pushdowns(
        self,
        pushdowns: PyPushdowns,
        *,
        use_arrow: bool,
        shard_list: list[int] | None = None,
    ) -> tuple[str | None, bool, bool]:
        """Decide which pushdowns go into SQL and which remain for the engine.

        Returns ``(predicate_sql, filter_applied, limit_applied)``. Shard and
        partition restrictions which make tasks disjoint are always kept;
        ``disable_pushdowns`` only suppresses optional filter/limit pushdowns.
        """
        # Restrictions which split the table into disjoint tasks (physical
        # shards and partition bounds) are always applied. Only the optional
        # filter/limit pushdowns are controlled by ``disable_pushdowns``.
        filter_sql = pushdowns.filters.to_sql() if pushdowns.filters is not None else None
        filter_applied = not self._disable_pushdowns and filter_sql is not None

        predicate_parts: list[str] = []
        if filter_applied:
            predicate_parts.append(f"({filter_sql})")
        if shard_list:
            shard_predicate = f"hg_shard_id IN ({', '.join(map(str, shard_list))})"
            predicate_parts.append(shard_predicate)
        predicate_sql = " AND ".join(predicate_parts) if predicate_parts else None

        # Hologres' COPY TO ARROW export truncates or corrupts batches when a
        # pushed LIMIT runs through its export path, so the arrow path never
        # pushes the limit; the engine enforces it on the returned batches.
        limit_applied = not self._disable_pushdowns and not use_arrow and pushdowns.limit is not None
        return predicate_sql, filter_applied, limit_applied

    def _quoted_columns(self, projection: list[str] | None) -> list[str] | None:
        if projection is None:
            return None
        return [quote_identifier_if_needed(c) for c in projection]

    def _select_projection(self, projection: list[str] | None) -> list[str]:
        """Projection for the SELECT path with json/jsonb cast to text.

        The declared Daft type for json/jsonb columns is string, so drivers
        must return text instead of parsed objects.
        """
        if projection is None:
            projection = [c.name for c in self._holo_schema.columns]
        out = []
        for name in projection:
            quoted = quote_identifier_if_needed(name)
            idx = self._holo_schema.get_column_index(name)
            if idx is not None and self._holo_schema.columns[idx].type_name in ("json", "jsonb"):
                out.append(f"{quoted}::text AS {quoted}")
            else:
                out.append(quoted)
        return out

    def _projected_schema(self, pushdowns: PyPushdowns) -> Schema:
        if pushdowns.columns is None:
            return self._schema
        return Schema._from_fields([Field.create(name, self._schema[name].dtype) for name in pushdowns.columns])

    def _base_query(self) -> str:
        holo = self._holo_schema
        return qualified_name_for_sql(holo.schema_name, holo.table_name)

    def _compose_query(
        self,
        projection: list[str] | None = None,
        predicate: str | None = None,
        limit: int | None = None,
        partition_bounds: tuple[str, str] | None = None,
    ) -> str:
        """Compose a SELECT directly against the table (no subquery wrapping).

        The subquery form breaks Hologres' Arrow COPY export (truncated rows
        and ignored LIMITs), so projections/predicates/limits are applied in
        one flat SELECT. Daft-generated predicates are normalized through
        SQLGlot's postgres dialect.
        """
        # Projection items are raw SQL fragments (already quoted).
        cols = ", ".join(projection) if projection else "*"
        sql = f"SELECT {cols} FROM {self._base_query()}"

        where_parts: list[str] = []
        if predicate:
            where_parts.append(_normalize_predicate(predicate))
        if partition_bounds:
            where_parts.append(f"({partition_bounds[0]})")
            where_parts.append(f"({partition_bounds[1]})")
        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        return sql

    ###
    # Metadata queries
    ###

    def _get_num_rows(self, pushdowns: PyPushdowns) -> int:
        predicate_sql = pushdowns.filters.to_sql() if pushdowns.filters is not None else None
        count_sql = f"SELECT COUNT(*) FROM {self._base_query()}"
        if predicate_sql:
            count_sql += f" WHERE {_normalize_predicate(predicate_sql)}"
        table = self._sql_conn.execute_sql_query(count_sql)
        if table.num_rows != 1 or table.num_columns != 1:
            raise RuntimeError("Failed to get the number of rows: COUNT(*) query returned an unexpected shape.")
        return int(table.column(0)[0].as_py())

    def _get_partition_bounds(self, num_scan_tasks: int) -> list | None:
        if self._partition_col is None:
            raise ValueError("partition_col must be specified to partition the data.")

        dtype = self._schema[self._partition_col].dtype
        if not (dtype.is_temporal() or dtype.is_numeric() or dtype.is_string()):
            raise ValueError(
                f"Failed to get partition bounds: {self._partition_col} is not numeric, temporal, or string, "
                "and cannot be used for partitioning."
            )

        if dtype.is_string():
            # Hologres' percentile aggregates reject strings; split the sorted
            # distinct values instead, which is exact for partition columns.
            partition_col = quote_identifier_if_needed(self._partition_col)
            distinct_sql = f"SELECT DISTINCT {partition_col} FROM {self._base_query()} ORDER BY 1"
            table = self._sql_conn.execute_sql_query(distinct_sql)
            values = [r[0] for r in zip(*table.to_pydict().values())] if table.num_rows else []
            return _string_partition_bounds(values, num_scan_tasks)

        if self._partition_bound_strategy == PartitionBoundStrategy.PERCENTILE:
            try:
                partition_col = quote_identifier_if_needed(self._partition_col)
                percentiles = [i / num_scan_tasks for i in range(num_scan_tasks + 1)]
                bounds = ", ".join(
                    f"percentile_disc({p}) WITHIN GROUP (ORDER BY {partition_col}) AS bound_{i}"
                    for i, p in enumerate(percentiles)
                )
                percentile_sql = f"SELECT {bounds} FROM {self._base_query()} LIMIT 1"
                table = self._sql_conn.execute_sql_query(percentile_sql)
                if table.num_rows != 1:
                    raise RuntimeError(f"Expected 1 row, got {table.num_rows}.")
                pydict = RecordBatch.from_arrow_table(table).to_pydict()
                return [pydict[f"bound_{i}"][0] for i in range(num_scan_tasks + 1)]
            except Exception as e:
                warnings.warn(f"Failed percentile partitioning ({e}); falling back to MIN_MAX.", UserWarning)
                self._partition_bound_strategy = PartitionBoundStrategy.MIN_MAX

        partition_col = quote_identifier_if_needed(self._partition_col)
        min_max_sql = f"SELECT MIN({partition_col}) AS min, MAX({partition_col}) AS max FROM {self._base_query()}"
        import pyarrow as pa

        pa_schema = pa.schema([pa.field("min", dtype.to_arrow_dtype()), pa.field("max", dtype.to_arrow_dtype())])
        table = self._sql_conn.execute_sql_query(min_max_sql, schema=pa_schema)
        if table.num_rows != 1 or table.num_columns != 2:
            raise RuntimeError("Failed to get partition bounds: unexpected query shape.")
        pydict = RecordBatch.from_arrow_table(table).to_pydict()
        min_val, max_val = pydict["min"][0], pydict["max"][0]
        if min_val is None or max_val is None:
            return None
        if num_scan_tasks == 1:
            return [min_val, max_val]
        range_size = (max_val - min_val) / num_scan_tasks
        return [min_val + range_size * i for i in range(num_scan_tasks)] + [max_val]

    def _literal_sql(self, value) -> str:
        from daft.expressions.expressions import lit

        sql = lit(value)._to_sql()
        if sql is None:
            raise ValueError(f"Cannot render partition bound {value!r} as SQL")
        return sql


def _string_partition_bounds(values: list, num_scan_tasks: int) -> list | None:
    """Split sorted distinct string partition values into non-empty bounds.

    Nulls are excluded: MIN/MAX and DISTINCT ordering put them outside the
    useful range. The first generated scan task explicitly includes null keys
    (see :meth:`HologresScanOperator._partition_bound_scan_tasks`).
    """
    values = sorted({v for v in values if v is not None})
    if not values:
        return None
    num_scan_tasks = min(num_scan_tasks, len(values))
    if num_scan_tasks <= 1:
        return None

    chunk = max(1, -(-len(values) // num_scan_tasks))
    bounds = [values[0]]
    for i in range(chunk - 1, len(values) - 1, chunk):
        if values[i] != bounds[-1]:
            bounds.append(values[i])
    if bounds[-1] != values[-1]:
        bounds.append(values[-1])
    return bounds


def _hologres_arrow_scan_factory(connection_uri: str, sql: str, schema) -> Iterator:
    """ScanTask factory: stream a ``COPY ... TO STDOUT (FORMAT ARROW)`` query.

    The Hologres response is a PostgreSQL binary COPY stream whose fields are
    raw Arrow IPC streams (``[PGCOPY header][flags][ext]
    ([nfields][len][arrow stream])*[trailer]``). Each stream becomes one
    RecordBatch; the type fixes the Arrow export needs (boolean uint8 -> bool,
    timestamptz date64 -> UTC timestamp) are applied afterwards.
    """
    import psycopg
    import pyarrow as pa

    from daft.recordbatch import RecordBatch
    from daft.schema import Schema

    target = Schema._from_pyschema(schema)

    bool_columns = [f.name for f in target if f.dtype.is_boolean()]
    tz_columns = [f.name for f in target if f.dtype.is_timestamp() and f.dtype.timezone]

    conn = psycopg.connect(connection_uri, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SET timezone TO 'UTC'")
            with cur.copy(f"COPY ({sql}) TO STDOUT WITH (FORMAT ARROW)") as copy:
                for data in _iter_arrow_blocks_from_copy(copy):
                    with pa.ipc.open_stream(io.BytesIO(data)) as reader:
                        for batch in reader:
                            table = pa.Table.from_batches([batch])
                            table = _fix_arrow_types(table, bool_columns, tz_columns)
                            table = _compact_table(table)
                            yield RecordBatch.from_arrow_table(table)._recordbatch
    finally:
        try:
            conn.close()
        except Exception:
            pass


_PG_COPY_HEADER_SIZE = len(_PG_BINARY_SIGNATURE) + 8  # signature + flags + extension length


class _CopyStreamReader:
    """Incrementally read a psycopg COPY stream without buffering the result."""

    def __init__(self, copy) -> None:
        self._copy = copy
        self._buffer = bytearray()
        self._eof = False

    def _fill(self, size: int) -> bool:
        while len(self._buffer) < size and not self._eof:
            # psycopg's Copy.read() takes no size argument; every call returns
            # the next chunk made available by the server.
            chunk = self._copy.read()
            if not chunk:
                self._eof = True
                break
            self._buffer.extend(chunk)
        return len(self._buffer) >= size

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            parts = [bytes(self._buffer)]
            self._buffer.clear()
            while not self._eof:
                chunk = self._copy.read()
                if not chunk:
                    self._eof = True
                    break
                parts.append(chunk)
            return b"".join(parts)

        self._fill(size)
        data = bytes(self._buffer[:size])
        del self._buffer[:size]
        return data


class _RawArrowReader:
    """File-like adapter feeding an already-read prefix plus a COPY stream."""

    def __init__(self, prefix: bytes, stream: _CopyStreamReader) -> None:
        self._prefix = prefix
        self._stream = stream
        self._closed = False

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            data = self._prefix + self._stream.read()
            self._prefix = b""
            return data
        if not self._prefix:
            return self._stream.read(size)
        if len(self._prefix) >= size:
            data = self._prefix[:size]
            self._prefix = self._prefix[size:]
            return data
        prefix = self._prefix
        self._prefix = b""
        return prefix + self._stream.read(size - len(prefix))

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        self._closed = True


def _iter_arrow_blocks_from_copy(copy) -> Iterator[bytes]:
    """Yield Arrow IPC streams from a live ``COPY ... TO STDOUT`` response."""
    stream = _CopyStreamReader(copy)
    header = stream.read(_PG_COPY_HEADER_SIZE)

    if not header.startswith(_PG_BINARY_SIGNATURE):
        # Some servers return a bare Arrow IPC stream instead of PGCOPY
        # framing. Feed the prefix back to pyarrow together with the rest of
        # the stream so it can still decode incrementally.
        if not header:
            raise ValueError("Empty Hologres COPY ARROW response")
        reader = _RawArrowReader(header, stream)
        import pyarrow as pa

        with pa.ipc.open_stream(reader) as ipc_reader:
            for batch in ipc_reader:
                buffer = pa.BufferOutputStream()
                with pa.ipc.new_stream(buffer, batch.schema) as writer:
                    writer.write_batch(batch)
                yield buffer.getvalue().to_pybytes()
        return

    while True:
        nfields_bytes = stream.read(2)
        if len(nfields_bytes) < 2:
            break
        (nfields,) = struct.unpack("!h", nfields_bytes)
        if nfields == -1:  # trailer
            break

        length_bytes = stream.read(4)
        if len(length_bytes) < 4:
            raise ValueError("Truncated Hologres COPY ARROW field length")
        (field_length,) = struct.unpack("!i", length_bytes)
        data = stream.read(field_length)
        if len(data) < field_length:
            raise ValueError("Truncated Hologres COPY ARROW field data")
        yield data


def _iter_arrow_blocks(raw: bytes) -> Iterator[bytes]:
    """Yield Arrow IPC streams from a ``COPY ... TO STDOUT (FORMAT ARROW)`` response.

    The response is PostgreSQL binary COPY framing: ``PGCOPY`` header
    (signature + flags + extension length), then rows of
    ``[nfields][length][arrow IPC stream]``, terminated by ``ff ff``.
    """
    if raw.startswith((_ARROW_IPC_PREFIX, b"\xff\xff\xff\xff")):
        yield raw
        return

    if not raw.startswith(_PG_BINARY_SIGNATURE):
        raise ValueError("Unrecognized Hologres COPY ARROW response format")

    pos = _PG_COPY_HEADER_SIZE
    while pos + 2 <= len(raw):
        (nfields,) = struct.unpack("!h", raw[pos : pos + 2])
        pos += 2
        if nfields == -1:  # trailer
            break
        (flen,) = struct.unpack("!i", raw[pos : pos + 4])
        pos += 4
        yield raw[pos : pos + flen]
        pos += flen


def _compact_table(table: pa.Table) -> pa.Table:
    """Rebuild decoded arrays with fresh, zero-padded buffers.

    Hologres' Arrow export pads buffers with uninitialized bytes (a 5-row
    batch can carry 32KB string buffers). Strict consumers validate whole
    buffers, so string/list columns are re-encoded from their Python values,
    guaranteeing clean, zero-padded buffers regardless of what the export
    left in the padding.
    """
    import pyarrow as pa

    # Strip custom field metadata: Hologres attaches binary column-statistics
    # metadata ("col_stat") that downstream consumers choke on.
    clean_schema = pa.schema([pa.field(f.name, f.type, nullable=f.nullable) for f in table.schema])
    return pa.Table.from_pydict(
        {name: table.column(name).to_pylist() for name in table.column_names},
        schema=clean_schema,
    )


def _fix_arrow_types(table: pa.Table, bool_columns: list[str], tz_columns: list[str]) -> pa.Table:
    """Post-decode casts: boolean uint8 -> bool, timestamptz date64 -> UTC timestamp."""
    import pyarrow as pa
    import pyarrow.compute as pc

    if not bool_columns and not tz_columns:
        return table

    # Normalize chunking across all columns first: rebuilds with per-column
    # casts can otherwise produce differently-chunked columns, and
    # ``to_batches`` on such a table misaligns column buffers.
    table = table.combine_chunks()

    columns = []
    fields = []
    for field in table.schema:
        col = table.column(field.name)
        if isinstance(col, pa.ChunkedArray):
            col = col.chunk(0) if col.num_chunks == 1 else col.combine_chunks()
        if isinstance(col, pa.ChunkedArray):
            col = col.combine_chunks()
        if field.name in bool_columns and not pa.types.is_boolean(col.type):
            col = pc.cast(col, pa.bool_())
            field = pa.field(field.name, pa.bool_(), nullable=field.nullable)
        elif field.name in tz_columns:
            if pa.types.is_date64(col.type):
                col = col.cast(pa.timestamp("ms"))
                field = pa.field(field.name, col.type, nullable=field.nullable)
            if isinstance(col.type, pa.TimestampType) and col.type.tz is None:
                col = pc.assume_timezone(col, "UTC")
                field = pa.field(field.name, col.type, nullable=field.nullable)
        columns.append(col)
        fields.append(field)
    return pa.Table.from_arrays(columns, schema=pa.schema(fields))
