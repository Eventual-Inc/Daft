"""Scan operator for Lance Partitioned Namespaces.

Reads ``__manifest`` to discover the partition tables, advertises partition
columns to Daft's planner, pushes filters into both partition pruning (via the
manifest's ``partition_field_*`` columns) and per-partition Arrow filter
pushdown, and emits one ScanTask per fragment (or fragment group).
"""

# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any, Optional

from daft.daft import PyExpr, PyPartitionField, PyPushdowns, PyRecordBatch, ScanTask
from daft.datatype import DataType, _ensure_registered_super_ext_type
from daft.dependencies import pa
from daft.expressions import Expression
from daft.io.partitioning import PartitionTransform
from daft.io.scan import ScanOperator, make_partition_field
from daft.logical.schema import Field, Schema
from daft.recordbatch import RecordBatch

from ..pushdowns import SupportsPushdownFilters
from .lance_namespace import (
    COL_LOCATION,
    COL_OBJECT_TYPE,
    COL_READ_VERSION,
    METADATA_KEY_SCHEMA,
    OBJECT_TYPE_TABLE,
    PARTITION_FIELD_PREFIX,
    get_schema_metadata_value,
    json_arrow_to_arrow_type,
    latest_partition_spec_key,
    parse_namespace_schema_json,
    parse_partition_spec_json,
)
from .utils import combine_filters_to_arrow

logger = logging.getLogger(__name__)


def _transform_from_spec(transform_obj: Any, source_type: DataType) -> tuple[Any, DataType]:
    """Convert a Partitioning spec transform entry into (PartitionTransform, result DataType).

    Accepts both the spec's structured form ``{"type": "year"}`` and the legacy
    string form ``"year"`` produced by older writers, so reads aren't brittle
    to writer variation.
    """
    if isinstance(transform_obj, dict):
        kind = transform_obj.get("type")
        num_buckets = transform_obj.get("num_buckets")
        width = transform_obj.get("width")
    else:
        kind = transform_obj
        num_buckets = None
        width = None

    if kind == "identity":
        return PartitionTransform.identity(), source_type
    if kind == "year":
        return PartitionTransform.year(), DataType.int32()
    if kind == "month":
        return PartitionTransform.month(), DataType.int32()
    if kind == "day":
        return PartitionTransform.day(), DataType.int32()
    if kind == "hour":
        return PartitionTransform.hour(), DataType.int32()
    if kind == "bucket":
        if num_buckets is None:
            raise ValueError("bucket transform requires num_buckets")
        return PartitionTransform.iceberg_bucket(int(num_buckets)), DataType.int32()
    if kind == "truncate":
        if width is None:
            raise ValueError("truncate transform requires width")
        return PartitionTransform.iceberg_truncate(int(width)), source_type
    raise NotImplementedError(f"Lance partition transform {kind!r} is not yet supported")


def _read_manifest(
    namespace_uri: str, storage_options: dict[str, str] | None
) -> tuple[pa.Table, dict[str, Any], pa.Schema | None, str]:
    import lance

    manifest_uri = namespace_uri.rstrip("/") + "/__manifest"
    manifest_ds = lance.dataset(manifest_uri, storage_options=storage_options)
    manifest_table = manifest_ds.to_table()

    schema_metadata = manifest_ds.schema.metadata or {}
    latest = latest_partition_spec_key(schema_metadata)
    if latest is None:
        raise ValueError(
            f"No partition_spec_v<N> key in {manifest_uri} metadata. Is this a partitioned Lance namespace?"
        )
    spec_n, spec_key = latest
    spec_payload = get_schema_metadata_value(schema_metadata, spec_key)
    if spec_payload is None:
        raise ValueError(f"Missing value for partition spec key {spec_key!r} in {manifest_uri} metadata")
    partition_spec = parse_partition_spec_json(spec_payload)

    raw_schema = get_schema_metadata_value(schema_metadata, METADATA_KEY_SCHEMA)
    namespace_schema = parse_namespace_schema_json(raw_schema) if raw_schema is not None else None

    return manifest_table, partition_spec, namespace_schema, f"v{spec_n}"


def _build_partition_keys(partition_spec: dict[str, Any], namespace_schema: pa.Schema) -> list[PyPartitionField]:
    fields: list[PyPartitionField] = []
    for field_def in partition_spec.get("fields", []):
        field_id = field_def["field_id"]
        source_ids = field_def.get("source_ids", [])

        if source_ids:
            # source_ids reference lance:field_id; for v1 we assigned positional ids
            # to the namespace schema, so the lookup is positional.
            source_idx = int(source_ids[0])
            source_arrow_field = namespace_schema.field(source_idx)
        elif field_id in namespace_schema.names:
            source_arrow_field = namespace_schema.field(namespace_schema.get_field_index(field_id))
        else:
            raise ValueError(f"Cannot resolve source field for partition field {field_id!r}")

        source_type = DataType.from_arrow_type(source_arrow_field.type)
        transform, result_type = _transform_from_spec(field_def.get("transform"), source_type)

        result_field = Field.create(field_id, result_type)
        source_field = Field.create(source_arrow_field.name, source_type)
        fields.append(
            make_partition_field(
                result_field,
                source_field,
                transform=transform._partition_transform if transform is not None else None,
            )
        )
    return fields


def _inject_partition_columns(
    rb: pa.RecordBatch,
    partition_values: dict[str, Any],
    partition_field_types: dict[str, pa.DataType],
    required_columns: list[str] | None,
) -> pa.RecordBatch:
    """Inject constant partition columns into a leaf record batch on read."""
    if not partition_values:
        return rb
    columns = list(rb.columns)
    names = list(rb.schema.names)
    for col_name, value in partition_values.items():
        if required_columns is not None and col_name not in required_columns:
            continue
        if col_name in names:
            continue
        dtype = partition_field_types.get(col_name, pa.utf8())
        columns.append(pa.array([value] * len(rb), type=dtype))
        names.append(col_name)
    return pa.RecordBatch.from_arrays(columns, names=names)


def _lance_namespace_factory_function(
    ds_uri: str,
    fragment_ids: list[int] | None = None,
    required_columns: list[str] | None = None,
    filter: Optional["pa.compute.Expression"] = None,
    limit: int | None = None,
    partition_values: dict[str, Any] | None = None,
    partition_field_types: dict[str, pa.DataType] | None = None,
    read_version: int | None = None,
    storage_options: dict[str, str] | None = None,
) -> Iterator[PyRecordBatch]:
    try:
        import lance
    except ImportError as e:
        raise ImportError(
            "Unable to import the `lance` package, please ensure that Daft is installed with the lance extra dependency: `pip install daft[lance]`"
        ) from e

    kwargs: dict[str, Any] = {}
    if storage_options is not None:
        kwargs["storage_options"] = storage_options
    if read_version is not None:
        kwargs["version"] = read_version
    ds = lance.dataset(ds_uri, **kwargs)

    pv = partition_values or {}
    pft = partition_field_types or {}

    # Strip partition columns from what we ask the leaf dataset to return;
    # they're injected afterward.
    non_partition_cols: list[str] | None = None
    if required_columns is not None:
        non_partition_cols = [c for c in required_columns if c not in pv]
        if not non_partition_cols:
            non_partition_cols = None

    if fragment_ids is None:
        scanner = ds.scanner(columns=non_partition_cols, filter=filter, limit=limit)
        for rb in scanner.to_batches():
            rb = _inject_partition_columns(rb, pv, pft, required_columns)
            yield RecordBatch.from_arrow_record_batches([rb], rb.schema)._recordbatch
    else:
        rows_yielded = 0
        for fid in fragment_ids:
            if limit is not None and rows_yielded >= limit:
                break
            fragment = ds.get_fragment(fid)
            fragment_limit = (limit - rows_yielded) if limit is not None else None
            scanner = ds.scanner(
                fragments=[fragment],
                columns=non_partition_cols,
                filter=filter,
                limit=fragment_limit,
            )
            for rb in scanner.to_batches():
                if limit is not None:
                    remaining = limit - rows_yielded
                    if remaining <= 0:
                        break
                    if len(rb) > remaining:
                        rb = rb.slice(0, remaining)
                rb = _inject_partition_columns(rb, pv, pft, required_columns)
                yield RecordBatch.from_arrow_record_batches([rb], rb.schema)._recordbatch
                rows_yielded += len(rb)


class LanceNamespaceScanOperator(ScanOperator, SupportsPushdownFilters):
    def __init__(
        self,
        namespace_uri: str,
        storage_options: dict[str, str] | None = None,
        fragment_group_size: int | None = None,
    ) -> None:
        self._namespace_uri = namespace_uri.rstrip("/")
        self._storage_options = storage_options
        self._fragment_group_size = fragment_group_size
        self._pushed_filters: list[PyExpr] | None = None
        self._remaining_filters: list[PyExpr] | None = None

        _ensure_registered_super_ext_type()

        manifest_table, partition_spec, namespace_schema, spec_version = _read_manifest(
            self._namespace_uri, self._storage_options
        )
        self._manifest_table = manifest_table
        self._partition_spec = partition_spec
        self._spec_version = spec_version

        if namespace_schema is None:
            namespace_schema = self._infer_namespace_schema_from_leaves()
        self._namespace_pa_schema = namespace_schema
        self._schema = Schema.from_pyarrow_schema(namespace_schema)
        self._partition_keys = _build_partition_keys(partition_spec, namespace_schema)

        # Map partition field id -> Arrow type, for constant-column injection on read.
        self._partition_field_types: dict[str, pa.DataType] = {}
        for field_def in partition_spec.get("fields", []):
            fid = field_def["field_id"]
            rt = field_def.get("result_type")
            if isinstance(rt, dict):
                self._partition_field_types[fid] = json_arrow_to_arrow_type(rt)
            elif fid in namespace_schema.names:
                self._partition_field_types[fid] = namespace_schema.field(fid).type

    # ---- ScanOperator surface --------------------------------------------

    def name(self) -> str:
        return "LanceNamespaceScanOperator"

    def display_name(self) -> str:
        return f"LanceNamespaceScanOperator({self._namespace_uri})"

    def schema(self) -> Schema:
        return self._schema

    def partitioning_keys(self) -> list[PyPartitionField]:
        return self._partition_keys

    def can_absorb_filter(self) -> bool:
        return True

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return True

    def supports_count_pushdown(self) -> bool:
        return False

    def as_pushdown_filter(self) -> SupportsPushdownFilters | None:
        return self

    def multiline_display(self) -> list[str]:
        n_tables = sum(
            1
            for i in range(self._manifest_table.num_rows)
            if self._manifest_table.column(COL_OBJECT_TYPE)[i].as_py() == OBJECT_TYPE_TABLE
        )
        return [
            self.display_name(),
            f"Schema = {self.schema()}",
            f"Partitions = {n_tables} tables",
        ]

    def push_filters(self, filters: list[PyExpr]) -> tuple[list[PyExpr], list[PyExpr]]:
        pushed: list[PyExpr] = []
        remaining: list[PyExpr] = []
        for expr in filters:
            try:
                Expression._from_pyexpr(expr).to_arrow_expr()
                pushed.append(expr)
            except NotImplementedError:
                remaining.append(expr)
        self._pushed_filters = pushed if pushed else None
        self._remaining_filters = remaining if remaining else None
        return pushed, remaining

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        if pushdowns.columns is None:
            required_columns: list[str] | None = None
        else:
            filter_req = pushdowns.filter_required_column_names() or []
            required_columns = list(set(list(pushdowns.columns) + list(filter_req)))

        pushed_arrow_filter = combine_filters_to_arrow(self._pushed_filters)

        partition_field_ids = [f["field_id"] for f in self._partition_spec.get("fields", [])]

        for i in range(self._manifest_table.num_rows):
            if self._manifest_table.column(COL_OBJECT_TYPE)[i].as_py() != OBJECT_TYPE_TABLE:
                continue
            location = self._manifest_table.column(COL_LOCATION)[i].as_py()
            if location is None:
                logger.warning("Skipping table row at index %d: missing location", i)
                continue

            partition_values: dict[str, Any] = {}
            for fid in partition_field_ids:
                col = f"{PARTITION_FIELD_PREFIX}{fid}"
                if col in self._manifest_table.column_names:
                    partition_values[fid] = self._manifest_table.column(col)[i].as_py()

            if pushdowns.partition_filters is not None and not self._partition_filter_matches(
                pushdowns.partition_filters, partition_values
            ):
                continue

            ds_uri = f"{self._namespace_uri}/{location}"
            read_version: int | None = None
            if COL_READ_VERSION in self._manifest_table.column_names:
                rv = self._manifest_table.column(COL_READ_VERSION)[i].as_py()
                if rv is not None:
                    read_version = int(rv)

            yield from self._partition_scan_tasks(
                ds_uri=ds_uri,
                partition_values=partition_values,
                required_columns=required_columns,
                pushed_arrow_filter=pushed_arrow_filter,
                pushdowns=pushdowns,
                read_version=read_version,
            )

    # ---- Internals --------------------------------------------------------

    def _infer_namespace_schema_from_leaves(self) -> pa.Schema:
        """Fallback for manifests written without the ``schema`` metadata key."""
        import lance

        partition_field_arrow: list[pa.Field] = []
        for field_def in self._partition_spec.get("fields", []):
            fid = field_def["field_id"]
            rt = field_def.get("result_type")
            arrow_type = json_arrow_to_arrow_type(rt) if isinstance(rt, dict) else pa.utf8()
            partition_field_arrow.append(pa.field(fid, arrow_type))

        for i in range(self._manifest_table.num_rows):
            if self._manifest_table.column(COL_OBJECT_TYPE)[i].as_py() != OBJECT_TYPE_TABLE:
                continue
            loc = self._manifest_table.column(COL_LOCATION)[i].as_py()
            if loc is None:
                continue
            try:
                ds = lance.dataset(f"{self._namespace_uri}/{loc}", storage_options=self._storage_options)
            except Exception:
                continue
            return pa.schema(list(ds.schema) + partition_field_arrow)
        raise ValueError(
            f"Could not determine namespace schema for {self._namespace_uri}: no readable leaf tables and "
            f"no `schema` metadata on the manifest."
        )

    def _partition_filter_matches(self, partition_filter: PyExpr, partition_values: dict[str, Any]) -> bool:
        """Evaluate a Daft partition filter against a single row's partition values."""
        try:
            arrays: dict[str, pa.Array] = {}
            for fid, value in partition_values.items():
                dtype = self._partition_field_types.get(fid, pa.utf8())
                arrays[fid] = pa.array([value], type=dtype)

            from daft.expressions.expressions import ExpressionsProjection

            rb = RecordBatch.from_pydict(arrays)
            result = rb.eval_expression_list(ExpressionsProjection([Expression._from_pyexpr(partition_filter)]))
            col = result.to_arrow_table().column(0)
            return bool(col[0].as_py())
        except Exception as e:  # pragma: no cover — fail-open on evaluation errors
            logger.warning("Partition filter eval failed (%s); including partition", e)
            return True

    def _partition_scan_tasks(
        self,
        *,
        ds_uri: str,
        partition_values: dict[str, Any],
        required_columns: list[str] | None,
        pushed_arrow_filter: Optional["pa.compute.Expression"],
        pushdowns: PyPushdowns,
        read_version: int | None,
    ) -> Iterator[ScanTask]:
        import lance

        try:
            ds = lance.dataset(ds_uri, storage_options=self._storage_options, version=read_version)
        except Exception as e:
            logger.warning("Failed to open partition dataset at %s: %s", ds_uri, e)
            return

        fragments = list(ds.get_fragments())

        def _size(frag: Any) -> int:
            md = getattr(frag, "metadata", None)
            files = getattr(md, "files", None) if md else None
            if not files:
                return 0
            return sum(f.file_size_bytes for f in files if f.file_size_bytes is not None)

        def _make_task(frag_ids: list[int], num_rows: int, size_bytes: int) -> ScanTask:
            return ScanTask.python_factory_func_scan_task(
                module=_lance_namespace_factory_function.__module__,
                func_name=_lance_namespace_factory_function.__name__,
                func_args=(
                    ds_uri,
                    frag_ids,
                    required_columns,
                    pushed_arrow_filter,
                    pushdowns.limit,
                    partition_values,
                    self._partition_field_types,
                    read_version,
                    self._storage_options,
                ),
                schema=self._schema._schema,
                num_rows=num_rows,
                size_bytes=size_bytes,
                pushdowns=pushdowns,
                stats=None,
                source_name=self.display_name(),
            )

        group_size = self._fragment_group_size
        if group_size is not None and group_size > 1:
            current: list[int] = []
            group_rows = 0
            group_bytes = 0
            for fragment in fragments:
                rows = fragment.count_rows(pushed_arrow_filter)
                if rows == 0:
                    continue
                current.append(fragment.fragment_id)
                group_rows += rows
                group_bytes += _size(fragment)
                if len(current) >= group_size:
                    yield _make_task(current, group_rows, group_bytes)
                    current = []
                    group_rows = 0
                    group_bytes = 0
            if current:
                yield _make_task(current, group_rows, group_bytes)
        else:
            for fragment in fragments:
                rows = fragment.count_rows(pushed_arrow_filter)
                if rows == 0:
                    continue
                yield _make_task([fragment.fragment_id], rows, _size(fragment))
