# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from typing import Any, Optional

from daft.daft import PyExpr, PyPartitionField, PyPushdowns, PyRecordBatch, ScanTask
from daft.datatype import DataType, _ensure_registered_super_ext_type
from daft.dependencies import pa
from daft.expressions import Expression
from daft.io.scan import ScanOperator, make_partition_field
from daft.logical.schema import Field, Schema
from daft.recordbatch import RecordBatch

from ..pushdowns import SupportsPushdownFilters
from .utils import combine_filters_to_arrow

logger = logging.getLogger(__name__)

_ARROW_TYPE_MAP: dict[str, pa.DataType] = {
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float16": pa.float16(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "double": pa.float64(),
    "float": pa.float32(),
    "string": pa.utf8(),
    "utf8": pa.utf8(),
    "large_string": pa.large_string(),
    "large_utf8": pa.large_utf8(),
    "bool": pa.bool_(),
    "boolean": pa.bool_(),
    "date32": pa.date32(),
}


def _convert_lance_transform(transform_name: str, source_type: DataType) -> tuple[Any, DataType]:
    from daft.io.partitioning import PartitionTransform

    if transform_name == "identity":
        return PartitionTransform.identity(), source_type
    elif transform_name == "year":
        return PartitionTransform.year(), DataType.int32()
    elif transform_name == "month":
        return PartitionTransform.month(), DataType.int32()
    elif transform_name == "day":
        return PartitionTransform.day(), DataType.date()
    elif transform_name == "hour":
        return PartitionTransform.hour(), DataType.int32()
    elif transform_name.startswith("bucket("):
        n = int(transform_name[len("bucket(") : -1])
        return PartitionTransform.iceberg_bucket(n), DataType.int32()
    elif transform_name.startswith("truncate("):
        w = int(transform_name[len("truncate(") : -1])
        return PartitionTransform.iceberg_truncate(w), source_type
    else:
        raise NotImplementedError(f"Lance partition transform '{transform_name}' is not yet supported")


def _read_lance_manifest(
    namespace_uri: str, storage_options: dict[str, str] | None = None
) -> tuple[Any, dict[str, Any], dict[str, Any] | None, str]:
    import lance

    manifest_uri = namespace_uri.rstrip("/") + "/__manifest"
    manifest_ds = lance.dataset(manifest_uri, storage_options=storage_options)
    manifest_table = manifest_ds.to_table()

    schema_metadata = manifest_ds.schema.metadata or {}
    partition_spec = None
    max_version = -1
    for key in schema_metadata:
        key_str = key.decode() if isinstance(key, bytes) else key
        if key_str.startswith("partition_spec_v"):
            version_num = int(key_str[len("partition_spec_v") :])
            if version_num > max_version:
                max_version = version_num
                val = schema_metadata[key]
                partition_spec = json.loads(val.decode() if isinstance(val, bytes) else val)

    if partition_spec is None:
        raise ValueError(
            f"No partition spec found in manifest metadata at {manifest_uri}. Is this a partitioned Lance namespace?"
        )

    ns_schema_json = schema_metadata.get(b"schema") or schema_metadata.get("schema")
    ns_schema = None
    if ns_schema_json:
        ns_schema = json.loads(ns_schema_json.decode() if isinstance(ns_schema_json, bytes) else ns_schema_json)

    return manifest_table, partition_spec, ns_schema, f"v{max_version}"


def _build_partition_fields(partition_spec: dict[str, Any], namespace_schema: pa.Schema) -> list[PyPartitionField]:
    fields = []
    for field_def in partition_spec.get("fields", []):
        field_id = field_def["field_id"]
        source_ids = field_def.get("source_ids", [])
        transform_raw = field_def.get("transform", {"type": "identity"})
        transform_name = transform_raw["type"] if isinstance(transform_raw, dict) else transform_raw

        if source_ids:
            source_idx = source_ids[0]
            source_arrow_field = namespace_schema.field(source_idx)
            source_name = source_arrow_field.name
            source_type = DataType.from_arrow_type(source_arrow_field.type)
        else:
            source_name = field_id
            source_type = DataType.from_arrow_type(
                namespace_schema.field(namespace_schema.get_field_index(field_id)).type
            )

        partition_transform, result_type = _convert_lance_transform(transform_name, source_type)
        tfm = partition_transform._partition_transform if partition_transform is not None else None

        result_field = Field.create(field_id, result_type)
        source_field = Field.create(source_name, source_type)
        fields.append(make_partition_field(result_field, source_field, transform=tfm))

    return fields


def _lancedb_namespace_factory_function(
    ds_uri: str,
    open_kwargs: dict[Any, Any] | None = None,
    fragment_ids: list[int] | None = None,
    required_columns: list[str] | None = None,
    filter: Optional["pa.compute.Expression"] = None,
    limit: int | None = None,
    partition_values: dict[str, Any] | None = None,
    partition_col_types: dict[str, str] | None = None,
    read_version: int | None = None,
) -> Iterator[PyRecordBatch]:
    try:
        import lance
    except ImportError as e:
        raise ImportError(
            "Unable to import the `lance` package, please ensure that Daft is installed with the lance extra dependency: `pip install daft[lance]`"
        ) from e

    kwargs = dict(open_kwargs or {})
    if read_version is not None:
        kwargs["version"] = read_version
    ds = lance.dataset(ds_uri, **kwargs)

    non_partition_columns = None
    if required_columns is not None:
        partition_col_set = set(partition_values.keys()) if partition_values else set()
        non_partition_columns = [c for c in required_columns if c not in partition_col_set]
        if not non_partition_columns:
            non_partition_columns = None

    if fragment_ids is None:
        scanner = ds.scanner(columns=non_partition_columns, filter=filter, limit=limit)
        for rb in scanner.to_batches():
            rb = _inject_partition_columns(rb, partition_values, partition_col_types, required_columns)
            yield RecordBatch.from_arrow_record_batches([rb], rb.schema)._recordbatch
    else:
        fragments = [ds.get_fragment(fid) for fid in fragment_ids]
        rows_yielded = 0
        for fragment in fragments:
            if limit is not None and rows_yielded >= limit:
                break
            fragment_limit = (limit - rows_yielded) if limit is not None else None
            scanner = ds.scanner(
                fragments=[fragment],
                columns=non_partition_columns or None,
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
                rb = _inject_partition_columns(rb, partition_values, partition_col_types, required_columns)
                yield RecordBatch.from_arrow_record_batches([rb], rb.schema)._recordbatch
                rows_yielded += len(rb)


def _inject_partition_columns(
    rb: pa.RecordBatch,
    partition_values: dict[str, Any] | None,
    partition_col_types: dict[str, str] | None,
    required_columns: list[str] | None,
) -> pa.RecordBatch:
    if not partition_values:
        return rb

    columns = list(rb.columns)
    names = list(rb.schema.names)
    for col_name, value in partition_values.items():
        if required_columns is not None and col_name not in required_columns:
            continue
        if col_name in names:
            continue
        arrow_type: pa.DataType = pa.utf8()
        if partition_col_types and col_name in partition_col_types:
            arrow_type = _ARROW_TYPE_MAP.get(partition_col_types[col_name], pa.utf8())
        columns.append(pa.array([value] * len(rb), type=arrow_type))
        names.append(col_name)

    return pa.RecordBatch.from_arrays(columns, names=names)


class LanceNamespaceScanOperator(ScanOperator, SupportsPushdownFilters):
    def __init__(
        self,
        namespace_uri: str,
        storage_options: dict[str, str] | None = None,
        fragment_group_size: int | None = None,
    ):
        self._namespace_uri = namespace_uri.rstrip("/")
        self._storage_options = storage_options
        self._fragment_group_size = fragment_group_size
        self._pushed_filters: list[PyExpr] | None = None
        self._remaining_filters: list[PyExpr] | None = None

        _ensure_registered_super_ext_type()

        manifest_table, partition_spec, ns_schema_json, spec_version = _read_lance_manifest(
            self._namespace_uri, self._storage_options
        )
        self._manifest_table = manifest_table
        self._partition_spec = partition_spec
        self._spec_version = spec_version

        self._namespace_pa_schema = self._build_namespace_schema(ns_schema_json, manifest_table)
        self._partition_keys = _build_partition_fields(partition_spec, self._namespace_pa_schema)
        self._schema = Schema.from_pyarrow_schema(self._namespace_pa_schema)

        self._partition_col_types: dict[str, str] = {}
        for field_def in partition_spec.get("fields", []):
            fid = field_def["field_id"]
            idx = self._namespace_pa_schema.get_field_index(fid)
            if idx >= 0:
                self._partition_col_types[fid] = str(self._namespace_pa_schema.field(idx).type)

    def _build_namespace_schema(self, ns_schema_json: dict[str, Any] | None, manifest_table: pa.Table) -> pa.Schema:
        if ns_schema_json and "fields" in ns_schema_json:
            return self._schema_from_json(ns_schema_json)

        from .utils import namespace_physical_path

        for i in range(manifest_table.num_rows):
            object_id = manifest_table.column("object_id")[i].as_py()
            ds_uri = namespace_physical_path(self._namespace_uri, object_id)
            try:
                import lance

                ds = lance.dataset(ds_uri, storage_options=self._storage_options)
                data_schema = ds.schema
                partition_fields = []
                for field_def in self._partition_spec.get("fields", []):
                    fid = field_def["field_id"]
                    manifest_col = f"partition_field_{fid}"
                    if manifest_col in manifest_table.column_names:
                        partition_fields.append(pa.field(fid, manifest_table.schema.field(manifest_col).type))
                return pa.schema(list(data_schema) + partition_fields)
            except Exception:
                continue

        raise ValueError(f"Could not determine namespace schema from manifest at {self._namespace_uri}")

    def _schema_from_json(self, ns_schema_json: dict[str, Any]) -> pa.Schema:
        fields: list[pa.Field] = []
        for f in ns_schema_json["fields"]:
            arrow_type = _ARROW_TYPE_MAP.get(f["type"], pa.utf8())
            fields.append(pa.field(f["name"], arrow_type))
        return pa.schema(fields)

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
        return [
            self.display_name(),
            f"Schema = {self.schema()}",
            f"Partitions = {len(self._manifest_table)} datasets",
        ]

    def push_filters(self, filters: list[PyExpr]) -> tuple[list[PyExpr], list[PyExpr]]:
        pushed = []
        remaining = []
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
        from .utils import namespace_physical_path

        required_columns: list[str] | None
        if pushdowns.columns is None:
            required_columns = None
        else:
            filter_required_column_names = pushdowns.filter_required_column_names()
            required_columns = list(
                set(
                    pushdowns.columns
                    if filter_required_column_names is None
                    else pushdowns.columns + filter_required_column_names
                )
            )

        pushed_arrow_filter = combine_filters_to_arrow(self._pushed_filters)

        has_read_version = "read_version" in self._manifest_table.column_names

        for i in range(self._manifest_table.num_rows):
            partition_values = {}
            for field_def in self._partition_spec.get("fields", []):
                fid = field_def["field_id"]
                manifest_col = f"partition_field_{fid}"
                if manifest_col in self._manifest_table.column_names:
                    partition_values[fid] = self._manifest_table.column(manifest_col)[i].as_py()

            if pushdowns.partition_filters is not None:
                if not self._evaluate_partition_filter(pushdowns.partition_filters, partition_values):
                    continue

            object_id = self._manifest_table.column("object_id")[i].as_py()
            ds_uri = namespace_physical_path(self._namespace_uri, object_id)

            read_version: int | None = None
            if has_read_version:
                read_version = self._manifest_table.column("read_version")[i].as_py()

            yield from self._create_scan_tasks_for_partition(
                ds_uri, partition_values, required_columns, pushed_arrow_filter, pushdowns, read_version
            )

    def _evaluate_partition_filter(self, partition_filter: PyExpr, partition_values: dict[str, Any]) -> bool:
        try:
            pv_arrays = {}
            for fid, value in partition_values.items():
                idx = self._namespace_pa_schema.get_field_index(fid)
                if idx >= 0:
                    arrow_type = self._namespace_pa_schema.field(idx).type
                else:
                    arrow_type = pa.utf8()
                pv_arrays[fid] = pa.array([value], type=arrow_type)

            from daft.expressions.expressions import ExpressionsProjection

            pv_rb = RecordBatch.from_pydict(pv_arrays)
            expr = Expression._from_pyexpr(partition_filter)
            result = pv_rb.eval_expression_list(ExpressionsProjection([expr]))
            arrow_table = result.to_arrow_table()
            col = arrow_table.column(0)
            if len(col) == 0:
                return False
            val = col[0].as_py()
            return val is True
        except Exception as e:
            logger.warning("Failed to evaluate partition filter, including partition: %s", e)
            return True

    def _create_scan_tasks_for_partition(
        self,
        ds_uri: str,
        partition_values: dict[str, Any],
        required_columns: list[str] | None,
        pushed_arrow_filter: Optional["pa.compute.Expression"],
        pushdowns: PyPushdowns,
        read_version: int | None = None,
    ) -> Iterator[ScanTask]:
        import lance

        try:
            ds = lance.dataset(ds_uri, storage_options=self._storage_options, version=read_version)
        except Exception as e:
            logger.warning("Failed to open partition dataset at %s: %s", ds_uri, e)
            return

        open_kwargs = getattr(ds, "_lance_open_kwargs", None)
        fragments = list(ds.get_fragments())

        def _estimate_size(frag: Any) -> int:
            if frag.metadata and frag.metadata.files:
                return sum(f.file_size_bytes for f in frag.metadata.files if f.file_size_bytes is not None)
            return 0

        def _make_scan_task(frag_ids: list[int], num_rows: int, size_bytes: int) -> ScanTask:
            return ScanTask.python_factory_func_scan_task(
                module=_lancedb_namespace_factory_function.__module__,
                func_name=_lancedb_namespace_factory_function.__name__,
                func_args=(
                    ds_uri,
                    open_kwargs,
                    frag_ids,
                    required_columns,
                    pushed_arrow_filter,
                    pushdowns.limit,
                    partition_values,
                    self._partition_col_types,
                    read_version,
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
            current_ids: list[int] = []
            group_rows = 0
            group_bytes = 0
            for fragment in fragments:
                rows = fragment.count_rows(pushed_arrow_filter)
                if rows == 0:
                    continue
                current_ids.append(fragment.fragment_id)
                group_rows += rows
                group_bytes += _estimate_size(fragment)
                if len(current_ids) >= group_size:
                    yield _make_scan_task(current_ids, group_rows, group_bytes)
                    current_ids = []
                    group_rows = 0
                    group_bytes = 0
            if current_ids:
                yield _make_scan_task(current_ids, group_rows, group_bytes)
        else:
            for fragment in fragments:
                num_rows = fragment.count_rows(pushed_arrow_filter)
                if num_rows == 0:
                    continue
                yield _make_scan_task([fragment.fragment_id], num_rows, _estimate_size(fragment))
