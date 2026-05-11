"""Scan operator for Lance Partitioned Namespaces.

Reads ``__manifest`` to discover the partition tables, advertises partition
columns to Daft's planner, pushes filters into both partition pruning (via the
manifest's ``partition_field_*`` columns) and per-partition Arrow filter
pushdown, and emits one ScanTask per fragment (or fragment group).
"""

# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterator
from typing import Any, Optional

from daft.daft import PyExpr, PyPartitionField, PyPushdowns, PyRecordBatch, ScanTask
from daft.datatype import DataType, _ensure_registered_super_ext_type
from daft.dependencies import pa
from daft.expressions import Expression
from daft.expressions.expressions import ExpressionsProjection
from daft.io.partitioning import PartitionTransform
from daft.io.scan import ScanOperator, make_partition_field
from daft.logical.schema import Field, Schema
from daft.recordbatch import RecordBatch

from ..pushdowns import SupportsPushdownFilters
from .lance_namespace import (
    COL_LOCATION,
    COL_OBJECT_ID,
    COL_OBJECT_TYPE,
    COL_READ_VERSION,
    LANCE_FIELD_ID_KEY,
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


def _schema_field_by_lance_field_id(namespace_schema: pa.Schema, target_id: int) -> pa.Field | None:
    """Find a schema field by its `lance:field_id` metadata value.

    Per the partitioning spec, `source_ids` references the logical
    ``lance:field_id`` stored in field metadata — not the positional index of
    the field in the Arrow schema. Field IDs can be non-sequential or start
    above zero in spec-compliant namespaces written by other tools.
    """
    target = str(target_id)
    for field in namespace_schema:
        md = field.metadata or {}
        raw = md.get(LANCE_FIELD_ID_KEY.encode(), md.get(LANCE_FIELD_ID_KEY))
        if raw is None:
            continue
        raw_str = raw.decode() if isinstance(raw, (bytes, bytearray)) else raw
        if raw_str == target:
            return field
    return None


def _build_partition_keys(partition_spec: dict[str, Any], namespace_schema: pa.Schema) -> list[PyPartitionField]:
    fields: list[PyPartitionField] = []
    for field_def in partition_spec.get("fields", []):
        field_id = field_def["field_id"]
        source_ids = field_def.get("source_ids", [])

        source_arrow_field: pa.Field | None = None
        if source_ids:
            source_arrow_field = _schema_field_by_lance_field_id(namespace_schema, int(source_ids[0]))
        if source_arrow_field is None and field_id in namespace_schema.names:
            source_arrow_field = namespace_schema.field(namespace_schema.get_field_index(field_id))
        if source_arrow_field is None:
            raise ValueError(
                f"Cannot resolve source field for partition field {field_id!r}; "
                f"source_ids={source_ids} not found by lance:field_id and no schema field named {field_id!r}."
            )

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


def _inject_fragment_id(rb: pa.RecordBatch, fragment_id: int) -> pa.RecordBatch:
    """Append a constant fragment_id column to a record batch."""
    columns = list(rb.columns) + [pa.array([fragment_id] * len(rb), type=pa.int64())]
    names = list(rb.schema.names) + ["fragment_id"]
    return pa.RecordBatch.from_arrays(columns, names=names)


# Synthetic columns that don't live in a Lance leaf's data schema but are
# injected by us (or by Lance scan options) when reading a namespace.
_SYNTHETIC_LEAF_COLS = frozenset({"fragment_id", "_rowaddr", "_rowid"})


def _compute_leaf_projection(
    leaf_names: set[str],
    required_columns: list[str] | None,
    partition_field_names: set[str],
) -> tuple[list[str] | None, bool]:
    """Compute the leaf-level projection or skip the leaf entirely.

    Different leaves in a partitioned namespace may have different schemas
    (e.g., after a partial column merge, or topic-keyed partitioning where
    each topic has its own signal set). Passing a column to
    ``lance.scanner(columns=...)`` that the leaf doesn't have raises
    ``LanceError(Schema)``; this is a metadata-only check that decides up
    front what to scan from each leaf.

    Behavior is per-leaf:

    - If the leaf has *none* of the requested data columns, **skip** it —
      the leaf can't contribute meaningful rows for this query.
    - Otherwise, scan the leaf with the intersection of requested columns
      and the leaf's schema. Columns requested but absent from the leaf
      are filled with NULL by Daft's planner at a higher layer; we do not
      fabricate column data here.

    Args:
        leaf_names: Column names in the leaf's Arrow schema.
        required_columns: Daft's projection, or None for "all columns".
        partition_field_names: Set of partition field ids — injected after
            the scan, so not requested from the leaf.

    Returns:
        ``(leaf_cols, skip)``:

        - ``leaf_cols`` — list to pass to ``ds.scanner(columns=...)`` (or
          ``None`` if the user requested no data columns).
        - ``skip`` — True iff the user requested at least one data column
          and the leaf has none of them; the caller should drop this leaf.
    """
    if required_columns is None:
        return None, False

    data_cols_requested = [
        c for c in required_columns if c not in partition_field_names and c not in _SYNTHETIC_LEAF_COLS
    ]
    if not data_cols_requested:
        return None, False

    leaf_cols = [c for c in data_cols_requested if c in leaf_names]
    if not leaf_cols:
        return None, True
    return leaf_cols, False


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
    include_fragment_id: bool = False,
    default_scan_options: dict[str, Any] | None = None,
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
    if default_scan_options is not None:
        kwargs["default_scan_options"] = default_scan_options
    ds = lance.dataset(ds_uri, **kwargs)

    pv = partition_values or {}
    pft = partition_field_types or {}

    # Leaves in a partitioned namespace may have disjoint schemas (after a
    # partial column merge or topic-keyed partitioning). Skip this leaf if
    # any requested data column is absent — no NULL fabrication.
    leaf_names = set(ds.schema.names)
    leaf_cols, skip = _compute_leaf_projection(leaf_names, required_columns, set(pv.keys()))
    if skip:
        return

    def _emit(rb: pa.RecordBatch, fragment_id: int | None) -> Iterator[PyRecordBatch]:
        rb = _inject_partition_columns(rb, pv, pft, required_columns)
        if include_fragment_id and fragment_id is not None:
            rb = _inject_fragment_id(rb, fragment_id)
        yield RecordBatch.from_arrow_record_batches([rb], rb.schema)._recordbatch

    if fragment_ids is None:
        # Scanning the whole dataset — Lance doesn't expose per-row fragment ids
        # through the dataset-level scanner. include_fragment_id requires the
        # per-fragment path.
        scanner = ds.scanner(columns=leaf_cols, filter=filter, limit=limit)
        for rb in scanner.to_batches():
            yield from _emit(rb, None)
    else:
        rows_yielded = 0
        for fid in fragment_ids:
            if limit is not None and rows_yielded >= limit:
                break
            fragment = ds.get_fragment(fid)
            fragment_limit = (limit - rows_yielded) if limit is not None else None
            scanner = ds.scanner(
                fragments=[fragment],
                columns=leaf_cols,
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
                yield from _emit(rb, fid)
                rows_yielded += len(rb)


def _lance_namespace_stitched_factory_function(
    leaf_uris: list[str],
    leaf_read_versions: list[int | None],
    required_columns: list[str] | None = None,
    filter: Optional["pa.compute.Expression"] = None,
    limit: int | None = None,
    partition_values: dict[str, Any] | None = None,
    partition_field_types: dict[str, pa.DataType] | None = None,
    storage_options: dict[str, str] | None = None,
    default_scan_options: dict[str, Any] | None = None,
) -> Iterator[PyRecordBatch]:
    """Positional horizontal merge across sibling leaves of a partitioned namespace.

    Sibling leaves under the same parent intermediate namespace carry
    column-wise slices of the same logical rows (e.g., a `(robot_id, topic)`
    namespace where each topic stores its own signal column for that robot,
    written in lockstep). At read time, we scan all such siblings and zip
    their columns positionally — index 0 in leaf A corresponds to index 0
    in leaf B. This is the partitioned-namespace analog of Lance's
    in-fragment column-stitching, which is itself positional.

    Requires identical row counts across stitched leaves; mismatches raise.
    """
    try:
        import lance
    except ImportError as e:
        raise ImportError(
            "Unable to import the `lance` package, please ensure that Daft is installed with the lance extra dependency: `pip install daft[lance]`"
        ) from e

    pv = partition_values or {}
    pft = partition_field_types or {}

    # `_rowid` is requested so we can cheaply verify alignment at the
    # boundaries; we drop it after stitching. (Format guarantees the
    # zip is safe; we just sanity-check first+last _rowid per leaf.)
    leaf_scan_opts: dict[str, Any] = dict(default_scan_options or {})
    leaf_scan_opts["with_row_id"] = True

    # Peek schemas first so we only scan leaves that contribute new data.
    # E.g., for select(ts, speed) on a (speed, caption) stitch group, the
    # caption leaf has [ts, caption] but `ts` is already covered by the
    # speed leaf and `caption` wasn't requested — caption leaf scans nothing.
    leaf_handles: list[tuple[Any, list[str]]] = []
    claimed: set[str] = set()
    data_cols_requested = (
        None
        if required_columns is None
        else [c for c in required_columns if c not in pv and c not in _SYNTHETIC_LEAF_COLS]
    )
    for uri, version in zip(leaf_uris, leaf_read_versions):
        open_kwargs: dict[str, Any] = {"default_scan_options": leaf_scan_opts}
        if storage_options is not None:
            open_kwargs["storage_options"] = storage_options
        if version is not None:
            open_kwargs["version"] = version
        ds = lance.dataset(uri, **open_kwargs)
        leaf_names = set(ds.schema.names)

        if data_cols_requested is None:
            # No projection → scan every column we haven't already taken.
            new_cols = [c for c in ds.schema.names if c not in claimed and c not in _SYNTHETIC_LEAF_COLS]
        else:
            new_cols = [c for c in data_cols_requested if c in leaf_names and c not in claimed]

        if not new_cols:
            continue
        leaf_handles.append((ds, new_cols))
        claimed.update(new_cols)

    if not leaf_handles:
        return

    leaf_tables: list[pa.Table] = []
    for ds, new_cols in leaf_handles:
        leaf_data_cols = [c for c in ds.schema.names if c not in _SYNTHETIC_LEAF_COLS]
        if set(new_cols) == set(leaf_data_cols):
            # Full leaf read — let Lance take the no-projection fast path.
            # Passing an explicit `columns=` list scales poorly with column
            # count (>30x slower at 250 cols).
            leaf_tables.append(ds.to_table(filter=filter))
        else:
            leaf_tables.append(ds.to_table(columns=list(new_cols) + ["_rowid"], filter=filter))

    # Positional horizontal merge — the format guarantees siblings are
    # written in lockstep, so a zip is safe and ~10× cheaper than a join.
    # Verify (1) all leaves share the same row count, (2) first+last
    # `_rowid` match across leaves; bail loudly if either invariant
    # breaks.
    row_counts = {t.num_rows for t in leaf_tables}
    if len(row_counts) != 1:
        raise ValueError(
            f"Cannot stitch sibling leaves with mismatched row counts: {sorted(row_counts)}. "
            "Stitched leaves must be written in lockstep with the same number of rows."
        )
    n_rows = next(iter(row_counts))
    if n_rows > 0:
        boundary_rowids: list[tuple[Any, Any]] = []
        for tbl in leaf_tables:
            if "_rowid" not in tbl.column_names:
                continue
            rid = tbl.column("_rowid")
            boundary_rowids.append((rid[0].as_py(), rid[n_rows - 1].as_py()))
        if len(set(boundary_rowids)) > 1:
            raise ValueError(
                f"Cannot stitch sibling leaves with misaligned `_rowid`s "
                f"(first/last per leaf: {boundary_rowids}). "
                "Stitched leaves must be written in lockstep with identical row ids."
            )

    # Build the stitched table in a single pa.Table.from_arrays call.
    # `pa.Table.append_column` is O(current_n_cols), so a per-column loop is
    # O(N²) in total column count — pathological for wide column-stitched
    # leaves (e.g. signal datasets with thousands of columns).
    arrays: list[Any] = []
    names: list[str] = []
    seen: set[str] = set()
    for tbl in leaf_tables:
        for name in tbl.column_names:
            if name == "_rowid" or name in seen:
                continue
            arrays.append(tbl.column(name))
            names.append(name)
            seen.add(name)

    # Inject partition values that are constant across the stitch group.
    n_rows = leaf_tables[0].num_rows
    for col_name, value in pv.items():
        if col_name in seen:
            continue
        if required_columns is not None and col_name not in required_columns:
            continue
        dtype = pft.get(col_name, pa.utf8())
        arrays.append(pa.array([value] * n_rows, type=dtype))
        names.append(col_name)
        seen.add(col_name)

    combined = pa.Table.from_arrays(arrays, names=names)
    if limit is not None and combined.num_rows > limit:
        combined = combined.slice(0, limit)

    for rb in combined.to_batches():
        yield RecordBatch.from_arrow_record_batches([rb], rb.schema)._recordbatch


class LanceNamespaceScanOperator(ScanOperator, SupportsPushdownFilters):
    def __init__(
        self,
        namespace_uri: str,
        storage_options: dict[str, str] | None = None,
        fragment_group_size: int | None = None,
        include_fragment_id: bool = False,
        default_scan_options: dict[str, Any] | None = None,
    ) -> None:
        self._namespace_uri = namespace_uri.rstrip("/")
        self._storage_options = storage_options
        self._fragment_group_size = fragment_group_size
        self._include_fragment_id = include_fragment_id
        self._default_scan_options = default_scan_options
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

        # Reflect `with_row_address` and similar scan options on the namespace
        # schema so Daft's planner sees the surfaced columns.
        if self._default_scan_options:
            extras: list[pa.Field] = []
            opts = self._default_scan_options
            if opts.get("with_row_address") or opts.get("with_rowaddr"):
                extras.append(pa.field("_rowaddr", pa.uint64()))
            if opts.get("with_row_id"):
                extras.append(pa.field("_rowid", pa.uint64()))
            if extras:
                namespace_schema = pa.schema(list(namespace_schema) + extras, metadata=namespace_schema.metadata)

        if self._include_fragment_id:
            namespace_schema = pa.schema(
                list(namespace_schema) + [pa.field("fragment_id", pa.int64())],
                metadata=namespace_schema.metadata,
            )

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

        # Group manifest table rows by their parent intermediate namespace.
        # Two leaves whose object_ids differ only in the last namespace segment
        # are stitch-siblings: they're independently-written Lance datasets
        # representing column-wise slices of the same logical rows (matched
        # by Lance's stable `_rowid`). This is the partitioned-namespace
        # analog of Lance's in-fragment column-stitching pattern.
        parent_to_indices: dict[str, list[int]] = defaultdict(list)
        for i in range(self._manifest_table.num_rows):
            if self._manifest_table.column(COL_OBJECT_TYPE)[i].as_py() != OBJECT_TYPE_TABLE:
                continue
            oid = self._manifest_table.column(COL_OBJECT_ID)[i].as_py()
            parts = oid.split("$")
            # Parts shape: ["v<N>", <ns_1>, ..., <ns_K>, "dataset"].
            # A leaf can have siblings only if there is at least one namespace
            # above it besides the spec-version: i.e., parts has at least 4
            # segments. With fewer, every leaf is its own parent and there's
            # nothing to stitch.
            if len(parts) >= 4:
                parent = "$".join(parts[:-2])
            else:
                parent = oid  # unique parent => no stitching candidates
            parent_to_indices[parent].append(i)

        # Namespace data columns (everything except partition cols + synthetic).
        namespace_data_cols = {
            n for n in self._namespace_pa_schema.names if n not in partition_field_ids
        } - _SYNTHETIC_LEAF_COLS

        for parent, indices in parent_to_indices.items():
            stitch = len(indices) > 1 and self._siblings_are_column_partitioned(indices, namespace_data_cols)
            if not stitch:
                for i in indices:
                    yield from self._emit_single_leaf_tasks(
                        i,
                        partition_field_ids,
                        required_columns,
                        pushed_arrow_filter,
                        pushdowns,
                    )
            else:
                yield from self._emit_stitched_task(
                    indices,
                    partition_field_ids,
                    required_columns,
                    pushed_arrow_filter,
                    pushdowns,
                )

    def _siblings_are_column_partitioned(self, indices: list[int], namespace_data_cols: set[str]) -> bool:
        """Decide whether sibling leaves are column-partitioned (and so should stitch).

        Sniff the first leaf's data schema. If it's a *proper subset* of the
        namespace's data columns, the namespace's leaves carry column-wise
        slices of a wider logical row — stitch them on `_rowid`. If the first
        leaf has the full namespace data schema, all leaves probably do, and
        we treat them as ordinary row-partitioned siblings to be unioned.

        One leaf-open per parent group; cheap relative to per-leaf scan tasks.
        Returns False on any error opening the leaf (safe default).
        """
        import lance

        loc = self._manifest_table.column(COL_LOCATION)[indices[0]].as_py()
        if loc is None:
            return False
        uri = f"{self._namespace_uri}/{loc}"
        try:
            ds = lance.dataset(uri, storage_options=self._storage_options)
        except Exception as e:
            logger.warning("Sibling stitching check: could not open %s: %s", uri, e)
            return False
        leaf_data_cols = set(ds.schema.names) - _SYNTHETIC_LEAF_COLS
        return leaf_data_cols < namespace_data_cols

    def _emit_single_leaf_tasks(
        self,
        i: int,
        partition_field_ids: list[str],
        required_columns: list[str] | None,
        pushed_arrow_filter: Optional["pa.compute.Expression"],
        pushdowns: PyPushdowns,
    ) -> Iterator[ScanTask]:
        location = self._manifest_table.column(COL_LOCATION)[i].as_py()
        if location is None:
            logger.warning("Skipping table row at index %d: missing location", i)
            return

        partition_values: dict[str, Any] = {}
        for fid in partition_field_ids:
            col = f"{PARTITION_FIELD_PREFIX}{fid}"
            if col in self._manifest_table.column_names:
                partition_values[fid] = self._manifest_table.column(col)[i].as_py()

        if pushdowns.partition_filters is not None and not self._partition_filter_matches(
            pushdowns.partition_filters, partition_values
        ):
            return

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

    def _emit_stitched_task(
        self,
        indices: list[int],
        partition_field_ids: list[str],
        required_columns: list[str] | None,
        pushed_arrow_filter: Optional["pa.compute.Expression"],
        pushdowns: PyPushdowns,
    ) -> Iterator[ScanTask]:
        leaf_uris: list[str] = []
        leaf_read_versions: list[int | None] = []

        # Partition values shared by all stitch-siblings: those that match
        # across the indices. The varying ones (typically the last partition
        # col — e.g., "topic") become NULL in stitched output rows because
        # the row aggregates across multiple values of that field.
        per_index_pvs: list[dict[str, Any]] = []
        for i in indices:
            pv: dict[str, Any] = {}
            for fid in partition_field_ids:
                col = f"{PARTITION_FIELD_PREFIX}{fid}"
                if col in self._manifest_table.column_names:
                    pv[fid] = self._manifest_table.column(col)[i].as_py()
            per_index_pvs.append(pv)

        shared_pvs: dict[str, Any] = {}
        if per_index_pvs:
            for fid in partition_field_ids:
                first = per_index_pvs[0].get(fid)
                if all(p.get(fid) == first for p in per_index_pvs):
                    shared_pvs[fid] = first

        if pushdowns.partition_filters is not None and not self._partition_filter_matches(
            pushdowns.partition_filters, shared_pvs
        ):
            return

        for i in indices:
            location = self._manifest_table.column(COL_LOCATION)[i].as_py()
            if location is None:
                logger.warning("Skipping stitched leaf at index %d: missing location", i)
                continue
            leaf_uris.append(f"{self._namespace_uri}/{location}")
            rv: int | None = None
            if COL_READ_VERSION in self._manifest_table.column_names:
                raw = self._manifest_table.column(COL_READ_VERSION)[i].as_py()
                if raw is not None:
                    rv = int(raw)
            leaf_read_versions.append(rv)

        if not leaf_uris:
            return

        # Stitched task: scan all sibling leaves and join their columns on
        # Lance's stable `_rowid`. One task per stitch group.
        yield ScanTask.python_factory_func_scan_task(
            module=_lance_namespace_stitched_factory_function.__module__,
            func_name=_lance_namespace_stitched_factory_function.__name__,
            func_args=(
                leaf_uris,
                leaf_read_versions,
                required_columns,
                pushed_arrow_filter,
                pushdowns.limit,
                shared_pvs,
                self._partition_field_types,
                self._storage_options,
                self._default_scan_options,
            ),
            schema=self._schema._schema,
            num_rows=None,
            size_bytes=None,
            pushdowns=pushdowns,
            stats=None,
            source_name=self.display_name(),
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
            open_kwargs: dict[str, Any] = {
                "storage_options": self._storage_options,
                "version": read_version,
            }
            if self._default_scan_options is not None:
                open_kwargs["default_scan_options"] = self._default_scan_options
            ds = lance.dataset(ds_uri, **open_kwargs)
        except Exception as e:
            logger.warning("Failed to open partition dataset at %s: %s", ds_uri, e)
            return

        # Metadata-only check: if this leaf can't satisfy the projection
        # (e.g., it's missing a requested column because of disjoint per-leaf
        # schemas), don't emit scan tasks for it at all.
        leaf_names = set(ds.schema.names)
        _, skip = _compute_leaf_projection(leaf_names, required_columns, set(partition_values.keys()))
        if skip:
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
                    self._include_fragment_id,
                    self._default_scan_options,
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
