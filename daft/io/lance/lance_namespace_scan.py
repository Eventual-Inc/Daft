"""DataSource for Lance Partitioned Namespaces.

Reads the ``__manifest`` Lance table to discover partition tables, advertises
partition columns to Daft's planner, pushes filters down (partition-level via
the manifest, value-level into each leaf's Arrow scanner), and emits
``DataSourceTask``s — one per Lance fragment for ordinary leaves, one per
sibling-leaf stitch group for column-partitioned siblings.
"""

# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, Any, Optional

from daft.daft import PyExpr
from daft.datatype import DataType, _ensure_registered_super_ext_type
from daft.dependencies import pa
from daft.expressions import Expression
from daft.expressions.expressions import ExpressionsProjection
from daft.io.partitioning import PartitionField, PartitionTransform
from daft.io.source import DataSource, DataSourceTask
from daft.logical.schema import Field, Schema
from daft.recordbatch import RecordBatch

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

if TYPE_CHECKING:
    from daft.io.pushdowns import Pushdowns

logger = logging.getLogger(__name__)


# Synthetic columns that don't live in a Lance leaf's data schema but are
# injected by us (or by Lance scan options) when reading a namespace.
_SYNTHETIC_LEAF_COLS = frozenset({"fragment_id", "_rowaddr", "_rowid"})


# ===========================================================================
# Manifest parsing helpers
# ===========================================================================


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


def _build_partition_fields(partition_spec: dict[str, Any], namespace_schema: pa.Schema) -> list[PartitionField]:
    """Convert a partition spec into Daft's ``PartitionField`` objects."""
    fields: list[PartitionField] = []
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
        fields.append(PartitionField.create(result_field, source_field=source_field, transform=transform))
    return fields


# ===========================================================================
# Per-batch post-processing helpers
# ===========================================================================


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

    Returns ``(leaf_cols, skip)``:

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


# ===========================================================================
# DataSourceTask implementations
# ===========================================================================


class _LanceLeafTask(DataSourceTask):
    """Reads one (or more) fragments of a single Lance leaf dataset.

    Used for non-stitched leaves — both row-partitioned siblings (siblings
    with the same schema, treated as separate row groups) and isolated
    leaves with no siblings under their parent intermediate namespace.
    """

    def __init__(
        self,
        *,
        ds_uri: str,
        fragment_ids: list[int] | None,
        required_columns: list[str] | None,
        filter: Optional["pa.compute.Expression"],
        limit: int | None,
        partition_values: dict[str, Any],
        partition_field_types: dict[str, pa.DataType],
        read_version: int | None,
        storage_options: dict[str, str] | None,
        include_fragment_id: bool,
        default_scan_options: dict[str, Any] | None,
        schema: Schema,
    ) -> None:
        self._ds_uri = ds_uri
        self._fragment_ids = fragment_ids
        self._required_columns = required_columns
        self._filter = filter
        self._limit = limit
        self._partition_values = partition_values
        self._partition_field_types = partition_field_types
        self._read_version = read_version
        self._storage_options = storage_options
        self._include_fragment_id = include_fragment_id
        self._default_scan_options = default_scan_options
        self._schema = schema

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        try:
            import lance
        except ImportError as e:
            raise ImportError(
                "Unable to import the `lance` package, please ensure that Daft is installed with the lance "
                "extra dependency: `pip install daft[lance]`"
            ) from e

        kwargs: dict[str, Any] = {}
        if self._storage_options is not None:
            kwargs["storage_options"] = self._storage_options
        if self._read_version is not None:
            kwargs["version"] = self._read_version
        if self._default_scan_options is not None:
            kwargs["default_scan_options"] = self._default_scan_options
        ds = lance.dataset(self._ds_uri, **kwargs)

        # Decide which columns to actually pull from this leaf — leaves can
        # have disjoint schemas (after partial merge or topic-keyed
        # partitioning); Lance errors if we pass a column it doesn't have.
        leaf_names = set(ds.schema.names)
        leaf_cols, skip = _compute_leaf_projection(
            leaf_names, self._required_columns, set(self._partition_values.keys())
        )
        if skip:
            return

        def _emit(rb: pa.RecordBatch, fragment_id: int | None) -> RecordBatch:
            rb = _inject_partition_columns(
                rb, self._partition_values, self._partition_field_types, self._required_columns
            )
            if self._include_fragment_id and fragment_id is not None:
                rb = _inject_fragment_id(rb, fragment_id)
            return RecordBatch.from_arrow_record_batches([rb], rb.schema)

        if self._fragment_ids is None:
            # Whole-dataset scan — Lance doesn't expose per-row fragment
            # ids through the dataset-level scanner, so include_fragment_id
            # has no useful value to emit here.
            scanner = ds.scanner(columns=leaf_cols, filter=self._filter, limit=self._limit)
            for rb in scanner.to_batches():
                yield _emit(rb, None)
            return

        rows_yielded = 0
        for fid in self._fragment_ids:
            if self._limit is not None and rows_yielded >= self._limit:
                break
            fragment = ds.get_fragment(fid)
            fragment_limit = (self._limit - rows_yielded) if self._limit is not None else None
            scanner = ds.scanner(
                fragments=[fragment],
                columns=leaf_cols,
                filter=self._filter,
                limit=fragment_limit,
            )
            for rb in scanner.to_batches():
                if self._limit is not None:
                    remaining = self._limit - rows_yielded
                    if remaining <= 0:
                        break
                    if len(rb) > remaining:
                        rb = rb.slice(0, remaining)
                yield _emit(rb, fid)
                rows_yielded += len(rb)


class _LanceStitchedTask(DataSourceTask):
    """Reads a stitch group of sibling Lance leaves and zips their columns.

    Sibling leaves under the same parent intermediate namespace carry
    column-wise slices of the same logical rows (e.g., a ``(robot_id, topic)``
    namespace where each topic stores one signal column per robot, written
    in lockstep). At read time we open all such siblings, scan each with the
    per-leaf projection, and zip their columns positionally — the analog of
    Lance's in-fragment column-stitching but across separately-committed
    leaves. The format contract makes the zip safe; we sanity-check first
    and last ``_rowid`` per leaf to fail loudly if alignment drifts.
    """

    def __init__(
        self,
        *,
        leaf_uris: list[str],
        leaf_read_versions: list[int | None],
        required_columns: list[str] | None,
        filter: Optional["pa.compute.Expression"],
        limit: int | None,
        partition_values: dict[str, Any],
        partition_field_types: dict[str, pa.DataType],
        storage_options: dict[str, str] | None,
        default_scan_options: dict[str, Any] | None,
        schema: Schema,
    ) -> None:
        self._leaf_uris = leaf_uris
        self._leaf_read_versions = leaf_read_versions
        self._required_columns = required_columns
        self._filter = filter
        self._limit = limit
        self._partition_values = partition_values
        self._partition_field_types = partition_field_types
        self._storage_options = storage_options
        self._default_scan_options = default_scan_options
        self._schema = schema

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        import asyncio

        try:
            import lance
        except ImportError as e:
            raise ImportError(
                "Unable to import the `lance` package, please ensure that Daft is installed with the lance "
                "extra dependency: `pip install daft[lance]`"
            ) from e

        # `_rowid` is requested so we can cheaply verify alignment at the
        # boundaries; we drop it after stitching.
        leaf_scan_opts: dict[str, Any] = dict(self._default_scan_options or {})
        leaf_scan_opts["with_row_id"] = True

        data_cols_requested = (
            None
            if self._required_columns is None
            else [
                c for c in self._required_columns if c not in self._partition_values and c not in _SYNTHETIC_LEAF_COLS
            ]
        )

        # Phase 1 — open all leaves in parallel. Lance dataset construction
        # is dominated by metadata I/O; with hundreds of column-partitioned
        # siblings the sequential cost (one open per leaf) dwarfs the actual
        # data read for narrow projections. Thread-pool fans these out.
        def _open(uri: str, version: int | None) -> tuple[Any, set[str]]:
            open_kwargs: dict[str, Any] = {"default_scan_options": leaf_scan_opts}
            if self._storage_options is not None:
                open_kwargs["storage_options"] = self._storage_options
            if version is not None:
                open_kwargs["version"] = version
            ds = lance.dataset(uri, **open_kwargs)
            return ds, set(ds.schema.names)

        opens = await asyncio.gather(
            *(asyncio.to_thread(_open, uri, v) for uri, v in zip(self._leaf_uris, self._leaf_read_versions))
        )

        # Phase 2 — deterministic "first leaf wins" claim in original order.
        # Has to be serial because each leaf's contribution depends on what
        # earlier leaves already claimed.
        claimed: set[str] = set()
        contributors: list[tuple[Any, list[str]]] = []
        for ds, leaf_names in opens:
            if data_cols_requested is None:
                new_cols = [c for c in ds.schema.names if c not in claimed and c not in _SYNTHETIC_LEAF_COLS]
            else:
                new_cols = [c for c in data_cols_requested if c in leaf_names and c not in claimed]
            if not new_cols:
                continue
            contributors.append((ds, new_cols))
            claimed.update(new_cols)

        if not contributors:
            return

        # Phase 3 — read contributing leaves' tables in parallel.
        def _read(ds: Any, new_cols: list[str]) -> pa.Table:
            scan_cols = list(new_cols) + ["_rowid"]
            return ds.to_table(columns=scan_cols, filter=self._filter)

        leaf_tables = list(await asyncio.gather(*(asyncio.to_thread(_read, ds, cols) for ds, cols in contributors)))

        # Positional horizontal merge — the format guarantees siblings are
        # written in lockstep. Verify row count match across leaves and
        # spot-check first+last `_rowid`; bail loudly if either breaks.
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

        seen: set[str] = {"_rowid"}  # internal; never propagate
        combined: pa.Table = leaf_tables[0]
        if "_rowid" in combined.column_names:
            combined = combined.drop(["_rowid"])
        for n in combined.column_names:
            seen.add(n)
        for tbl in leaf_tables[1:]:
            for name in tbl.column_names:
                if name in seen:
                    continue
                combined = combined.append_column(name, tbl.column(name))
                seen.add(name)

        # Inject partition values that are constant across the stitch group.
        for col_name, value in self._partition_values.items():
            if col_name in combined.column_names:
                continue
            if self._required_columns is not None and col_name not in self._required_columns:
                continue
            dtype = self._partition_field_types.get(col_name, pa.utf8())
            combined = combined.append_column(col_name, pa.array([value] * combined.num_rows, type=dtype))

        if self._limit is not None and combined.num_rows > self._limit:
            combined = combined.slice(0, self._limit)

        for rb in combined.to_batches():
            yield RecordBatch.from_arrow_record_batches([rb], rb.schema)


# ===========================================================================
# The DataSource itself
# ===========================================================================


class LanceNamespaceDataSource(DataSource):
    """DataSource for a Lance Partitioned Namespace.

    Opens the ``__manifest`` Lance table at construction, parses the partition
    spec and namespace schema, and at scan time emits one ``DataSourceTask``
    per Lance fragment (for ordinary leaves) or per sibling-stitch group
    (when sibling leaves carry column-wise slices of the same logical rows).
    """

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

        # Reflect `with_row_address` / `with_row_id` scan options + the
        # `fragment_id` virtual column on the namespace schema so Daft's
        # planner sees them as real columns.
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
        self._partition_fields = _build_partition_fields(partition_spec, namespace_schema)

        # Map partition field id -> Arrow type for constant-column injection.
        self._partition_field_types: dict[str, pa.DataType] = {}
        for field_def in partition_spec.get("fields", []):
            fid = field_def["field_id"]
            rt = field_def.get("result_type")
            if isinstance(rt, dict):
                self._partition_field_types[fid] = json_arrow_to_arrow_type(rt)
            elif fid in namespace_schema.names:
                self._partition_field_types[fid] = namespace_schema.field(fid).type

    # ---- DataSource protocol ----------------------------------------------

    @property
    def name(self) -> str:
        return f"LanceNamespaceDataSource({self._namespace_uri})"

    @property
    def schema(self) -> Schema:
        return self._schema

    def get_partition_fields(self) -> list[PartitionField]:
        return self._partition_fields

    def push_filters(self, filters: list[PyExpr]) -> tuple[list[PyExpr], list[PyExpr]]:
        """Push filters that round-trip cleanly to Arrow expressions.

        The rest are kept for post-scan evaluation.
        """
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

    async def get_tasks(self, pushdowns: "Pushdowns") -> AsyncIterator[DataSourceTask]:
        """Walk the manifest and yield one task per fragment or stitch group.

        Decides per parent group whether to stitch siblings or treat them as
        single leaves, then yields one task per fragment (single-leaf path)
        or one task per stitch group.
        """
        if pushdowns.columns is None:
            required_columns: list[str] | None = None
        else:
            filter_req = pushdowns.filter_required_column_names() or set()
            required_columns = list(set(list(pushdowns.columns) + list(filter_req)))

        pushed_arrow_filter = combine_filters_to_arrow(self._pushed_filters)

        partition_field_ids = [f["field_id"] for f in self._partition_spec.get("fields", [])]

        # Group manifest table rows by parent intermediate namespace.
        # Two leaves whose object_ids differ only in the last namespace
        # segment are stitch-siblings.
        parent_to_indices: dict[str, list[int]] = defaultdict(list)
        for i in range(self._manifest_table.num_rows):
            if self._manifest_table.column(COL_OBJECT_TYPE)[i].as_py() != OBJECT_TYPE_TABLE:
                continue
            oid = self._manifest_table.column(COL_OBJECT_ID)[i].as_py()
            parts = oid.split("$")
            # Parts shape: ["v<N>", <ns_1>, ..., <ns_K>, "dataset"]. A leaf
            # has siblings only if there's at least one namespace above it
            # besides the spec-version (i.e., parts has 4+ segments).
            parent = "$".join(parts[:-2]) if len(parts) >= 4 else oid
            parent_to_indices[parent].append(i)

        # Namespace data columns (everything except partition cols + synthetic).
        namespace_data_cols = {
            n for n in self._namespace_pa_schema.names if n not in partition_field_ids
        } - _SYNTHETIC_LEAF_COLS

        for indices in parent_to_indices.values():
            stitch = len(indices) > 1 and self._siblings_are_column_partitioned(indices, namespace_data_cols)
            if not stitch:
                for i in indices:
                    for task in self._single_leaf_tasks(
                        i, partition_field_ids, required_columns, pushed_arrow_filter, pushdowns
                    ):
                        yield task
            else:
                stitched = self._stitched_task(
                    indices, partition_field_ids, required_columns, pushed_arrow_filter, pushdowns
                )
                if stitched is not None:
                    yield stitched

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
            f"Could not determine namespace schema for {self._namespace_uri}: no readable leaf "
            "tables and no `schema` metadata on the manifest."
        )

    def _siblings_are_column_partitioned(self, indices: list[int], namespace_data_cols: set[str]) -> bool:
        """Decide whether sibling leaves are column-partitioned (and so should stitch).

        Sniff the first leaf's data schema. If it's a *proper subset* of the
        namespace's data columns, the namespace's leaves carry column-wise
        slices of a wider logical row — stitch them on `_rowid`. If the
        first leaf has the full namespace data schema, all leaves probably
        do, and we treat them as ordinary row-partitioned siblings.
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

    def _partition_filter_matches(self, partition_filter: Expression, partition_values: dict[str, Any]) -> bool:
        """Evaluate a partition filter against a single row's partition values."""
        try:
            arrays: dict[str, pa.Array] = {}
            for fid, value in partition_values.items():
                dtype = self._partition_field_types.get(fid, pa.utf8())
                arrays[fid] = pa.array([value], type=dtype)
            rb = RecordBatch.from_pydict(arrays)
            result = rb.eval_expression_list(ExpressionsProjection([partition_filter]))
            col = result.to_arrow_table().column(0)
            return bool(col[0].as_py())
        except Exception as e:
            logger.warning("Partition filter eval failed (%s); including partition", e)
            return True

    def _single_leaf_tasks(
        self,
        i: int,
        partition_field_ids: list[str],
        required_columns: list[str] | None,
        pushed_arrow_filter: Optional["pa.compute.Expression"],
        pushdowns: "Pushdowns",
    ) -> Iterator[DataSourceTask]:
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

        yield from self._fragment_tasks(
            ds_uri=ds_uri,
            partition_values=partition_values,
            required_columns=required_columns,
            pushed_arrow_filter=pushed_arrow_filter,
            pushdowns=pushdowns,
            read_version=read_version,
        )

    def _fragment_tasks(
        self,
        *,
        ds_uri: str,
        partition_values: dict[str, Any],
        required_columns: list[str] | None,
        pushed_arrow_filter: Optional["pa.compute.Expression"],
        pushdowns: "Pushdowns",
        read_version: int | None,
    ) -> Iterator[DataSourceTask]:
        """Open a single leaf, enumerate fragments (with grouping), yield one task per group."""
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

        # Metadata-only check: if this leaf can't satisfy the projection,
        # don't emit scan tasks for it at all.
        leaf_names = set(ds.schema.names)
        _, skip = _compute_leaf_projection(leaf_names, required_columns, set(partition_values.keys()))
        if skip:
            return

        fragments = list(ds.get_fragments())

        def _make_task(frag_ids: list[int]) -> DataSourceTask:
            return _LanceLeafTask(
                ds_uri=ds_uri,
                fragment_ids=frag_ids,
                required_columns=required_columns,
                filter=pushed_arrow_filter,
                limit=pushdowns.limit,
                partition_values=partition_values,
                partition_field_types=self._partition_field_types,
                read_version=read_version,
                storage_options=self._storage_options,
                include_fragment_id=self._include_fragment_id,
                default_scan_options=self._default_scan_options,
                schema=self._schema,
            )

        group_size = self._fragment_group_size
        if group_size is not None and group_size > 1:
            current: list[int] = []
            for fragment in fragments:
                rows = fragment.count_rows(pushed_arrow_filter)
                if rows == 0:
                    continue
                current.append(fragment.fragment_id)
                if len(current) >= group_size:
                    yield _make_task(current)
                    current = []
            if current:
                yield _make_task(current)
        else:
            for fragment in fragments:
                rows = fragment.count_rows(pushed_arrow_filter)
                if rows == 0:
                    continue
                yield _make_task([fragment.fragment_id])

    def _stitched_task(
        self,
        indices: list[int],
        partition_field_ids: list[str],
        required_columns: list[str] | None,
        pushed_arrow_filter: Optional["pa.compute.Expression"],
        pushdowns: "Pushdowns",
    ) -> DataSourceTask | None:
        """Build a single task that stitches all sibling leaves in this group."""
        per_index_pvs: list[dict[str, Any]] = []
        for i in indices:
            pv: dict[str, Any] = {}
            for fid in partition_field_ids:
                col = f"{PARTITION_FIELD_PREFIX}{fid}"
                if col in self._manifest_table.column_names:
                    pv[fid] = self._manifest_table.column(col)[i].as_py()
            per_index_pvs.append(pv)

        # Partition values shared by all stitch siblings (typically the
        # parent-level cols); the leaf-distinguishing one becomes NULL on
        # stitched rows since they aggregate across multiple values.
        shared_pvs: dict[str, Any] = {}
        if per_index_pvs:
            for fid in partition_field_ids:
                first = per_index_pvs[0].get(fid)
                if all(p.get(fid) == first for p in per_index_pvs):
                    shared_pvs[fid] = first

        if pushdowns.partition_filters is not None and not self._partition_filter_matches(
            pushdowns.partition_filters, shared_pvs
        ):
            return None

        leaf_uris: list[str] = []
        leaf_read_versions: list[int | None] = []
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
            return None

        return _LanceStitchedTask(
            leaf_uris=leaf_uris,
            leaf_read_versions=leaf_read_versions,
            required_columns=required_columns,
            filter=pushed_arrow_filter,
            limit=pushdowns.limit,
            partition_values=shared_pvs,
            partition_field_types=self._partition_field_types,
            storage_options=self._storage_options,
            default_scan_options=self._default_scan_options,
            schema=self._schema,
        )
