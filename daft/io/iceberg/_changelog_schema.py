"""Schema safety gate for Iceberg COW changelog (CDC) reads.

Two layers:

- `validate_single_schema_table`: a cheap, table-metadata-level *fast rejection*. It is
  NOT a safety proof -- `ExpireSnapshots.cleanExpiredMetadata(true)` can prune
  `table.metadata.schemas` down to a single entry even though some still-referenced data
  file was physically written under a schema that is no longer listed there.
- `validate_task_file_schemas`: the real safety gate. It reads every distinct data file's
  Parquet footer (via Daft's own I/O client, so it shares credentials/endpoint/protocol
  handling with the real ScanTask read) and recursively compares its Iceberg field IDs,
  names, requiredness, and types against the baseline schema at every nesting level
  (top-level, struct child, list element, map key/value).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.io.pyarrow import pyarrow_to_schema
from pyiceberg.types import ListType, MapType, NestedField, StructType

from daft.recordbatch.recordbatch import read_parquet_arrow_schema

if TYPE_CHECKING:
    from collections.abc import Iterable

    import pyarrow as pa
    from pyiceberg.schema import Schema
    from pyiceberg.table import Table
    from pyiceberg.types import IcebergType

    from daft.daft import StorageConfig
    from daft.io.iceberg._changelog_planning import ChangelogFileTask


def validate_single_schema_table(table: Table) -> Schema:
    """Cheap, table-metadata-level fast rejection.

    Not a safety proof, just an early filter to avoid unnecessary footer I/O for obviously
    unsupported tables.

    `len(table.metadata.schemas) == 1` does not prove the table never underwent schema
    evolution: `ExpireSnapshots.cleanExpiredMetadata(true)` computes the reachable-schema
    set from *retained snapshots' own declared schema_id*, not from the physical write
    schema of the data files those snapshots reference, so an old schema can be pruned
    from `metadata.schemas` even while a data file written under it is still live and
    referenced. The real safety proof lives in `validate_task_file_schemas`.
    """
    if len(table.metadata.schemas) != 1:
        raise NotImplementedError(
            "daft.read_iceberg_changes() only supports tables whose metadata currently "
            f"contains exactly one schema (table.metadata.schemas has "
            f"{len(table.metadata.schemas)}); tables with any schema evolution history are "
            "not yet supported."
        )
    return table.metadata.schemas[0]


def _read_footer_arrow_schema(path: str, storage_config: StorageConfig) -> pa.Schema:
    """Read a Parquet file's footer schema via Daft's own I/O client.

    Uses the same `storage_config` the real ScanTask read will use, so credential/endpoint/
    protocol handling can never diverge between the gate and the real read.
    """
    return read_parquet_arrow_schema(
        path,
        io_config=storage_config.io_config,
        multithreaded_io=storage_config.multithreaded_io,
    )


def validate_task_file_schemas(
    baseline_schema: Schema,
    format_version: int,
    storage_config: StorageConfig,
    tasks: Iterable[ChangelogFileTask],
) -> None:
    """For every distinct data file referenced by `tasks`, recursively verify schema compatibility.

    Verifies Iceberg field IDs, names, requiredness, and types are strictly compatible with
    `baseline_schema` at every nesting level. This is the real schema safety proof;
    `validate_single_schema_table` is only the fast-rejection front end.

    Strict-equality contract for the first release: field-id set, per-field name, required,
    and type must match exactly at every level (top-level, struct child, list element, map
    key/value) -- no null/default fill for missing columns, no renaming, no type promotion.
    De-duplicates by file path so the same physical file referenced by multiple tasks only
    triggers one footer read.
    """
    _assert_globally_unique_field_ids(list(baseline_schema.fields), path="<baseline schema>", side="baseline schema")
    seen_paths: set[str] = set()

    for task in tasks:
        path = task.data_file.file_path
        if path in seen_paths:
            continue
        seen_paths.add(path)

        # FileNotFoundError/PermissionError propagate as-is (I/O errors, not schema
        # incompatibility) -- only the pyarrow_to_schema conversion below is wrapped.
        arrow_schema = _read_footer_arrow_schema(path, storage_config)

        try:
            # format_version must be the table's own format version, not
            # pyarrow_to_schema's default (v2): format v3 tables allow nanosecond
            # timestamps / UnknownType that convert differently under v2 rules, which
            # would misclassify legitimate v3 files as unsupported.
            file_schema = pyarrow_to_schema(arrow_schema, format_version=format_version)
        except (ValueError, TypeError) as e:
            raise NotImplementedError(
                f"data file {path} does not carry Iceberg field IDs (at every nesting "
                "level) in its Parquet footer, or uses an Arrow type this conversion "
                f"doesn't support under format_version={format_version}; "
                "daft.read_iceberg_changes() cannot safely verify its schema compatibility."
            ) from e

        _assert_globally_unique_field_ids(list(file_schema.fields), path=path, side="data file")
        _assert_fields_compatible(list(baseline_schema.fields), list(file_schema.fields), path, "$")


def _assert_globally_unique_field_ids(
    fields: list[NestedField],
    path: str,
    side: str,
    _seen: dict[int, str] | None = None,
    _logical_path: str = "$",
) -> None:
    """Recursively collect every field id in the schema tree along with its logical path.

    Rejects as soon as the same field id shows up at two different logical paths --
    regardless of whether they're sibling fields at the same level.

    `_index_by_field_id` (used inside `_assert_fields_compatible`) only checks for
    duplicates among sibling fields at a single level; a field id reused across nesting
    levels (e.g. once at the top level, again inside some struct's child) wouldn't trip
    that check, and the recursive comparison would just report a "type mismatch" at
    whichever position it happens to notice first rather than pointing at the real cause.
    This is a diagnostic-quality improvement, not a correctness requirement -- without it,
    the recursive comparison usually still fails somewhere, just with a less direct error.
    """
    seen = _seen if _seen is not None else {}
    for f in fields:
        field_path = f"{_logical_path}.{f.name}"
        if f.field_id in seen:
            raise NotImplementedError(
                f"{side} for {path} reuses Iceberg field id={f.field_id} at both "
                f"{seen[f.field_id]!r} and {field_path!r}; field ids must be globally "
                "unique within a schema."
            )
        seen[f.field_id] = field_path
        _walk_type_for_field_ids(f.field_type, path, side, seen, field_path)


def _walk_type_for_field_ids(
    field_type: IcebergType,
    path: str,
    side: str,
    seen: dict[int, str],
    logical_path: str,
) -> None:
    """Container-type descent for `_assert_globally_unique_field_ids`.

    Uses the same "synthesize a single-field list" trick as `_assert_types_compatible` for
    list/map so both functions walk the type tree identically.
    """
    if isinstance(field_type, StructType):
        _assert_globally_unique_field_ids(list(field_type.fields), path, side, seen, logical_path)
    elif isinstance(field_type, ListType):
        _assert_globally_unique_field_ids(
            [
                NestedField(
                    field_type.element_id, "element", field_type.element_type, required=field_type.element_required
                )
            ],
            path,
            side,
            seen,
            logical_path,
        )
    elif isinstance(field_type, MapType):
        _assert_globally_unique_field_ids(
            [
                NestedField(field_type.key_id, "key", field_type.key_type, required=True),
                NestedField(field_type.value_id, "value", field_type.value_type, required=field_type.value_required),
            ],
            path,
            side,
            seen,
            logical_path,
        )
    # PrimitiveType: no children, nothing further to walk.


def _assert_fields_compatible(
    baseline_fields: list[NestedField],
    file_fields: list[NestedField],
    path: str,
    logical_path: str,
) -> None:
    """Compare one level of sibling fields by field id.

    "One level" means the schema top level, a struct's children, or the single-element list
    wrapping a list element / map key / map value. field id, name, required, and type must
    all match. doc/default are not compared -- they aren't part of the physical schema.
    `logical_path` is used purely for error messages, e.g. "$.orders.items.element.product_id".
    """
    baseline_by_id = _index_by_field_id(baseline_fields, path, logical_path, side="baseline schema")
    file_by_id = _index_by_field_id(file_fields, path, logical_path, side="data file")

    for field_id, bf in baseline_by_id.items():
        field_path = f"{logical_path}.{bf.name}"
        ff = file_by_id.get(field_id)
        if ff is None:
            raise NotImplementedError(
                f"data file {path} is missing Iceberg field id={field_id} ({field_path}); "
                "daft.read_iceberg_changes() requires every changelog data file to "
                "physically contain every baseline schema field in its first release."
            )
        if ff.name != bf.name:
            raise NotImplementedError(
                f"data file {path} field id={field_id} at {field_path} has physical name "
                f"{ff.name!r}, expected {bf.name!r}; field renaming is not yet supported "
                "(a deliberately conservative first-release restriction, not a fundamental "
                "limitation)."
            )
        if ff.required != bf.required:
            raise NotImplementedError(
                f"data file {path} field id={field_id} at {field_path} has "
                f"required={ff.required}, expected required={bf.required}."
            )
        _assert_types_compatible(bf.field_type, ff.field_type, path, field_path)

    extra_ids = set(file_by_id) - set(baseline_by_id)
    if extra_ids:
        raise NotImplementedError(
            f"data file {path} at {logical_path} contains Iceberg field id(s) "
            f"{sorted(extra_ids)} that are not present in the baseline schema; "
            "daft.read_iceberg_changes() requires exact schema equality in its first release."
        )


def _index_by_field_id(
    fields: list[NestedField],
    path: str,
    logical_path: str,
    side: str,
) -> dict[int, NestedField]:
    result: dict[int, NestedField] = {}
    for f in fields:
        if f.field_id in result:
            raise NotImplementedError(
                f"data file {path}: duplicate Iceberg field id={f.field_id} in the {side} "
                f"at {logical_path}; the schema is not well-formed."
            )
        result[f.field_id] = f
    return result


def _assert_types_compatible(
    baseline_type: IcebergType,
    file_type: IcebergType,
    path: str,
    logical_path: str,
) -> None:
    if isinstance(baseline_type, StructType) and isinstance(file_type, StructType):
        _assert_fields_compatible(list(baseline_type.fields), list(file_type.fields), path, logical_path)
        return
    if isinstance(baseline_type, ListType) and isinstance(file_type, ListType):
        # Pass logical_path itself, not f"{logical_path}.element": the synthetic field's
        # name is already "element", and _assert_fields_compatible appends
        # f"{logical_path}.{bf.name}" = f"{logical_path}.element" on its own; appending it
        # again here would produce "...element.element" (the map branch below is
        # unaffected, since its synthetic field names are "key"/"value", not a repeat of
        # logical_path's last segment).
        _assert_fields_compatible(
            [
                NestedField(
                    baseline_type.element_id,
                    "element",
                    baseline_type.element_type,
                    required=baseline_type.element_required,
                )
            ],
            [NestedField(file_type.element_id, "element", file_type.element_type, required=file_type.element_required)],
            path,
            logical_path,
        )
        return
    if isinstance(baseline_type, MapType) and isinstance(file_type, MapType):
        _assert_fields_compatible(
            [
                NestedField(baseline_type.key_id, "key", baseline_type.key_type, required=True),
                NestedField(
                    baseline_type.value_id, "value", baseline_type.value_type, required=baseline_type.value_required
                ),
            ],
            [
                NestedField(file_type.key_id, "key", file_type.key_type, required=True),
                NestedField(file_type.value_id, "value", file_type.value_type, required=file_type.value_required),
            ],
            path,
            logical_path,
        )
        return
    if isinstance(baseline_type, (StructType, ListType, MapType)) or isinstance(
        file_type, (StructType, ListType, MapType)
    ):
        raise NotImplementedError(
            f"data file {path} at {logical_path}: container type mismatch (baseline={baseline_type}, file={file_type})."
        )
    if baseline_type != file_type:
        # Both sides are PrimitiveType at this point; IntegerType() == IntegerType() and
        # friends are pyiceberg's own value-equality semantics.
        raise NotImplementedError(
            f"data file {path} at {logical_path}: type {file_type} differs from baseline "
            f"schema type {baseline_type}; type promotion is not yet supported by "
            "daft.read_iceberg_changes()."
        )
