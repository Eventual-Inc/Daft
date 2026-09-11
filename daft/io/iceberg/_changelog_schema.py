"""Schema safety gate for Iceberg COW changelog (CDC) reads.

Two layers, run for every distinct data file referenced by a changelog scan:

- Daft's own field-id completeness/global-uniqueness gate. Converts the file's raw footer
  Arrow schema to an Iceberg `Schema` *without* a name-mapping fallback, so a footer with no
  Iceberg field-id metadata at all fails closed here rather than being silently accepted by
  name -- this is deliberately not delegated to PyIceberg, whose own compatibility checker
  (below) accepts exactly that case via its internal name-mapping fallback (verified: a file
  with matching column names but zero `PARQUET:field_id` metadata, or with a duplicate field
  id reused across two physical columns, is accepted by `_check_pyarrow_schema_compatible`
  alone).
- PyIceberg's own `_check_pyarrow_schema_compatible`, which decides whether the file (now
  known to carry complete, globally-unique field ids) is compatible with the baseline
  schema: renamed fields (matched by id), missing optional fields (null-filled), missing
  required fields (rejected), type promotion (via `pyiceberg.schema.promote`), and
  requiredness in both directions (file-required-vs-baseline-optional allowed,
  file-optional-vs-baseline-required rejected). This is a private PyIceberg API; see
  `require_pyiceberg_version_for_changelog` for the version floor and the sentinel tests
  for the behavior checks that guard PyIceberg upgrades.

A third, Daft-specific rule sits on top: a baseline field with a non-null
`initial_default` that's missing from a file is rejected rather than silently null-filled
-- PyIceberg's own table scan applies `initial_default` for such rows, and Daft does not
implement default-value materialization yet, so silently returning `None` there would be a
data-value correctness bug, not just a conservative restriction.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.io.pyarrow import _check_pyarrow_schema_compatible, pyarrow_to_schema
from pyiceberg.types import ListType, MapType, NestedField, StructType

from daft.exceptions import DaftCoreException
from daft.recordbatch.recordbatch import read_parquet_arrow_schema

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    import pyarrow as pa
    from pyiceberg.schema import Schema
    from pyiceberg.typedef import TableVersion
    from pyiceberg.types import IcebergType

    from daft.daft import StorageConfig
    from daft.io.iceberg._changelog_planning import ChangelogFileTask


def require_pyiceberg_version_for_changelog() -> None:
    """Reject early if the installed PyIceberg can't support the changelog schema gate.

    `_check_pyarrow_schema_compatible`/`pyarrow_to_schema` only gained their
    `format_version` keyword argument in PyIceberg 0.10.0; before that, calling either with
    `format_version=...` raises `TypeError`. This isn't specific to schema-evolution tables
    -- the single-schema gate (this module, pre-relax) already called `pyarrow_to_schema`
    unconditionally with `format_version=...`, so this floor applies to
    `read_iceberg_changes()` as a whole, regardless of how many schemas the table has had.
    `daft.read_iceberg()` (non-CDC) never calls into this module and is unaffected.

    Must be called at DataFrame-construction time, before any snapshot-range resolution or
    I/O, so an unsupported PyIceberg version fails immediately rather than partway through
    planning.
    """
    import pyiceberg
    from packaging.version import parse

    if parse(pyiceberg.__version__) < parse("0.11.0"):
        raise NotImplementedError(
            f"daft.io.iceberg.read_iceberg_changes() requires pyiceberg>=0.11.0 (found "
            f"{pyiceberg.__version__}); the installed version does not support the "
            "format_version parameter this function's schema validation depends on. "
            "daft.read_iceberg() (non-CDC) is unaffected and continues to support older "
            "pyiceberg versions."
        )


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
    format_version: TableVersion,
    storage_config: StorageConfig,
    tasks: Iterable[ChangelogFileTask],
) -> None:
    """For every distinct data file referenced by `tasks`, verify it can be safely read against `baseline_schema`.

    De-duplicates by file path so the same physical file referenced by multiple tasks only
    triggers one footer read. Every failure mode below is wrapped with the file path,
    baseline schema id, and original cause via `raise ... from e`.
    """
    _assert_globally_unique_field_ids(list(baseline_schema.fields), path="<baseline schema>", side="baseline schema")
    seen_paths: set[str] = set()

    for task in tasks:
        path = task.data_file.file_path
        if path in seen_paths:
            continue
        seen_paths.add(path)

        # OSError (missing file, permission denied) and DaftCoreException (corrupt footer,
        # credential/storage errors surfaced from Daft's own Rust I/O layer) are the two known,
        # expected-to-happen failure families here -- both get the same file path/baseline
        # schema id context every other failure mode in this function gets. Reraised as
        # the same exception type (not a generic wrapper) so any
        # existing `except FileNotFoundError`-style handling upstream keeps working; not a
        # bare `except Exception`, so an unexpected programming error still propagates
        # unwrapped instead of being misreported as an I/O failure.
        try:
            arrow_schema = _read_footer_arrow_schema(path, storage_config)
        except (OSError, DaftCoreException) as e:
            raise type(e)(
                f"could not read the Parquet footer for data file {path} in order to verify "
                f"its schema against baseline schema id={baseline_schema.schema_id}: {e}"
            ) from e

        # Layer 1 (Daft-owned): field-id completeness + global uniqueness. Deliberately
        # does *not* pass `name_mapping` -- without it, `pyarrow_to_schema` raises
        # ValueError outright if the footer has no Iceberg field ids at all, instead of
        # falling back to matching by column name. This is the fail-closed behavior the
        # design requires and that PyIceberg's own compatibility checker (layer 2, below)
        # does not provide on its own.
        try:
            file_schema = pyarrow_to_schema(arrow_schema, format_version=format_version)
        except (ValueError, TypeError) as e:
            raise NotImplementedError(
                f"data file {path} does not carry Iceberg field IDs (at every nesting "
                "level) in its Parquet footer, or uses an Arrow type this conversion "
                f"doesn't support under format_version={format_version}; "
                f"daft.io.iceberg.read_iceberg_changes() cannot safely verify its schema "
                f"compatibility against baseline schema id={baseline_schema.schema_id}."
            ) from e
        file_field_ids = _assert_globally_unique_field_ids(list(file_schema.fields), path=path, side="data file")

        # Layer 2 (delegated): rename/null-fill/type-promotion/requiredness compatibility,
        # decided by PyIceberg's own checker. Fed the *original* arrow_schema, not
        # file_schema above -- `_check_pyarrow_schema_compatible` does its own conversion
        # internally (with a name_mapping fallback that layer 1 deliberately avoided; by
        # this point field-id completeness/uniqueness is already proven, so that fallback
        # path is no longer a safety concern for this call).
        try:
            _check_pyarrow_schema_compatible(baseline_schema, arrow_schema, format_version=format_version)
        except ValueError as e:
            raise NotImplementedError(
                f"data file {path} is not compatible with baseline schema id={baseline_schema.schema_id}: {e}"
            ) from e

        # Layer 3 (Daft-specific, fail-closed): a baseline field with a declared
        # initial_default that's missing from this file would be silently null-filled by
        # layer 2's rule above -- PyIceberg's own scan applies the default instead, and
        # Daft does not implement default-value materialization, so this must be rejected
        # rather than returning a wrong value.
        _reject_missing_fields_with_initial_default(baseline_schema, file_field_ids, path)


def _reject_missing_fields_with_initial_default(
    baseline_schema: Schema,
    file_field_ids: dict[int, NestedField],
    path: str,
) -> None:
    for field in _walk_all_nested_fields(list(baseline_schema.fields)):
        if field.field_id not in file_field_ids and field.initial_default is not None:
            raise NotImplementedError(
                f"data file {path} is missing Iceberg field id={field.field_id} "
                f"({field.name}), for which baseline schema id={baseline_schema.schema_id} "
                "declares an initial_default. daft.io.iceberg.read_iceberg_changes() does not yet "
                "materialize Iceberg default values for historical files that predate a "
                "column's addition; this table's schema evolution history is not yet "
                "supported."
            )


def _walk_all_nested_fields(fields: list[NestedField]) -> Iterator[NestedField]:
    """Recursively yield every field in the schema tree, without checking for duplicates.

    Includes struct children, list elements, and map keys/values as synthetic single-field
    entries. Only safe to use once field-id uniqueness has already been established for
    this schema (`_assert_globally_unique_field_ids`) -- this function does not itself
    guard against reused field ids.
    """
    for f in fields:
        yield f
        yield from _walk_nested_type_fields(f.field_type)


def _walk_nested_type_fields(field_type: IcebergType) -> Iterator[NestedField]:
    if isinstance(field_type, StructType):
        yield from _walk_all_nested_fields(list(field_type.fields))
    elif isinstance(field_type, ListType):
        yield from _walk_all_nested_fields(
            [
                NestedField(
                    field_type.element_id, "element", field_type.element_type, required=field_type.element_required
                )
            ]
        )
    elif isinstance(field_type, MapType):
        yield from _walk_all_nested_fields(
            [
                NestedField(field_type.key_id, "key", field_type.key_type, required=True),
                NestedField(field_type.value_id, "value", field_type.value_type, required=field_type.value_required),
            ]
        )
    # PrimitiveType: no children, nothing further to walk.


def _assert_globally_unique_field_ids(
    fields: list[NestedField],
    path: str,
    side: str,
    _seen: dict[int, NestedField] | None = None,
    _logical_path: str = "$",
) -> dict[int, NestedField]:
    """Recursively collect every field id in the schema tree along with its field, keyed by id.

    Rejects as soon as the same field id shows up at two different logical paths --
    regardless of whether they're sibling fields at the same level. Returns the full
    field-id -> field mapping so callers (the initial-default check) can look up individual
    fields without a second tree walk.
    """
    seen = _seen if _seen is not None else {}
    for f in fields:
        field_path = f"{_logical_path}.{f.name}"
        if f.field_id in seen:
            raise NotImplementedError(
                f"{side} for {path} reuses Iceberg field id={f.field_id} at both "
                f"a prior position and {field_path!r}; field ids must be globally unique "
                "within a schema."
            )
        seen[f.field_id] = f
        _walk_type_for_field_ids(f.field_type, path, side, seen, field_path)
    return seen


def _walk_type_for_field_ids(
    field_type: IcebergType,
    path: str,
    side: str,
    seen: dict[int, NestedField],
    logical_path: str,
) -> None:
    """Container-type descent for `_assert_globally_unique_field_ids`."""
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
