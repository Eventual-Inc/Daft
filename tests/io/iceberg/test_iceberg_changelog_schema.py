from __future__ import annotations

import inspect
import re
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.io.pyarrow import _check_pyarrow_schema_compatible, pyarrow_to_schema, schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, ListType, LongType, MapType, NestedField, StringType, StructType

from daft.daft import IOConfig, StorageConfig
from daft.io.iceberg._changelog_schema import (
    _assert_globally_unique_field_ids,
    _read_footer_arrow_schema,
    require_pyiceberg_version_for_changelog,
    validate_task_file_schemas,
)

FORMAT_VERSION = 2

BASELINE_SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(
        2,
        "nested",
        StructType(
            NestedField(10, "a", IntegerType(), required=True),
            NestedField(11, "b", StringType(), required=False),
        ),
        required=False,
    ),
    NestedField(3, "items", ListType(21, IntegerType(), element_required=True), required=False),
    NestedField(4, "kv", MapType(31, StringType(), 32, IntegerType(), value_required=False), required=False),
)


def _storage_config() -> StorageConfig:
    return StorageConfig(True, IOConfig())


def _task(path: str) -> SimpleNamespace:
    return SimpleNamespace(data_file=SimpleNamespace(file_path=path))


def _write_parquet(tmp_path, pa_schema: pa.Schema, arrays: dict[str, pa.Array] | None = None) -> str:
    if arrays is None:
        arrays = {f.name: pa.array([], type=f.type) for f in pa_schema}
    table = pa.table(arrays, schema=pa_schema)
    path = str(tmp_path / "data.parquet")
    pq.write_table(table, path)
    return path


def _baseline_data() -> dict[str, pa.Array]:
    return {
        "id": pa.array([1], type=pa.int64()),
        "nested": pa.array([{"a": 1, "b": "x"}], type=pa.struct([("a", pa.int32()), ("b", pa.large_string())])),
        "items": pa.array([[1, 2]], type=pa.large_list(pa.int32())),
        "kv": pa.array(
            [[("k", 1)]],
            type=pa.map_(pa.field("key", pa.large_string(), nullable=False), pa.field("value", pa.int32())),
        ),
    }


def test_matching_schema_passes(tmp_path):
    pa_schema = schema_to_pyarrow(BASELINE_SCHEMA)
    path = _write_parquet(tmp_path, pa_schema, _baseline_data())
    validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_field_rename_allowed(tmp_path):
    renamed = Schema(
        NestedField(1, "identifier", LongType(), required=True),  # "id" -> "identifier", same field id
        NestedField(
            2,
            "nested",
            StructType(
                NestedField(10, "a", IntegerType(), required=True), NestedField(11, "b", StringType(), required=False)
            ),
            required=False,
        ),
        NestedField(3, "items", ListType(21, IntegerType(), element_required=True), required=False),
        NestedField(4, "kv", MapType(31, StringType(), 32, IntegerType(), value_required=False), required=False),
    )
    pa_schema = schema_to_pyarrow(renamed)
    data = _baseline_data()
    data["identifier"] = data.pop("id")
    path = _write_parquet(tmp_path, pa_schema, data)
    validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_type_promotion_allowed(tmp_path):
    promoted = Schema(
        NestedField(1, "id", IntegerType(), required=True),  # baseline is long; file is int (promotable)
        NestedField(
            2,
            "nested",
            StructType(
                NestedField(10, "a", IntegerType(), required=True), NestedField(11, "b", StringType(), required=False)
            ),
            required=False,
        ),
        NestedField(3, "items", ListType(21, IntegerType(), element_required=True), required=False),
        NestedField(4, "kv", MapType(31, StringType(), 32, IntegerType(), value_required=False), required=False),
    )
    pa_schema = schema_to_pyarrow(promoted)
    path = _write_parquet(tmp_path, pa_schema)
    validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_type_mismatch_rejected(tmp_path):
    mismatched = Schema(
        NestedField(1, "id", StringType(), required=True),  # long -> string, not promotable
        NestedField(
            2,
            "nested",
            StructType(
                NestedField(10, "a", IntegerType(), required=True), NestedField(11, "b", StringType(), required=False)
            ),
            required=False,
        ),
        NestedField(3, "items", ListType(21, IntegerType(), element_required=True), required=False),
        NestedField(4, "kv", MapType(31, StringType(), 32, IntegerType(), value_required=False), required=False),
    )
    pa_schema = schema_to_pyarrow(mismatched)
    path = _write_parquet(tmp_path, pa_schema)
    with pytest.raises(NotImplementedError, match="not compatible with baseline schema"):
        validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_required_mismatch_file_optional_rejected(tmp_path):
    # baseline requires "id"; file makes it optional -- unsafe direction, rejected.
    mismatched = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(
            2,
            "nested",
            StructType(
                NestedField(10, "a", IntegerType(), required=True), NestedField(11, "b", StringType(), required=False)
            ),
            required=False,
        ),
        NestedField(3, "items", ListType(21, IntegerType(), element_required=True), required=False),
        NestedField(4, "kv", MapType(31, StringType(), 32, IntegerType(), value_required=False), required=False),
    )
    pa_schema = schema_to_pyarrow(mismatched)
    path = _write_parquet(tmp_path, pa_schema)
    with pytest.raises(NotImplementedError, match="not compatible with baseline schema"):
        validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_required_mismatch_file_required_allowed(tmp_path):
    # baseline field "b" (id=11) is optional; file makes it required -- safe direction, allowed.
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(
            2,
            "nested",
            StructType(
                NestedField(10, "a", IntegerType(), required=True), NestedField(11, "b", StringType(), required=True)
            ),
            required=False,
        ),
        NestedField(3, "items", ListType(21, IntegerType(), element_required=True), required=False),
        NestedField(4, "kv", MapType(31, StringType(), 32, IntegerType(), value_required=False), required=False),
    )
    pa_schema = schema_to_pyarrow(schema)
    path = _write_parquet(tmp_path, pa_schema, _baseline_data())
    validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_extra_file_field_allowed(tmp_path):
    extra = Schema(
        *BASELINE_SCHEMA.fields,
        NestedField(5, "extra", StringType(), required=False),
    )
    pa_schema = schema_to_pyarrow(extra)
    data = _baseline_data()
    data["extra"] = pa.array(["z"], type=pa.large_string())
    path = _write_parquet(tmp_path, pa_schema, data)
    validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_missing_optional_field_allowed(tmp_path):
    # baseline field "nested" (id=2) is optional and has no initial_default -- missing
    # entirely from the file is allowed (null-filled).
    missing = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(3, "items", ListType(21, IntegerType(), element_required=True), required=False),
        NestedField(4, "kv", MapType(31, StringType(), 32, IntegerType(), value_required=False), required=False),
    )
    pa_schema = schema_to_pyarrow(missing)
    path = _write_parquet(tmp_path, pa_schema)
    validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_missing_required_field_rejected(tmp_path):
    missing_required = Schema(
        NestedField(
            2,
            "nested",
            StructType(
                NestedField(10, "a", IntegerType(), required=True), NestedField(11, "b", StringType(), required=False)
            ),
            required=False,
        ),
        NestedField(3, "items", ListType(21, IntegerType(), element_required=True), required=False),
        NestedField(4, "kv", MapType(31, StringType(), 32, IntegerType(), value_required=False), required=False),
    )
    pa_schema = schema_to_pyarrow(missing_required)
    path = _write_parquet(tmp_path, pa_schema)
    with pytest.raises(NotImplementedError, match="not compatible with baseline schema"):
        validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_missing_field_with_initial_default_rejected(tmp_path):
    baseline_with_default = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "name", StringType(), required=False, initial_default="unknown"),
    )
    file_schema = Schema(NestedField(1, "id", LongType(), required=True))
    pa_schema = schema_to_pyarrow(file_schema)
    path = _write_parquet(tmp_path, pa_schema)
    with pytest.raises(NotImplementedError, match="declares an initial_default"):
        validate_task_file_schemas(baseline_with_default, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_missing_field_without_initial_default_allowed(tmp_path):
    baseline_no_default = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "name", StringType(), required=False),
    )
    file_schema = Schema(NestedField(1, "id", LongType(), required=True))
    pa_schema = schema_to_pyarrow(file_schema)
    path = _write_parquet(tmp_path, pa_schema)
    validate_task_file_schemas(baseline_no_default, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_nested_rename_and_promotion_allowed(tmp_path):
    baseline_nested = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "nested", StructType(NestedField(10, "a", LongType(), required=True)), required=False),
    )
    file_schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(
            2, "nested", StructType(NestedField(10, "renamed_a", IntegerType(), required=True)), required=False
        ),
    )
    pa_schema = schema_to_pyarrow(file_schema)
    path = _write_parquet(tmp_path, pa_schema)
    validate_task_file_schemas(baseline_nested, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_missing_field_id_metadata_rejected(tmp_path):
    pa_schema = pa.schema([pa.field("id", pa.int64())])  # no PARQUET:field_id at all
    path = _write_parquet(tmp_path, pa_schema)
    with pytest.raises(NotImplementedError, match="does not carry Iceberg field IDs"):
        validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_struct_child_missing_field_id_metadata_rejected(tmp_path):
    # Top-level "nested" field carries a field id; only its struct child "a" is missing one.
    baseline = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "nested", StructType(NestedField(10, "a", LongType(), required=False)), required=False),
    )
    child_no_id = pa.field("a", pa.int64())
    nested_field = pa.field("nested", pa.struct([child_no_id]), metadata={b"PARQUET:field_id": b"2"})
    id_field = pa.field("id", pa.int64(), nullable=False, metadata={b"PARQUET:field_id": b"1"})
    pa_schema = pa.schema([id_field, nested_field])
    path = _write_parquet(tmp_path, pa_schema)
    with pytest.raises(NotImplementedError, match="does not carry Iceberg field IDs"):
        validate_task_file_schemas(baseline, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_list_element_missing_field_id_metadata_rejected(tmp_path):
    baseline = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "items", ListType(20, LongType(), element_required=False), required=False),
    )
    elem_no_id = pa.field("item", pa.int64(), nullable=True)  # no field-id metadata
    items_field = pa.field("items", pa.list_(elem_no_id), metadata={b"PARQUET:field_id": b"2"})
    id_field = pa.field("id", pa.int64(), nullable=False, metadata={b"PARQUET:field_id": b"1"})
    pa_schema = pa.schema([id_field, items_field])
    path = _write_parquet(tmp_path, pa_schema)
    with pytest.raises(NotImplementedError, match="does not carry Iceberg field IDs"):
        validate_task_file_schemas(baseline, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_map_value_missing_field_id_metadata_rejected(tmp_path):
    baseline = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "kv", MapType(20, StringType(), 21, LongType(), value_required=False), required=False),
    )
    key_field = pa.field("key", pa.string(), nullable=False, metadata={b"PARQUET:field_id": b"20"})
    value_field_no_id = pa.field("value", pa.int64(), nullable=True)  # no field-id metadata
    kv_field = pa.field("kv", pa.map_(key_field, value_field_no_id), metadata={b"PARQUET:field_id": b"2"})
    id_field = pa.field("id", pa.int64(), nullable=False, metadata={b"PARQUET:field_id": b"1"})
    pa_schema = pa.schema([id_field, kv_field])
    path = _write_parquet(tmp_path, pa_schema)
    with pytest.raises(NotImplementedError, match="does not carry Iceberg field IDs"):
        validate_task_file_schemas(baseline, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_duplicate_field_id_in_file_rejected(tmp_path):
    pa_schema = pa.schema(
        [
            pa.field("a", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),
            pa.field("b", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),  # duplicate
        ]
    )
    path = _write_parquet(tmp_path, pa_schema)
    baseline = Schema(NestedField(1, "a", LongType(), required=False))
    # Caught by Layer 1's global-uniqueness pass, which runs before Layer 2 (PyIceberg's own
    # compatibility checker) ever sees the file -- it reports both logical paths involved,
    # which is strictly more informative than PyIceberg's own error would be.
    with pytest.raises(NotImplementedError, match="reuses Iceberg field id=1"):
        validate_task_file_schemas(baseline, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_cross_level_duplicate_field_id_in_real_footer_rejected(tmp_path):
    # Top-level "id" (field id 1) and nested struct child "nested.a" both declare field id 1
    # in a real Parquet footer -- not just via the _assert_globally_unique_field_ids helper
    # called directly, but through the full validate_task_file_schemas call chain.
    baseline = Schema(NestedField(1, "id", LongType(), required=False))
    child_dup = pa.field("a", pa.int64(), metadata={b"PARQUET:field_id": b"1"})
    nested_field = pa.field("nested", pa.struct([child_dup]), metadata={b"PARQUET:field_id": b"2"})
    id_field = pa.field("id", pa.int64(), nullable=True, metadata={b"PARQUET:field_id": b"1"})
    pa_schema = pa.schema([id_field, nested_field])
    path = _write_parquet(tmp_path, pa_schema)
    with pytest.raises(NotImplementedError, match=r"reuses Iceberg field id=1 at both.*'\$\.nested\.a'"):
        validate_task_file_schemas(baseline, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_nested_struct_field_missing_with_initial_default_rejected(tmp_path):
    baseline = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(
            2,
            "nested",
            StructType(
                NestedField(10, "a", LongType(), required=False),
                NestedField(11, "b", StringType(), required=False, initial_default="unknown"),
            ),
            required=False,
        ),
    )
    # File's "nested" struct is missing child field id=11 ("b"), which the baseline declares
    # an initial_default for -- must be rejected (Layer 3), not silently null-filled.
    file_schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "nested", StructType(NestedField(10, "a", LongType(), required=False)), required=False),
    )
    pa_schema = schema_to_pyarrow(file_schema)
    path = _write_parquet(tmp_path, pa_schema)
    with pytest.raises(NotImplementedError, match=r"missing Iceberg field id=11 \(b\).*declares an initial_default"):
        validate_task_file_schemas(baseline, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_missing_file_wraps_file_not_found_with_context(tmp_path):
    missing_path = str(tmp_path / "does_not_exist.parquet")
    with pytest.raises(FileNotFoundError, match=rf"{re.escape(missing_path)}.*baseline schema id=0") as excinfo:
        validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(missing_path)])
    # Original OS-level cause must still be reachable, not swallowed by the wrapping.
    assert isinstance(excinfo.value.__cause__, FileNotFoundError)


def test_same_path_dedup_only_reads_footer_once(tmp_path, monkeypatch):
    pa_schema = schema_to_pyarrow(BASELINE_SCHEMA)
    path = _write_parquet(tmp_path, pa_schema, _baseline_data())

    call_count = 0
    real = _read_footer_arrow_schema

    def counting(path_arg, storage_config):
        nonlocal call_count
        call_count += 1
        return real(path_arg, storage_config)

    monkeypatch.setattr("daft.io.iceberg._changelog_schema._read_footer_arrow_schema", counting)

    tasks = [_task(path), _task(path), _task(path)]
    validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), tasks)
    assert call_count == 1


def test_globally_unique_field_ids_rejects_cross_level_reuse():
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(
            2, "nested", StructType(NestedField(1, "a", IntegerType(), required=True)), required=False
        ),  # reuses id=1
    )
    with pytest.raises(NotImplementedError, match="reuses Iceberg field id=1"):
        _assert_globally_unique_field_ids(list(schema.fields), path="<test>", side="test schema")


def test_baseline_schema_duplicate_field_ids_rejected_up_front():
    duplicate_baseline = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "nested", StructType(NestedField(1, "a", IntegerType(), required=True)), required=False),
    )
    # Rejected as soon as validate_task_file_schemas is called, before any per-file work --
    # even with an empty tasks list, the baseline-uniqueness check still runs up front.
    with pytest.raises(NotImplementedError, match="reuses Iceberg field id=1"):
        validate_task_file_schemas(duplicate_baseline, FORMAT_VERSION, _storage_config(), [])


def test_require_pyiceberg_version_for_changelog_rejects_old_version(monkeypatch):
    monkeypatch.setattr(pyiceberg, "__version__", "0.9.0")
    with pytest.raises(NotImplementedError, match=r"requires pyiceberg>=0\.11\.0"):
        require_pyiceberg_version_for_changelog()


def test_require_pyiceberg_version_for_changelog_passes_current_version():
    # The actually-installed pyiceberg version must satisfy the floor this module depends on.
    require_pyiceberg_version_for_changelog()


def test_pyiceberg_private_api_sentinel_signatures():
    """Guard against silent signature drift in the two private PyIceberg APIs this module depends on.

    Only binds the two top-level symbols Daft actually calls directly
    (`_check_pyarrow_schema_compatible`, `pyarrow_to_schema`) -- not their internal
    implementation details (e.g. `_check_schema_compatible`, `_SchemaCompatibilityVisitor`),
    which are free to change as long as these two entry points keep the same contract.

    Signatures alone can't catch a behavior-only drift (same parameters, different
    compatibility verdict) -- see the sibling `test_pyiceberg_private_api_sentinel_*` behavior
    tests below, which call `_check_pyarrow_schema_compatible` directly (not through Daft's own
    `validate_task_file_schemas`) and pin concrete accept/reject outcomes for exactly this
    reason.

    NON-BLOCKING FOLLOW-UP:
    both the signature and behavior sentinels here only run against whichever PyIceberg
    version happens to be installed in a given environment -- CI (`pr-test-suite.yml`) has no
    matrix job that installs `pyiceberg==0.11.0` (this module's declared floor) separately from
    `pyiceberg==0.11.1` (the version pinned for the rest of the suite), so a behavior drift
    specific to 0.11.0 would not be caught. This does not block merging the feature: these
    tests pin behavior for the project's installed CI version, but must not be described as
    continuously proving behavior stability across the full allowed version range.
    """
    compat_params = inspect.signature(_check_pyarrow_schema_compatible).parameters
    assert "format_version" in compat_params
    assert list(compat_params)[:2] == ["requested_schema", "provided_schema"]

    convert_params = inspect.signature(pyarrow_to_schema).parameters
    assert "format_version" in convert_params
    assert "name_mapping" in convert_params
    assert list(convert_params)[:1] == ["schema"]


def test_pyiceberg_private_api_sentinel_nested_rename_allowed():
    baseline = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "nested", StructType(NestedField(10, "a", LongType(), required=False)), required=False),
    )
    renamed = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "nested", StructType(NestedField(10, "renamed_a", LongType(), required=False)), required=False),
    )
    _check_pyarrow_schema_compatible(baseline, schema_to_pyarrow(renamed), format_version=FORMAT_VERSION)


def test_pyiceberg_private_api_sentinel_missing_optional_field_allowed():
    baseline = Schema(
        NestedField(1, "id", LongType(), required=True), NestedField(2, "opt", StringType(), required=False)
    )
    file_schema = Schema(NestedField(1, "id", LongType(), required=True))
    _check_pyarrow_schema_compatible(baseline, schema_to_pyarrow(file_schema), format_version=FORMAT_VERSION)


def test_pyiceberg_private_api_sentinel_file_optional_baseline_required_rejected():
    baseline = Schema(NestedField(1, "id", LongType(), required=True))
    file_schema = Schema(NestedField(1, "id", LongType(), required=False))
    with pytest.raises(ValueError, match="Mismatch in fields"):
        _check_pyarrow_schema_compatible(baseline, schema_to_pyarrow(file_schema), format_version=FORMAT_VERSION)


def test_pyiceberg_private_api_sentinel_file_required_baseline_optional_allowed():
    baseline = Schema(NestedField(1, "id", LongType(), required=False))
    file_schema = Schema(NestedField(1, "id", LongType(), required=True))
    _check_pyarrow_schema_compatible(baseline, schema_to_pyarrow(file_schema), format_version=FORMAT_VERSION)


def test_pyiceberg_private_api_sentinel_safe_type_promotion_allowed():
    baseline = Schema(NestedField(1, "val", LongType(), required=False))
    file_schema = Schema(NestedField(1, "val", IntegerType(), required=False))  # int -> long is a safe promotion
    _check_pyarrow_schema_compatible(baseline, schema_to_pyarrow(file_schema), format_version=FORMAT_VERSION)


def test_pyiceberg_private_api_sentinel_unsafe_type_change_rejected():
    baseline = Schema(NestedField(1, "val", LongType(), required=False))
    file_schema = Schema(NestedField(1, "val", StringType(), required=False))  # string -> long is not a promotion
    with pytest.raises(ValueError, match="Mismatch in fields"):
        _check_pyarrow_schema_compatible(baseline, schema_to_pyarrow(file_schema), format_version=FORMAT_VERSION)
