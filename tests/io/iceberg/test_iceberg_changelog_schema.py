from __future__ import annotations

from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, ListType, LongType, MapType, NestedField, StringType, StructType

from daft.daft import IOConfig, StorageConfig
from daft.io.iceberg._changelog_schema import (
    _assert_globally_unique_field_ids,
    _read_footer_arrow_schema,
    validate_single_schema_table,
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


def test_struct_child_field_id_mismatch_rejected(tmp_path):
    mismatched = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(
            2,
            "nested",
            StructType(
                NestedField(999, "a", IntegerType(), required=True),  # field id 10 -> 999
                NestedField(11, "b", StringType(), required=False),
            ),
            required=False,
        ),
        NestedField(3, "items", ListType(21, IntegerType(), element_required=True), required=False),
        NestedField(4, "kv", MapType(31, StringType(), 32, IntegerType(), value_required=False), required=False),
    )
    pa_schema = schema_to_pyarrow(mismatched)
    path = _write_parquet(tmp_path, pa_schema)
    with pytest.raises(NotImplementedError, match="nested.a"):
        validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_list_element_field_id_mismatch_rejected(tmp_path):
    mismatched = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(
            2,
            "nested",
            StructType(
                NestedField(10, "a", IntegerType(), required=True), NestedField(11, "b", StringType(), required=False)
            ),
            required=False,
        ),
        NestedField(3, "items", ListType(999, IntegerType(), element_required=True), required=False),  # 21 -> 999
        NestedField(4, "kv", MapType(31, StringType(), 32, IntegerType(), value_required=False), required=False),
    )
    pa_schema = schema_to_pyarrow(mismatched)
    path = _write_parquet(tmp_path, pa_schema)
    with pytest.raises(NotImplementedError, match="items.element"):
        validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_map_key_field_id_mismatch_rejected(tmp_path):
    mismatched = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(
            2,
            "nested",
            StructType(
                NestedField(10, "a", IntegerType(), required=True), NestedField(11, "b", StringType(), required=False)
            ),
            required=False,
        ),
        NestedField(3, "items", ListType(21, IntegerType(), element_required=True), required=False),
        NestedField(
            4, "kv", MapType(999, StringType(), 32, IntegerType(), value_required=False), required=False
        ),  # 31 -> 999
    )
    pa_schema = schema_to_pyarrow(mismatched)
    path = _write_parquet(tmp_path, pa_schema)
    with pytest.raises(NotImplementedError, match="kv.key"):
        validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_missing_baseline_field_rejected(tmp_path):
    missing = Schema(
        NestedField(1, "id", LongType(), required=True),
        # "nested" (field id 2) is missing entirely.
        NestedField(3, "items", ListType(21, IntegerType(), element_required=True), required=False),
        NestedField(4, "kv", MapType(31, StringType(), 32, IntegerType(), value_required=False), required=False),
    )
    pa_schema = schema_to_pyarrow(missing)
    path = _write_parquet(tmp_path, pa_schema)
    with pytest.raises(NotImplementedError, match="missing Iceberg field id=2"):
        validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_extra_file_field_rejected(tmp_path):
    extra = Schema(
        *BASELINE_SCHEMA.fields,
        NestedField(5, "extra", StringType(), required=False),
    )
    pa_schema = schema_to_pyarrow(extra)
    data = _baseline_data()
    data["extra"] = pa.array(["z"], type=pa.large_string())
    path = _write_parquet(tmp_path, pa_schema, data)
    with pytest.raises(NotImplementedError, match=r"field id\(s\) \[5\]"):
        validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_type_mismatch_rejected(tmp_path):
    mismatched = Schema(
        NestedField(1, "id", StringType(), required=True),  # long -> string
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
    with pytest.raises(NotImplementedError, match="type promotion is not yet supported"):
        validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_required_mismatch_rejected(tmp_path):
    mismatched = Schema(
        NestedField(1, "id", LongType(), required=False),  # required=True -> False
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
    with pytest.raises(NotImplementedError, match="required="):
        validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_field_rename_rejected(tmp_path):
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
    path = _write_parquet(tmp_path, pa_schema)
    with pytest.raises(NotImplementedError, match="renaming is not yet supported"):
        validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_missing_field_id_metadata_rejected(tmp_path):
    pa_schema = pa.schema([pa.field("id", pa.int64())])  # no PARQUET:field_id at all
    path = _write_parquet(tmp_path, pa_schema)
    with pytest.raises(NotImplementedError, match="does not carry Iceberg field IDs"):
        validate_task_file_schemas(BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_duplicate_field_id_in_file_rejected(tmp_path):
    pa_schema = pa.schema(
        [
            pa.field("a", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),
            pa.field("b", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),  # duplicate
        ]
    )
    path = _write_parquet(tmp_path, pa_schema)
    baseline = Schema(NestedField(1, "a", LongType(), required=False))
    # Caught by the global-uniqueness pass (_assert_globally_unique_field_ids) before
    # _index_by_field_id's narrower same-level check ever runs -- it reports both logical
    # paths involved, which is strictly more informative.
    with pytest.raises(NotImplementedError, match="reuses Iceberg field id=1"):
        validate_task_file_schemas(baseline, FORMAT_VERSION, _storage_config(), [_task(path)])


def test_missing_file_propagates_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        validate_task_file_schemas(
            BASELINE_SCHEMA, FORMAT_VERSION, _storage_config(), [_task(str(tmp_path / "does_not_exist.parquet"))]
        )


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


@pytest.fixture(scope="function")
def local_catalog(tmpdir):
    catalog = SqlCatalog(
        "default",
        uri=f"sqlite:///{tmpdir}/pyiceberg_catalog.db",
        warehouse=f"file://{tmpdir}",
    )
    catalog.create_namespace("default")
    yield catalog
    catalog.engine.dispose()


def test_validate_single_schema_table_passes_with_one_schema(local_catalog):
    schema = Schema(NestedField(1, "id", LongType(), required=False))
    table = local_catalog.create_table("default.single_schema", schema=schema)
    result = validate_single_schema_table(table)
    assert result.schema_id == table.schema().schema_id


def test_validate_single_schema_table_rejects_multiple_schemas(local_catalog):
    schema = Schema(NestedField(1, "id", LongType(), required=False))
    table = local_catalog.create_table("default.multi_schema", schema=schema)
    with table.update_schema() as update:
        update.add_column("extra", LongType())
    table.refresh()
    with pytest.raises(NotImplementedError, match="exactly one schema"):
        validate_single_schema_table(table)
