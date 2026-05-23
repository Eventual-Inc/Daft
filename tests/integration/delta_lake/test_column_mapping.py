from __future__ import annotations

import json
import pathlib
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import daft
from daft.logical.schema import Schema
from tests.utils import assert_pyarrow_tables_equal

_DELTA_TYPE_BY_PA: dict[pa.DataType, str] = {
    pa.int32(): "integer",
    pa.int64(): "long",
    pa.string(): "string",
    pa.large_string(): "string",
    pa.float32(): "float",
    pa.float64(): "double",
    pa.bool_(): "boolean",
}


def _delta_type(pa_type: pa.DataType) -> str:
    try:
        return _DELTA_TYPE_BY_PA[pa_type]
    except KeyError:
        raise NotImplementedError(f"Unsupported pyarrow type for fixture: {pa_type}") from None


def _physical_schema(
    arrow_schema: pa.Schema,
    logical_to_physical: dict[str, str],
    field_ids: dict[str, int],
) -> pa.Schema:
    return pa.schema(
        [
            arrow_schema.field(name)
            .with_name(logical_to_physical[name])
            .with_metadata({b"PARQUET:field_id": str(field_ids[name]).encode()})
            for name in arrow_schema.names
        ]
    )


def _add_action(path: str, size: int, partition_values: dict[str, str]) -> dict:
    return {
        "add": {
            "path": path,
            "partitionValues": partition_values,
            "size": size,
            "modificationTime": 0,
            "dataChange": True,
        }
    }


def _write_cm_table(
    path: pathlib.Path,
    data: pa.Table,
    mode: str,
    logical_to_physical: dict[str, str],
    field_ids: dict[str, int],
    partition_columns: list[str] | None = None,
) -> None:
    """Hand-craft a Delta table with columnMapping enabled.

    Pre-assigns physical names and field ids for each logical column. Writing the
    _delta_log manually is required because delta-rs (1.5.x) cannot enable columnMapping
    through the Python writer; this lets us exercise Daft's read path regardless.
    """
    partition_columns = partition_columns or []
    path.mkdir(parents=True, exist_ok=True)
    log_dir = path / "_delta_log"
    log_dir.mkdir(exist_ok=True)

    # Schema JSON: logical names at the top, physical names + ids in field metadata.
    fields_json = [
        {
            "name": name,
            "type": _delta_type(data.schema.field(name).type),
            "nullable": data.schema.field(name).nullable,
            "metadata": {
                "delta.columnMapping.id": field_ids[name],
                "delta.columnMapping.physicalName": logical_to_physical[name],
            },
        }
        for name in data.schema.names
    ]
    schema_str = json.dumps({"type": "struct", "fields": fields_json})

    # Rewrite the parquet table with physical column names + PARQUET:field_id metadata.
    physical_table = pa.Table.from_arrays(
        data.columns,
        schema=_physical_schema(data.schema, logical_to_physical, field_ids),
    )

    # Delta-rs quirk: metaData.partitionColumns uses LOGICAL names, but add.partitionValues
    # keys use PHYSICAL names. Empirically this is what delta-rs 1.5 accepts.
    add_entries: list[dict] = []
    if partition_columns:
        phys_part_keys = [logical_to_physical[c] for c in partition_columns]
        groups: dict[tuple, list[int]] = {}
        for row_idx in range(physical_table.num_rows):
            key = tuple(physical_table.column(p)[row_idx].as_py() for p in phys_part_keys)
            groups.setdefault(key, []).append(row_idx)
        for key, indices in groups.items():
            sub = physical_table.take(pa.array(indices)).drop(phys_part_keys)
            rel_dir = "/".join(f"{p}={v}" for p, v in zip(phys_part_keys, key))
            file_rel = f"{rel_dir}/part-{uuid.uuid4().hex}.parquet"
            (path / rel_dir).mkdir(parents=True, exist_ok=True)
            pq.write_table(sub, path / file_rel)
            add_entries.append(
                _add_action(
                    file_rel,
                    (path / file_rel).stat().st_size,
                    {p: str(v) for p, v in zip(phys_part_keys, key)},
                )
            )
    else:
        file_rel = f"part-{uuid.uuid4().hex}.parquet"
        pq.write_table(physical_table, path / file_rel)
        add_entries.append(_add_action(file_rel, (path / file_rel).stat().st_size, {}))

    protocol: dict = {
        "protocol": {
            "minReaderVersion": 2 if mode == "name" else 3,
            "minWriterVersion": 5 if mode == "name" else 7,
        }
    }
    if mode == "id":
        protocol["protocol"]["readerFeatures"] = ["columnMapping"]
        protocol["protocol"]["writerFeatures"] = ["columnMapping"]

    metadata = {
        "metaData": {
            "id": str(uuid.uuid4()),
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema_str,
            "partitionColumns": partition_columns,
            "configuration": {
                "delta.columnMapping.mode": mode,
                "delta.columnMapping.maxColumnId": str(max(field_ids.values())),
            },
            "createdTime": 0,
        }
    }

    log_file = log_dir / "00000000000000000000.json"
    with log_file.open("w") as f:
        for action in [protocol, metadata, *add_entries]:
            f.write(json.dumps(action) + "\n")


@pytest.mark.parametrize("mode", ["name", "id"])
def test_deltalake_read_column_mapping_flat(tmp_path, mode):
    pytest.importorskip("deltalake")
    path = tmp_path / f"cm_{mode}_flat"

    data = pa.table({"a": pa.array([1, 2, 3], type=pa.int64()), "b": ["x", "y", "z"]})
    _write_cm_table(
        path,
        data,
        mode=mode,
        logical_to_physical={"a": "col-aaa", "b": "col-bbb"},
        field_ids={"a": 1, "b": 2},
    )

    df = daft.read_deltalake(str(path))
    from deltalake import DeltaTable

    expected_schema = Schema.from_pyarrow_schema(pa.schema(DeltaTable(path).schema().to_arrow()))
    assert df.schema() == expected_schema
    assert_pyarrow_tables_equal(df.to_arrow().sort_by("a"), data.sort_by("a"))


@pytest.mark.parametrize("mode", ["name", "id"])
def test_deltalake_read_column_mapping_select_projection(tmp_path, mode):
    pytest.importorskip("deltalake")
    path = tmp_path / f"cm_{mode}_select"

    data = pa.table(
        {
            "a": pa.array([1, 2, 3], type=pa.int64()),
            "b": ["x", "y", "z"],
            "c": pa.array([1.1, 2.2, 3.3], type=pa.float64()),
        }
    )
    _write_cm_table(
        path,
        data,
        mode=mode,
        logical_to_physical={"a": "col-aaa", "b": "col-bbb", "c": "col-ccc"},
        field_ids={"a": 1, "b": 2, "c": 3},
    )

    result = daft.read_deltalake(str(path)).select("a", "c").to_arrow().sort_by("a")
    expected = data.select(["a", "c"]).sort_by("a")
    assert_pyarrow_tables_equal(result, expected)


def _write_nested_struct_cm_table(path: pathlib.Path) -> pa.Table:
    """Hand-craft a column-mapped Delta table with a top-level struct column.

    Logical schema: `outer struct<a: long, b: string>`.
    Physical names: `outer`→`col-outer`, `a`→`col-a`, `b`→`col-b`. Field ids 1/2/3.
    Returns the logical pyarrow Table for comparison.
    """
    path.mkdir(parents=True, exist_ok=True)
    log_dir = path / "_delta_log"
    log_dir.mkdir(exist_ok=True)

    schema_str = json.dumps(
        {
            "type": "struct",
            "fields": [
                {
                    "name": "outer",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "a",
                                "type": "long",
                                "nullable": True,
                                "metadata": {
                                    "delta.columnMapping.id": 2,
                                    "delta.columnMapping.physicalName": "col-a",
                                },
                            },
                            {
                                "name": "b",
                                "type": "string",
                                "nullable": True,
                                "metadata": {
                                    "delta.columnMapping.id": 3,
                                    "delta.columnMapping.physicalName": "col-b",
                                },
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {
                        "delta.columnMapping.id": 1,
                        "delta.columnMapping.physicalName": "col-outer",
                    },
                }
            ],
        }
    )

    # Logical table for return; physical table is the same shape under physical names + field_ids.
    logical_table = pa.table(
        {
            "outer": pa.array(
                [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}],
                type=pa.struct([("a", pa.int64()), ("b", pa.string())]),
            )
        }
    )
    # Rebuild the struct array under physical child names (pa.array.cast across mismatched
    # field names produces nulls — must reconstruct from the children directly).
    src_struct = logical_table.column("outer").chunk(0)
    physical_struct = pa.StructArray.from_arrays(
        [src_struct.field("a"), src_struct.field("b")],
        fields=[
            pa.field("col-a", pa.int64(), metadata={b"PARQUET:field_id": b"2"}),
            pa.field("col-b", pa.string(), metadata={b"PARQUET:field_id": b"3"}),
        ],
    )
    physical_outer = pa.field("col-outer", physical_struct.type, metadata={b"PARQUET:field_id": b"1"})
    physical_table = pa.Table.from_arrays([physical_struct], schema=pa.schema([physical_outer]))

    file_rel = f"part-{uuid.uuid4().hex}.parquet"
    pq.write_table(physical_table, path / file_rel)
    add = _add_action(file_rel, (path / file_rel).stat().st_size, {})

    actions = [
        {
            "protocol": {
                "minReaderVersion": 2,
                "minWriterVersion": 5,
            }
        },
        {
            "metaData": {
                "id": str(uuid.uuid4()),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_str,
                "partitionColumns": [],
                "configuration": {
                    "delta.columnMapping.mode": "name",
                    "delta.columnMapping.maxColumnId": "3",
                },
                "createdTime": 0,
            }
        },
        add,
    ]
    (log_dir / "00000000000000000000.json").write_text("\n".join(json.dumps(a) for a in actions))
    return logical_table


def test_deltalake_read_column_mapping_malformed_no_field_ids(tmp_path):
    """Mode set but schema fields lack `delta.columnMapping.id` — delta-rs rejects at load."""
    pytest.importorskip("deltalake")
    path = tmp_path / "cm_malformed"
    path.mkdir()
    (path / "_delta_log").mkdir()
    schema_str = json.dumps(
        {
            "type": "struct",
            "fields": [{"name": "a", "type": "long", "nullable": True, "metadata": {}}],
        }
    )
    actions = [
        {"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}},
        {
            "metaData": {
                "id": str(uuid.uuid4()),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_str,
                "partitionColumns": [],
                "configuration": {"delta.columnMapping.mode": "name"},
                "createdTime": 0,
            }
        },
    ]
    (path / "_delta_log" / "00000000000000000000.json").write_text("\n".join(json.dumps(a) for a in actions))
    with pytest.raises(Exception, match="delta.columnMapping.id"):
        daft.read_deltalake(str(path))


def test_deltalake_read_column_mapping_nested_struct(tmp_path):
    """Read a column-mapped table with a top-level struct column.

    Exercises `_iter_mapped_fields` recursion through nested structs plus the
    nested rename in `apply_field_ids_to_arrowrs_parquet_metadata`.
    """
    pytest.importorskip("deltalake")
    expected = _write_nested_struct_cm_table(tmp_path / "cm_nested")
    result = daft.read_deltalake(str(tmp_path / "cm_nested")).to_arrow()
    assert_pyarrow_tables_equal(result, expected)


@pytest.mark.parametrize("mode", ["name", "id"])
def test_deltalake_read_column_mapping_partitioned(tmp_path, mode):
    pytest.importorskip("deltalake")
    path = tmp_path / f"cm_{mode}_partitioned"

    data = pa.table(
        {
            "a": pa.array([1, 2, 3, 4], type=pa.int64()),
            "part": ["p1", "p1", "p2", "p2"],
        }
    )
    _write_cm_table(
        path,
        data,
        mode=mode,
        logical_to_physical={"a": "col-aaa", "part": "col-ppp"},
        field_ids={"a": 1, "part": 2},
        partition_columns=["part"],
    )

    sort_keys = [("part", "ascending"), ("a", "ascending")]
    result = daft.read_deltalake(str(path)).to_arrow().sort_by(sort_keys)
    assert_pyarrow_tables_equal(result, data.sort_by(sort_keys))
