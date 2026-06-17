from __future__ import annotations

import contextlib
import datetime
import decimal
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pytest

import daft
from daft.io.object_store_options import io_config_to_storage_options
from daft.logical.schema import Schema
from tests.conftest import get_tests_daft_runner_name


class _FakeCommitProperties:
    def __init__(self, custom_metadata):
        self.custom_metadata = custom_metadata


@pytest.fixture
def custom_metadata():
    return {"CUSTOM_METADATA": "1"}


@pytest.fixture()
def commit_properties():
    @contextlib.contextmanager
    def _(deltalake):
        setattr(deltalake, "CommitProperties", _FakeCommitProperties)
        try:
            yield
        finally:
            delattr(deltalake, "CommitProperties")

    return _


def test_deltalake_write_basic(tmp_path, base_table):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"
    df = daft.from_arrow(base_table)
    result = df.write_deltalake(str(path))
    result = result.to_pydict()
    assert result["operation"] == ["ADD"]
    assert result["rows"] == [base_table.num_rows]

    read_delta = deltalake.DeltaTable(str(path))
    expected_schema = Schema.from_pyarrow_schema(pa.schema(read_delta.schema().to_arrow()))
    assert df.schema() == expected_schema
    assert read_delta.to_pyarrow_table() == base_table


def test_deltalake_multi_write_basic(tmp_path, base_table):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"
    df = daft.from_arrow(base_table)
    df.write_deltalake(str(path))

    result = df.write_deltalake(str(path))
    result = result.to_pydict()
    assert result["operation"] == ["ADD"]
    assert result["rows"] == [base_table.num_rows]

    read_delta = deltalake.DeltaTable(str(path))
    expected_schema = Schema.from_pyarrow_schema(pa.schema(read_delta.schema().to_arrow()))
    assert df.schema() == expected_schema
    assert read_delta.version() == 1
    assert read_delta.to_pyarrow_table() == pa.concat_tables([base_table, base_table])


def test_deltalake_write_cloud(base_table, cloud_paths):
    deltalake = pytest.importorskip("deltalake")
    path, io_config = cloud_paths
    df = daft.from_arrow(base_table)
    result = df.write_deltalake(str(path), io_config=io_config)
    result = result.to_pydict()
    assert result["operation"] == ["ADD"]
    assert result["rows"] == [base_table.num_rows]
    storage_options = io_config_to_storage_options(io_config, path) if io_config is not None else None
    read_delta = deltalake.DeltaTable(str(path), storage_options=storage_options)
    expected_schema = Schema.from_pyarrow_schema(pa.schema(read_delta.schema().to_arrow()))
    assert df.schema() == expected_schema
    assert read_delta.to_pyarrow_table() == base_table


def test_deltalake_write_overwrite_basic(tmp_path):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"
    df1 = daft.from_pydict({"a": [1, 2]})
    df1.write_deltalake(str(path))

    df2 = daft.from_pydict({"a": [3, 4]})
    result = df2.write_deltalake(str(path), mode="overwrite")
    result = result.to_pydict()
    assert result["operation"] == ["ADD", "DELETE"]
    assert result["rows"] == [2, 2]

    read_delta = deltalake.DeltaTable(str(path))
    expected_schema = Schema.from_pyarrow_schema(pa.schema(read_delta.schema().to_arrow()))
    assert df2.schema() == expected_schema
    assert read_delta.to_pyarrow_table() == df2.to_arrow()


def test_deltalake_write_overwrite_cloud(cloud_paths):
    deltalake = pytest.importorskip("deltalake")
    path, io_config = cloud_paths
    df1 = daft.from_pydict({"a": [1, 2]})
    df1.write_deltalake(str(path), io_config=io_config)

    df2 = daft.from_pydict({"a": [3, 4]})
    result = df2.write_deltalake(str(path), mode="overwrite", io_config=io_config)
    result = result.to_pydict()
    assert result["operation"] == ["ADD", "DELETE"]
    assert result["rows"] == [2, 2]

    storage_options = io_config_to_storage_options(io_config, path) if io_config is not None else None
    read_delta = deltalake.DeltaTable(str(path), storage_options=storage_options)
    expected_schema = Schema.from_pyarrow_schema(pa.schema(read_delta.schema().to_arrow()))
    assert df2.schema() == expected_schema
    assert read_delta.to_pyarrow_table() == df2.to_arrow()


@pytest.mark.skipif(
    get_tests_daft_runner_name() == "native",
    reason="Native executor does not support repartitioning",
)
def test_deltalake_write_overwrite_multi_partition(tmp_path):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"
    df1 = daft.from_pydict({"a": [1, 2, 3, 4]})
    df1 = df1.repartition(2)
    df1.write_deltalake(str(path))

    df2 = daft.from_pydict({"a": [5, 6, 7, 8]})
    df2 = df2.repartition(2)
    result = df2.write_deltalake(str(path), mode="overwrite")
    result = result.to_pydict()
    assert result["operation"] == ["ADD", "ADD", "DELETE", "DELETE"]

    read_delta = deltalake.DeltaTable(str(path))
    expected_schema = Schema.from_pyarrow_schema(pa.schema(read_delta.schema().to_arrow()))
    assert df2.schema() == expected_schema
    assert read_delta.to_pyarrow_table() == df2.to_arrow()


def test_deltalake_write_overwrite_schema(tmp_path):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"
    df1 = daft.from_pydict({"a": [1, 2]})
    df1.write_deltalake(str(path))

    df2 = daft.from_pydict({"b": [3, 4]})
    result = df2.write_deltalake(str(path), mode="overwrite", schema_mode="overwrite")
    result = result.to_pydict()
    assert result["operation"] == ["ADD", "DELETE"]

    read_delta = deltalake.DeltaTable(str(path))
    expected_schema = Schema.from_pyarrow_schema(pa.schema(read_delta.schema().to_arrow()))
    assert df2.schema() == expected_schema
    assert read_delta.to_pyarrow_table() == df2.to_arrow()


def test_deltalake_write_overwrite_error_schema(tmp_path):
    path = tmp_path / "some_table"
    df1 = daft.from_pydict({"a": [1, 2]})
    df1.write_deltalake(str(path), mode="overwrite")
    df2 = daft.from_pydict({"b": [3, 4]})
    with pytest.raises(ValueError):
        df2.write_deltalake(str(path), mode="overwrite")


def test_deltalake_write_error(tmp_path, base_table):
    path = tmp_path / "some_table"
    df = daft.from_arrow(base_table)
    df.write_deltalake(str(path), mode="error")
    with pytest.raises(AssertionError):
        df.write_deltalake(str(path), mode="error")


def test_deltalake_write_ignore(tmp_path):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"
    df1 = daft.from_pydict({"a": [1, 2]})
    df1.write_deltalake(str(path), mode="ignore")
    df2 = daft.from_pydict({"a": [3, 4]})
    result = df2.write_deltalake(str(path), mode="ignore")
    result = result.to_arrow()
    assert result.num_rows == 0

    read_delta = deltalake.DeltaTable(str(path))
    expected_schema = Schema.from_pyarrow_schema(pa.schema(read_delta.schema().to_arrow()))
    assert df1.schema() == expected_schema
    assert read_delta.to_pyarrow_table() == df1.to_arrow()


@pytest.mark.skipif(
    get_tests_daft_runner_name() == "native",
    reason="Native executor does not support repartitioning",
)
def test_deltalake_write_with_empty_partition(tmp_path, base_table):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"
    df = daft.from_arrow(base_table).into_partitions(4)
    result = df.write_deltalake(str(path))
    result = result.to_pydict()
    assert result["operation"] == ["ADD", "ADD", "ADD"]
    assert result["rows"] == [1, 1, 1]

    read_delta = deltalake.DeltaTable(str(path))
    expected_schema = Schema.from_pyarrow_schema(pa.schema(read_delta.schema().to_arrow()))
    assert df.schema() == expected_schema
    assert read_delta.to_pyarrow_table() == base_table


def check_equal_both_daft_and_delta_rs(df: daft.DataFrame, path: Path, sort_order: list[tuple[str, str]]):
    deltalake = pytest.importorskip("deltalake")

    arrow_df = df.to_arrow().sort_by(sort_order)

    read_daft = daft.read_deltalake(str(path))
    assert read_daft.schema() == df.schema()
    assert read_daft.to_arrow().sort_by(sort_order) == arrow_df

    read_delta = deltalake.DeltaTable(str(path))
    expected_schema = Schema.from_pyarrow_schema(pa.schema(read_delta.schema().to_arrow()))
    assert df.schema() == expected_schema
    assert read_delta.to_pyarrow_table().cast(expected_schema.to_pyarrow_schema()).sort_by(sort_order) == arrow_df


@pytest.mark.parametrize(
    "partition_cols,num_partitions",
    [
        (["int"], 3),
        (["float"], 3),
        (["str"], 3),
        pytest.param(["bin"], 3, marks=pytest.mark.xfail(reason="Binary partitioning is not yet supported")),
        (["bool"], 3),
        (["datetime"], 3),
        (["date"], 3),
        (["decimal"], 3),
        (["int", "float"], 4),
    ],
)
def test_deltalake_write_partitioned(tmp_path, partition_cols, num_partitions):
    path = tmp_path / "some_table"
    df = daft.from_pydict(
        {
            "int": [1, 1, 2, None],
            "float": [1.1, 2.2, 2.2, None],
            "str": ["foo", "foo", "bar", None],
            "bin": [b"foo", b"foo", b"bar", None],
            "bool": [True, True, False, None],
            "datetime": [
                datetime.datetime(2024, 2, 10),
                datetime.datetime(2024, 2, 10),
                datetime.datetime(2024, 2, 11),
                None,
            ],
            "date": [datetime.date(2024, 2, 10), datetime.date(2024, 2, 10), datetime.date(2024, 2, 11), None],
            "decimal": pa.array(
                [decimal.Decimal("1111.111"), decimal.Decimal("1111.111"), decimal.Decimal("2222.222"), None],
                type=pa.decimal128(7, 3),
            ),
        }
    )
    result = df.write_deltalake(str(path), partition_cols=partition_cols)
    result = result.to_pydict()
    assert len(result["operation"]) == num_partitions
    assert all(op == "ADD" for op in result["operation"])
    assert sum(result["rows"]) == len(df)

    sort_order = [("int", "ascending"), ("float", "ascending")]
    check_equal_both_daft_and_delta_rs(df, path, sort_order)


def test_deltalake_write_partitioned_empty(tmp_path):
    path = tmp_path / "some_table"

    df = daft.from_arrow(pa.schema([("int", pa.int64()), ("string", pa.string())]).empty_table())

    df.write_deltalake(str(path), partition_cols=["int"])

    check_equal_both_daft_and_delta_rs(df, path, [("int", "ascending")])


def test_deltalake_write_partitioned_some_empty(tmp_path):
    path = tmp_path / "some_table"

    df = daft.from_pydict({"int": [1, 2, 3, None], "string": ["foo", "foo", "bar", None]}).into_partitions(5)

    df.write_deltalake(str(path), partition_cols=["int"])

    check_equal_both_daft_and_delta_rs(df, path, [("int", "ascending")])


def test_deltalake_write_partitioned_existing_table(tmp_path):
    path = tmp_path / "some_table"

    df1 = daft.from_pydict({"int": [1], "string": ["foo"]})
    result = df1.write_deltalake(str(path), partition_cols=["int"])
    result = result.to_pydict()
    assert result["operation"] == ["ADD"]
    assert result["rows"] == [1]

    df2 = daft.from_pydict({"int": [1, 2], "string": ["bar", "bar"]})
    with pytest.raises(ValueError):
        df2.write_deltalake(str(path), partition_cols=["string"])

    result = df2.write_deltalake(str(path))
    result = result.to_pydict()
    assert result["operation"] == ["ADD", "ADD"]
    assert result["rows"] == [1, 1]

    check_equal_both_daft_and_delta_rs(df1.concat(df2), path, [("int", "ascending"), ("string", "ascending")])


def test_deltalake_write_roundtrip(tmp_path):
    path = tmp_path / "some_table"
    df = daft.from_pydict({"a": [1, 2, 3, 4]})
    df.write_deltalake(str(path))

    read_df = daft.read_deltalake(str(path))
    assert df.schema() == read_df.schema()
    assert df.to_arrow() == read_df.to_arrow()


def test_custom_metadata_added_for_new_table(tmp_path, custom_metadata):
    # import deltalake
    deltalake = pytest.importorskip("deltalake")

    path = tmp_path / "some_table"
    df = daft.from_pydict({"a": [1, 2, 3, 4]})
    df.write_deltalake(str(path), custom_metadata=custom_metadata)

    table = deltalake.DeltaTable(path)
    history = table.history(1)

    assert custom_metadata.items() <= history[0].items()
    assert "operationMetrics" in history[0]


def test_custom_metadata_updated_for_existing_table(tmp_path, custom_metadata):
    """Tests for deltalake version installed in the current environment (currently 0.19.2)."""
    # import deltalake
    deltalake = pytest.importorskip("deltalake")

    path = tmp_path / "some_table"
    df = daft.from_pydict({"a": [1, 2, 3, 4]})
    df.write_deltalake(str(path))

    df = daft.from_pydict({"a": [5, 6]})
    df.write_deltalake(str(path), custom_metadata=custom_metadata, mode="append")

    table = deltalake.DeltaTable(path)
    history = table.history(1)

    assert custom_metadata.items() <= history[0].items()
    assert "operationMetrics" in history[0]


def test_custom_metadata_updated_for_existing_table_with_commit_properties(
    tmp_path, custom_metadata, commit_properties
):
    deltalake = pytest.importorskip("deltalake")
    from deltalake._internal import RawDeltaTable

    # write once to get into the table is not None path
    path = tmp_path / "some_table"
    df = daft.from_pydict({"a": [1, 2, 3, 4]})
    df.write_deltalake(str(path))

    # Add mocked CommitProperties class introduced in 0.20.0
    with commit_properties(deltalake), patch.object(RawDeltaTable, "create_write_transaction") as mock_method:
        df = daft.from_pydict({"a": [5, 6]})
        df.write_deltalake(str(path), custom_metadata=custom_metadata, mode="append")

        mock_method.assert_called_once()
        (_, _, _, _, _, custom_metadata_arg) = mock_method.call_args[0]

        assert isinstance(custom_metadata_arg, _FakeCommitProperties)
        assert custom_metadata.items() <= custom_metadata_arg.custom_metadata.items()
        assert "operationMetrics" in custom_metadata_arg.custom_metadata


def test_operation_metrics_added_for_new_table(tmp_path):
    deltalake = pytest.importorskip("deltalake")

    path = tmp_path / "some_table"
    daft.from_pydict({"a": [1, 2, 3, 4]}).write_deltalake(str(path))

    history_entry = deltalake.DeltaTable(path).history(1)[0]
    assert "operationMetrics" in history_entry


def test_operation_metrics_added_for_existing_table_append(tmp_path):
    deltalake = pytest.importorskip("deltalake")

    path = tmp_path / "some_table"
    daft.from_pydict({"a": [1, 2]}).write_deltalake(str(path))
    daft.from_pydict({"a": [3, 4, 5]}).write_deltalake(str(path), mode="append")

    history_entry = deltalake.DeltaTable(path).history(1)[0]
    assert "operationMetrics" in history_entry


def test_operation_metrics_added_for_existing_table_overwrite(tmp_path):
    deltalake = pytest.importorskip("deltalake")

    path = tmp_path / "some_table"
    daft.from_pydict({"a": [1, 2]}).write_deltalake(str(path))
    daft.from_pydict({"a": [8]}).write_deltalake(str(path), mode="overwrite")

    history_entry = deltalake.DeltaTable(path).history(1)[0]
    assert "operationMetrics" in history_entry


def test_delete_deltalake_basic(tmp_path):
    path = tmp_path / "some_table"
    daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]}).write_deltalake(str(path))

    result = daft.delete_deltalake(str(path), predicate="a >= 2")
    assert isinstance(result, dict)

    rows = daft.read_deltalake(str(path)).to_pydict()
    assert rows == {"a": [1], "b": ["x"]}


def test_update_deltalake_basic(tmp_path):
    path = tmp_path / "some_table"
    daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]}).write_deltalake(str(path))

    result = daft.update_deltalake(str(path), updates={"b": "'updated'"}, predicate="a >= 2")
    assert isinstance(result, dict)

    rows = daft.read_deltalake(str(path)).sort("a").to_pydict()
    assert rows == {"a": [1, 2, 3], "b": ["x", "updated", "updated"]}


def test_merge_deltalake_scenario_0_initial_load(tmp_path):
    """Scenario 0: First run with no existing persistent entity -> all entities marked as ADDED"""
    path = tmp_path / "entity_table"
    
    # Initial source data (first load)
    source = daft.from_pydict({
        "entity_id": [1, 2, 3],
        "attributes": ["attr-a", "attr-b", "attr-c"],
        "status": ["ADDED", "ADDED", "ADDED"],
    })
    
    # For first load, write directly with ADDED status
    source.write_deltalake(str(path))
    
    rows = daft.read_deltalake(str(path)).sort("entity_id").to_pydict()
    assert rows == {
        "entity_id": [1, 2, 3],
        "attributes": ["attr-a", "attr-b", "attr-c"],
        "status": ["ADDED", "ADDED", "ADDED"]
    }


def test_merge_deltalake_scenario_1_new_entity(tmp_path):
    """Scenario 1: New entity in source -> marked as ADDED"""
    path = tmp_path / "entity_table"
    
    # Initial state: entities 1, 2
    daft.from_pydict({
        "entity_id": [1, 2],
        "attributes": ["attr-a", "attr-b"],
        "status": ["ADDED", "ADDED"],
    }).write_deltalake(str(path))
    
    # Source with entity 1, 2, and NEW entity 3
    source = daft.from_pydict({
        "entity_id": [1, 2, 3],
        "attributes": ["attr-a", "attr-b", "attr-c"],
    })
    
    result = (
        daft.merge_deltalake(
            str(path),
            source=source,
            predicate="target.entity_id = source.entity_id"
        )
        .when_matched_update_all(except_cols=["status"])
        .when_not_matched_insert(updates={"entity_id": "source.entity_id", "attributes": "source.attributes", "status": "'ADDED'"})
        .execute()
    )

    assert isinstance(result, daft.DataFrame)
    metrics = result._metadata["merge_metrics"]
    assert "num_target_rows_inserted" in metrics
    rows = daft.read_deltalake(str(path)).sort("entity_id").to_pydict()
    # Entity 3 should be marked as ADDED
    assert rows == {
        "entity_id": [1, 2, 3],
        "attributes": ["attr-a", "attr-b", "attr-c"],
        "status": ["ADDED", "ADDED", "ADDED"]
    }


def test_merge_deltalake_scenario_3_unchanged_entity(tmp_path):
    """Scenario 3: Entity with same attribute values -> marked as UNCHANGED"""
    path = tmp_path / "entity_table"
    
    # Initial state with entities
    daft.from_pydict({
        "entity_id": [1, 2],
        "attributes": ["attr-a", "attr-b"],
        "status": ["UPDATED", "UPDATED"],
    }).write_deltalake(str(path))
    
    # Source with SAME attributes for entities 1 and 2
    source = daft.from_pydict({
        "entity_id": [1, 2],
        "attributes": ["attr-a", "attr-b"],
    })
    
    result = (
        daft.merge_deltalake(
            str(path),
            source=source,
            predicate="target.entity_id = source.entity_id"
        )
        .when_matched_update(
            updates={"status": "'UNCHANGED'"},
            predicate="source.attributes = target.attributes"
        )
        .execute()
    )

    assert isinstance(result, daft.DataFrame)
    rows = daft.read_deltalake(str(path)).sort("entity_id").to_pydict()
    # Both entities should now be marked as UNCHANGED
    assert rows == {
        "entity_id": [1, 2],
        "attributes": ["attr-a", "attr-b"],
        "status": ["UNCHANGED", "UNCHANGED"]
    }


def test_merge_deltalake_scenario_4_updated_entity(tmp_path):
    """Scenario 4: Entity with different attribute values -> marked as UPDATED"""
    path = tmp_path / "entity_table"
    
    # Initial state with entities
    daft.from_pydict({
        "entity_id": [1, 2],
        "attributes": ["attr-a-old", "attr-b-old"],
        "status": ["ADDED", "ADDED"],
    }).write_deltalake(str(path))
    
    # Source with DIFFERENT attributes for entity 1, SAME for entity 2
    source = daft.from_pydict({
        "entity_id": [1, 2],
        "attributes": ["attr-a-new", "attr-b-old"],
    })
    
    result = (
        daft.merge_deltalake(
            str(path),
            source=source,
            predicate="target.entity_id = source.entity_id"
        )
        .when_matched_update(
            updates={"attributes": "source.attributes", "status": "'UPDATED'"},
            predicate="source.attributes != target.attributes"
        )
        .when_matched_update(
            updates={"status": "'UNCHANGED'"},
            predicate="source.attributes = target.attributes"
        )
        .execute()
    )

    assert isinstance(result, daft.DataFrame)
    rows = daft.read_deltalake(str(path)).sort("entity_id").to_pydict()
    # Entity 1: UPDATED (different attributes)
    # Entity 2: UNCHANGED (same attributes)
    assert rows == {
        "entity_id": [1, 2],
        "attributes": ["attr-a-new", "attr-b-old"],
        "status": ["UPDATED", "UNCHANGED"]
    }


def test_merge_deltalake_scenario_2_deleted_entity_with_antijoin(tmp_path):
    """Scenario 2: Entity deleted from source -> marked as DELETED (via anti-join + update)"""
    path = tmp_path / "entity_table"
    
    # Initial state with entities 1, 2, 3
    daft.from_pydict({
        "entity_id": [1, 2, 3],
        "attributes": ["attr-a", "attr-b", "attr-c"],
        "status": ["ADDED", "ADDED", "ADDED"],
    }).write_deltalake(str(path))
    
    # Source with only entities 1 and 2 (entity 3 is deleted from source)
    source = daft.from_pydict({
        "entity_id": [1, 2],
        "attributes": ["attr-a", "attr-b"],
    })
    
    result = (
        daft.merge_deltalake(
            str(path),
            source=source,
            predicate="target.entity_id = source.entity_id"
        )
        .when_not_matched_by_source_update(
            updates={"status": "'DELETED'"}
        )
        .execute()
    )

    assert isinstance(result, daft.DataFrame)
    
    rows = daft.read_deltalake(str(path)).sort("entity_id").to_pydict()
    # Entity 3 should now be marked as DELETED
    assert rows == {
        "entity_id": [1, 2, 3],
        "attributes": ["attr-a", "attr-b", "attr-c"],
        "status": ["ADDED", "ADDED", "DELETED"]
    }


def test_merge_deltalake_delete_apis(tmp_path):
    """Test the delete-oriented merge APIs exposed by TableMerger."""
    path = tmp_path / "delete_table"
    daft.from_pydict({"id": [1, 2, 3], "value": ["a", "b", "c"]}).write_deltalake(str(path))

    source = daft.from_pydict({"id": [1, 3], "value": ["a", "c"]})
    result = (
        daft.merge_deltalake(
            str(path),
            source=source,
            predicate="target.id = source.id"
        )
        .when_matched_delete(predicate="source.id = 1")
        .when_not_matched_by_source_delete()
        .execute()
    )

    assert isinstance(result, daft.DataFrame)
    rows = daft.read_deltalake(str(path)).sort("id").to_pydict()
    assert rows == {"id": [3], "value": ["c"]}


def test_merge_deltalake_multiple_when_matched(tmp_path):
    """Test merge with multiple when_matched_update clauses with different predicates"""
    path = tmp_path / "some_table"
    daft.from_pydict({"id": [1, 2], "status": ["active", "inactive"], "value": [100, 200]}).write_deltalake(str(path))

    source = daft.from_pydict({"id": [1, 2], "status": ["active", "inactive"], "value": [150, 250]})
    result = (
        daft.merge_deltalake(
            str(path),
            source=source,
            predicate="target.id = source.id"
        )
        .when_matched_update(
            updates={"value": "source.value"},
            predicate="source.status = 'active'"
        )
        .when_matched_update(
            updates={"value": "'999'"},
            predicate="source.status = 'inactive'"
        )
        .execute()
    )

    assert isinstance(result, daft.DataFrame)
    rows = daft.read_deltalake(str(path)).sort("id").to_pydict()
    assert rows == {"id": [1, 2], "status": ["active", "inactive"], "value": [150, 999]}


def test_merge_deltalake_with_predicates(tmp_path):
    """Test merge with both when_matched_update and when_not_matched_insert"""
    path = tmp_path / "some_table"
    daft.from_pydict({"id": [1, 2], "value": [100, 200]}).write_deltalake(str(path))

    source = daft.from_pydict({"id": [2, 3], "value": [250, 350]})
    result = (
        daft.merge_deltalake(
            str(path),
            source=source,
            predicate="target.id = source.id"
        )
        .when_matched_update({"value": "source.value"})
        .when_not_matched_insert({"id": "source.id", "value": "source.value"})
        .execute()
    )

    assert isinstance(result, daft.DataFrame)
    rows = daft.read_deltalake(str(path)).sort("id").to_pydict()
    assert rows == {"id": [1, 2, 3], "value": [100, 250, 350]}


def test_merge_deltalake_dataframe_api(tmp_path):
    """Test DataFrame.merge_deltalake method"""
    path = tmp_path / "some_table"
    daft.from_pydict({"id": [1, 2], "value": ["old-1", "old-2"]}).write_deltalake(str(path))

    source = daft.from_pydict({"id": [2, 3], "value": ["new-2", "new-3"]})
    result = (
        source.merge_deltalake(
            str(path),
            predicate="target.id = source.id"
        )
        .when_matched_update({"value": "source.value"})
        .when_not_matched_insert({"id": "source.id", "value": "source.value"})
        .execute()
    )

    assert isinstance(result, daft.DataFrame)
    rows = daft.read_deltalake(str(path)).sort("id").to_pydict()
    assert rows == {"id": [1, 2, 3], "value": ["old-1", "new-2", "new-3"]}


def test_history_deltalake_exposes_operation_metrics(tmp_path):
    path = tmp_path / "some_table"
    daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]}).write_deltalake(str(path))
    daft.update_deltalake(str(path), updates={"b": "'updated'"}, predicate="a >= 2")
    daft.delete_deltalake(str(path), predicate="a = 1")

    history = daft.history_deltalake(str(path), limit=3)
    assert len(history) == 3
    assert all("operationMetrics" in entry for entry in history)
    assert all(isinstance(entry["operationMetrics"], dict) for entry in history)




def test_merge_deltalake_returns_metrics(tmp_path):
    """Test that merge_deltalake returns formatted metrics"""
    path = tmp_path / "metrics_table"
    
    # Initial table with 2 rows
    daft.from_pydict({
        "id": [1, 2],
        "value": ["a", "b"]
    }).write_deltalake(str(path))
    
    # Merge source: update row 2, insert row 3
    source = daft.from_pydict({
        "id": [2, 3],
        "value": ["b-updated", "c"]
    })
    
    metrics = (
        daft.merge_deltalake(
            str(path),
            source=source,
            predicate="target.id = source.id"
        )
        .when_matched_update({"value": "source.value"})
        .when_not_matched_insert({"id": "source.id", "value": "source.value"})
        .execute()
    )
    
    # Verify metrics structure
    assert isinstance(metrics, daft.DataFrame)
    raw_metrics = metrics._metadata["merge_metrics"]
    assert "num_target_rows_inserted" in raw_metrics
    assert "num_target_rows_updated" in raw_metrics
    assert "num_target_rows_deleted" in raw_metrics
    assert "num_target_rows_copied" in raw_metrics
    assert "num_target_files_added" in raw_metrics
    assert "num_target_files_removed" in raw_metrics
    assert "execution_time_ms" in raw_metrics
    assert "scan_time_ms" in raw_metrics
    assert "rewrite_time_ms" in raw_metrics
    assert raw_metrics["num_target_rows_inserted"] == 1  # One row inserted
    assert raw_metrics["num_target_rows_updated"] == 1   # One row updated
    assert raw_metrics["num_target_rows_deleted"] == 0   # No deletions
    
    # Verify data
    rows = daft.read_deltalake(str(path)).sort("id").to_pydict()
    assert rows == {"id": [1, 2, 3], "value": ["a", "b-updated", "c"]}


def test_merge_deltalake_metrics_with_conditional_predicates(tmp_path):
    """Test merge_deltalake with multiple conditional predicates returns correct metrics"""
    path = tmp_path / "conditional_metrics"
    
    # Initial table
    daft.from_pydict({
        "id": [1, 2, 3],
        "status": ["active", "inactive", "active"],
        "value": [100, 200, 300]
    }).write_deltalake(str(path))
    
    # Merge source with updates and new rows
    source = daft.from_pydict({
        "id": [2, 3, 4, 5],
        "status": ["inactive", "active", "new", "new"],
        "value": [250, 350, 400, 500]
    })
    
    result = (
        daft.merge_deltalake(
            str(path),
            source=source,
            predicate="target.id = source.id"
        )
        .when_matched_update(
            updates={"value": "source.value"},
            predicate="source.status = 'active'"
        )
        .when_matched_update(
            updates={"value": "'999'"},
            predicate="source.status = 'inactive'"
        )
        .when_not_matched_insert({"id": "source.id", "status": "source.status", "value": "source.value"})
        .execute()
    )
    
    # Verify metrics structure
    assert isinstance(result, daft.DataFrame)
    metrics = result._metadata["merge_metrics"]
    assert metrics["num_target_rows_inserted"] == 2  # Rows 4 and 5
    assert metrics["num_target_rows_updated"] == 2   # Rows 2 and 3
    
    # Verify data
    rows = daft.read_deltalake(str(path)).sort("id").to_pydict()
    assert rows["id"] == [1, 2, 3, 4, 5]
    assert rows["value"] == [100, 999, 350, 400, 500]


def test_merge_deltalake_dataframe_returns_metrics(tmp_path):
    """Test that DataFrame.merge_deltalake returns metrics"""
    path = tmp_path / "df_metrics"
    
    # Initial table
    daft.from_pydict({
        "id": [1, 2],
        "name": ["alice", "bob"]
    }).write_deltalake(str(path))
    
    # Source DataFrame
    source = daft.from_pydict({
        "id": [2, 3],
        "name": ["bob-updated", "charlie"]
    })
    
    # Use DataFrame API
    metrics = (
        source.merge_deltalake(
            str(path),
            predicate="target.id = source.id"
        )
        .when_matched_update({"name": "source.name"})
        .when_not_matched_insert({"id": "source.id", "name": "source.name"})
        .execute()
    )
    
    # Verify metrics returned
    assert isinstance(metrics, daft.DataFrame)
    raw_metrics = metrics._metadata["merge_metrics"]
    assert raw_metrics["num_target_rows_updated"] == 1
    assert raw_metrics["num_target_rows_inserted"] == 1
    assert "num_target_rows_copied" in raw_metrics
    
    # Verify data
    rows = daft.read_deltalake(str(path)).sort("id").to_pydict()
    assert rows == {"id": [1, 2, 3], "name": ["alice", "bob-updated", "charlie"]}


