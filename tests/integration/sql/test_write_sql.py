from __future__ import annotations

import datetime
import uuid

import pytest
import sqlalchemy
from sqlalchemy import DateTime, Integer, String, inspect

import daft
from daft.api_annotations import APITypeError
from tests.conftest import assert_df_equals


def get_conn(test_db):
    return sqlalchemy.create_engine(test_db).connect()


@pytest.mark.integration()
@pytest.mark.parametrize("source", ["pydict", "csv", "json"])
def test_write_sql_from_sources(test_db, tmp_path, source):
    table_name = f"write_test_source_{uuid.uuid4().hex}"
    base_data = {"id": [1, 2], "name": ["A", "B"]}

    if source == "pydict":
        df = daft.from_pydict(base_data)
    elif source == "csv":
        csv_path = tmp_path / "input.csv"
        csv_path.write_text("id,name\n1,A\n2,B\n", encoding="utf-8")
        df = daft.read_csv(str(csv_path))
    elif source == "json":
        json_path = tmp_path / "input.json"
        json_path.write_text('[{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]', encoding="utf-8")
        df = daft.read_json(str(json_path))
    else:
        raise ValueError(f"Unsupported source type: {source}")

    df.write_sql(table_name, test_db)

    read_df = daft.read_sql(f"SELECT * FROM {table_name}", test_db).sort("id").collect()
    expected_df = daft.from_pydict(base_data).sort("id").collect()

    assert_df_equals(read_df.to_pandas(), expected_df.to_pandas(), sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("write_mode", ["append", "overwrite", "fail"])
def test_write_sql_write_modes(test_db, write_mode):
    table_name = f"write_test_modes_{uuid.uuid4().hex}"

    initial_df = daft.from_pydict({"id": [1], "name": ["A"]})
    initial_df.write_sql(table_name, test_db)

    new_df = daft.from_pydict({"id": [2], "name": ["B"]})

    if write_mode == "fail":
        with pytest.raises(ValueError, match="Table .* already exists"):
            new_df.write_sql(table_name, test_db, write_mode=write_mode)

        read_df = daft.read_sql(f"SELECT * FROM {table_name}", test_db).sort("id").collect()
        expected_df = initial_df.sort("id").collect()
        assert_df_equals(read_df.to_pandas(), expected_df.to_pandas(), sort_key="id")
    else:
        new_df.write_sql(table_name, test_db, write_mode=write_mode)
        read_df = daft.read_sql(f"SELECT * FROM {table_name}", test_db).sort("id").collect()

        if write_mode == "append":
            expected_df = daft.from_pydict({"id": [1, 2], "name": ["A", "B"]}).sort("id").collect()
        else:  # overwrite
            expected_df = new_df.sort("id").collect()

        assert_df_equals(read_df.to_pandas(), expected_df.to_pandas(), sort_key="id")


@pytest.mark.integration()
def test_write_sql_append_creates_table(test_db):
    table_name = f"write_test_append_new_{uuid.uuid4().hex}"
    df = daft.from_pydict({"id": [1], "name": ["A"]})

    def create_conn():
        return sqlalchemy.create_engine(test_db).connect()

    df.write_sql(table_name, create_conn, write_mode="append")

    read_df = daft.read_sql(f"SELECT * FROM {table_name}", test_db).sort("id").collect()
    expected_df = df.sort("id").collect()

    assert_df_equals(read_df.to_pandas(), expected_df.to_pandas(), sort_key="id")


@pytest.mark.integration()
def test_write_sql_invalid_mode(test_db):
    table_name = f"write_test_invalid_mode_{uuid.uuid4().hex}"
    df = daft.from_pydict({"id": [1]})

    with pytest.raises(APITypeError, match="write_sql received wrong input type"):
        df.write_sql(table_name, test_db, write_mode="invalid")


@pytest.mark.integration()
def test_write_sql_schema_mismatch_append(test_db):
    table_name = f"write_test_mismatch_{uuid.uuid4().hex}"
    df1 = daft.from_pydict({"id": [1], "name": ["A"]})
    df1.write_sql(table_name, test_db)

    df2 = daft.from_pydict({"id": [2], "name": ["B"], "extra": [3]})
    with pytest.raises(Exception):
        df2.write_sql(table_name, test_db, write_mode="append")


@pytest.mark.integration()
def test_write_sql_invalid_connection_string():
    df = daft.from_pydict({"a": [1]})
    with pytest.raises((ValueError, sqlalchemy.exc.ArgumentError)):
        df.write_sql("table", "invalid-protocol://host:port/db")


@pytest.mark.integration()
def test_write_sql_dtype_basic_types_with_metrics(test_db):
    table_name = f"write_test_dtype_basic_{uuid.uuid4().hex}"

    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "created_at": [
            datetime.datetime(2024, 1, 1, 0, 0, 0),
            datetime.datetime(2024, 1, 2, 0, 0, 0),
            datetime.datetime(2024, 1, 3, 0, 0, 0),
        ],
    }
    df = daft.from_pydict(data)

    dtype = {"id": Integer(), "name": String(length=64), "created_at": DateTime()}

    metrics_df = df.write_sql(table_name, test_db, dtype=dtype)
    assert metrics_df.column_names == ["total_written_rows", "total_written_bytes"]

    metrics = metrics_df.to_pydict()
    assert metrics["total_written_rows"][0] == 3
    assert metrics["total_written_bytes"][0] > 0

    engine = sqlalchemy.create_engine(test_db)
    try:
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        cols_by_name = {col["name"]: col for col in columns}

        id_type = str(cols_by_name["id"]["type"]).lower()
        assert "integer" in id_type or "int" in id_type

        name_type = str(cols_by_name["name"]["type"]).lower()
        assert any(k in name_type for k in ["varchar", "character varying", "text", "string"])

        created_at_type = str(cols_by_name["created_at"]["type"]).lower()
        assert "timestamp" in created_at_type or "datetime" in created_at_type
    finally:
        engine.dispose()

    read_df = daft.read_sql(f"SELECT * FROM {table_name}", test_db).sort("id").collect()
    expected_df = daft.from_pydict(data).sort("id").collect()

    assert_df_equals(
        read_df.to_pandas(coerce_temporal_nanoseconds=True),
        expected_df.to_pandas(coerce_temporal_nanoseconds=True),
        sort_key="id",
    )


@pytest.mark.integration()
def test_write_sql_dtype_empty_df_creates_table(test_db):
    table_name = f"write_test_dtype_empty_{uuid.uuid4().hex}"

    data = {"id": [], "name": [], "created_at": []}
    df = daft.from_pydict(data)

    dtype = {"id": Integer(), "name": String(length=64), "created_at": DateTime()}

    df.write_sql(table_name, test_db, dtype=dtype)

    engine = sqlalchemy.create_engine(test_db)
    try:
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        cols_by_name = {col["name"]: col for col in columns}

        assert set(cols_by_name.keys()) == {"id", "name", "created_at"}

        id_type = str(cols_by_name["id"]["type"]).lower()
        assert "integer" in id_type or "int" in id_type

        name_type = str(cols_by_name["name"]["type"]).lower()
        assert any(k in name_type for k in ["varchar", "character varying", "text", "string"])

        created_at_type = str(cols_by_name["created_at"]["type"]).lower()
        assert "timestamp" in created_at_type or "datetime" in created_at_type
    finally:
        engine.dispose()

    read_df = daft.read_sql(f"SELECT * FROM {table_name}", test_db).collect()
    assert len(read_df) == 0
    assert read_df.column_names == ["id", "name", "created_at"]


@pytest.mark.integration()
def test_write_sql_dtype_with_connection_factory(test_db):
    table_name = f"write_test_dtype_conn_factory_{uuid.uuid4().hex}"

    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "created_at": [
            datetime.datetime(2024, 1, 1, 0, 0, 0),
            datetime.datetime(2024, 1, 2, 0, 0, 0),
            datetime.datetime(2024, 1, 3, 0, 0, 0),
        ],
    }
    df = daft.from_pydict(data)

    dtype = {"id": Integer(), "name": String(length=64), "created_at": DateTime()}

    engine = sqlalchemy.create_engine(test_db)
    try:

        def create_conn():
            return sqlalchemy.create_engine(test_db).connect()

        df.write_sql(table_name, create_conn, dtype=dtype)

        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        cols_by_name = {col["name"]: col for col in columns}

        id_type = str(cols_by_name["id"]["type"]).lower()
        assert "integer" in id_type or "int" in id_type

        name_type = str(cols_by_name["name"]["type"]).lower()
        assert any(k in name_type for k in ["varchar", "character varying", "text", "string"])

        created_at_type = str(cols_by_name["created_at"]["type"]).lower()
        assert "timestamp" in created_at_type or "datetime" in created_at_type
    finally:
        engine.dispose()

    read_df = daft.read_sql(f"SELECT * FROM {table_name}", test_db).sort("id").collect()
    expected_df = daft.from_pydict(data).sort("id").collect()

    assert_df_equals(
        read_df.to_pandas(coerce_temporal_nanoseconds=True),
        expected_df.to_pandas(coerce_temporal_nanoseconds=True),
        sort_key="id",
    )


@pytest.mark.integration()
@pytest.mark.parametrize("chunk_size", [None, 50])
def test_write_sql_dtype_with_chunking(test_db, chunk_size):
    table_name = f"write_test_dtype_chunk_{uuid.uuid4().hex}"

    num_rows = 100
    data = {
        "id": list(range(num_rows)),
        "name": [f"name_{i}" for i in range(num_rows)],
        "created_at": [datetime.datetime(2024, 1, 1, 0, 0, 0) + datetime.timedelta(days=i) for i in range(num_rows)],
    }
    df = daft.from_pydict(data)

    dtype = {"id": Integer(), "name": String(length=64), "created_at": DateTime()}

    write_kwargs = {} if chunk_size is None else {"chunk_size": chunk_size}
    df.write_sql(table_name, test_db, dtype=dtype, **write_kwargs)

    read_df = daft.read_sql(f"SELECT * FROM {table_name}", test_db).sort("id").collect()
    expected_df = daft.from_pydict(data).sort("id").collect()

    assert len(read_df) == num_rows
    assert_df_equals(
        read_df.to_pandas(coerce_temporal_nanoseconds=True),
        expected_df.to_pandas(coerce_temporal_nanoseconds=True),
        sort_key="id",
    )

    engine = sqlalchemy.create_engine(test_db)
    try:
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        cols_by_name = {col["name"]: col for col in columns}

        id_type = str(cols_by_name["id"]["type"]).lower()
        assert "integer" in id_type or "int" in id_type
    finally:
        engine.dispose()
