from __future__ import annotations

import datetime
import uuid
import warnings

import pytest
import sqlalchemy
from sqlalchemy import DateTime, Integer, String, inspect

import daft
from daft.api_annotations import APITypeError
from tests.conftest import assert_df_equals


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

    df.write_sql(table_name, test_db, write_mode="append")

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
def test_write_sql_chunk_sizes(test_db):
    table_name = f"write_test_chunk_size_{uuid.uuid4().hex}"
    num_rows = 2000
    df = daft.from_pydict({"id": list(range(num_rows)), "val": [f"val_{i}" for i in range(num_rows)]})

    df.write_sql(table_name, test_db)

    read_df = daft.read_sql(f"SELECT * FROM {table_name}", test_db).collect()
    assert len(read_df) == num_rows

    pydict = read_df.to_pydict()
    assert sorted(pydict["id"]) == list(range(num_rows))


@pytest.mark.integration()
def test_write_sql_empty(test_db):
    table_name = f"write_test_empty_{uuid.uuid4().hex}"
    df = daft.from_pydict({"id": [], "name": []})
    df = df.select(df["id"].cast(daft.DataType.int64()), df["name"].cast(daft.DataType.string()))

    df.write_sql(table_name, test_db)

    read_df = daft.read_sql(f"SELECT * FROM {table_name}", test_db).collect()
    assert len(read_df) == 0
    assert read_df.column_names == ["id", "name"]


@pytest.mark.integration()
def test_write_sql_returns_metrics_dataframe(test_db):
    table_name = f"write_test_metrics_{uuid.uuid4().hex}"
    df = daft.from_pydict({"id": [1, 2, 3], "name": ["A", "B", "C"]})

    metrics_df = df.write_sql(table_name, test_db)

    assert metrics_df.column_names == ["total_written_rows", "total_written_bytes"]

    metrics = metrics_df.to_pydict()
    assert metrics["total_written_rows"][0] == 3
    assert metrics["total_written_bytes"][0] > 0


@pytest.mark.integration()
def test_write_sql_connection_factory(test_db):
    table_name = f"write_test_conn_factory_{uuid.uuid4().hex}"
    df = daft.from_pydict({"id": [1], "name": ["A"]})

    def create_conn():
        return sqlalchemy.create_engine(test_db).connect()

    df.write_sql(table_name, create_conn)

    read_df = daft.read_sql(f"SELECT * FROM {table_name}", test_db).collect()
    assert len(read_df) == 1
    assert read_df.to_pydict()["id"][0] == 1


@pytest.mark.integration()
def test_write_sql_schema_mismatch_append(test_db):
    table_name = f"write_test_mismatch_{uuid.uuid4().hex}"
    df1 = daft.from_pydict({"id": [1], "name": ["A"]})
    df1.write_sql(table_name, test_db)

    # Different schema (extra column)
    df2 = daft.from_pydict({"id": [2], "name": ["B"], "extra": [3]})

    # Appending data with extra columns should fail
    with pytest.raises(Exception):
        df2.write_sql(table_name, test_db, write_mode="append")


@pytest.mark.integration()
def test_write_sql_invalid_connection_string():
    df = daft.from_pydict({"a": [1]})
    with pytest.raises((ValueError, sqlalchemy.exc.ArgumentError)):
        df.write_sql("table", "invalid-protocol://host:port/db")


@pytest.mark.integration()
def test_write_sql_multi_db(test_db):
    table_name = f"write_test_multidb_{uuid.uuid4().hex}"
    df = daft.from_pydict({"id": [1, 2], "val": ["x", "y"]})

    df.write_sql(table_name, test_db)

    read_df = daft.read_sql(f"SELECT * FROM {table_name}", test_db).sort("id").collect()
    assert len(read_df) == 2
    data = read_df.to_pydict()
    assert data["id"] == [1, 2]
    assert data["val"] == ["x", "y"]


@pytest.mark.integration()
def test_write_sql_dtype_basic_types(test_db):
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

    column_types = {"id": Integer(), "name": String(length=64), "created_at": DateTime()}

    df.write_sql(table_name, test_db, column_types=column_types)

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

    column_types = {"id": Integer(), "name": String(length=64), "created_at": DateTime()}

    df.write_sql(table_name, test_db, column_types=column_types)

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

    column_types = {"id": Integer(), "name": String(length=64), "created_at": DateTime()}

    engine = sqlalchemy.create_engine(test_db)
    try:

        def create_conn():
            return sqlalchemy.create_engine(test_db).connect()

        df.write_sql(table_name, create_conn, column_types=column_types)

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
def test_write_sql_dtype_with_chunking(test_db):
    table_name = f"write_test_dtype_chunk_{uuid.uuid4().hex}"

    num_rows = 100
    data = {
        "id": list(range(num_rows)),
        "name": [f"name_{i}" for i in range(num_rows)],
        "created_at": [datetime.datetime(2024, 1, 1, 0, 0, 0) + datetime.timedelta(days=i) for i in range(num_rows)],
    }
    df = daft.from_pydict(data)

    column_types = {"id": Integer(), "name": String(length=64), "created_at": DateTime()}

    df.write_sql(table_name, test_db, column_types=column_types)

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


@pytest.mark.integration()
def test_write_sql_non_primitive_types_warning(test_db):
    table_name = f"write_test_non_primitive_{uuid.uuid4().hex}"

    # Create a DataFrame with non-primitive columns (list and struct)
    df = daft.from_pydict({"id": [1, 2], "list_col": [[1, 2], [3, 4]], "struct_col": [{"a": 1}, {"a": 2}]})

    # Verify that a warning is issued when non_primitive_handling is None (default)
    with pytest.warns(UserWarning, match="Detected non-primitive columns"):
        df.write_sql(table_name, test_db)

    # Verify data is written (as string/object usually)
    read_df = daft.read_sql(f"SELECT * FROM {table_name}", test_db).sort("id").collect()
    assert len(read_df) == 2

    # Just checking we got something back; exact representation depends on pandas/sqlalchemy/driver
    pydict = read_df.to_pydict()
    assert pydict["id"] == [1, 2]
    # The non-primitive columns should be present
    assert "list_col" in pydict
    assert "struct_col" in pydict


@pytest.mark.integration()
def test_write_sql_non_primitive_types_no_warning_explicit(test_db):
    table_name = f"write_test_non_primitive_nowarn_{uuid.uuid4().hex}"

    df = daft.from_pydict({"id": [1], "list_col": [[1, 2]]})

    # Verify NO warning is issued when non_primitive_handling is set
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")  # Cause all warnings to always be triggered.
        df.write_sql(table_name, test_db, non_primitive_handling="str")
        # Filter for our specific warning
        relevant_warnings = [
            x for x in w if issubclass(x.category, UserWarning) and "Detected non-primitive columns" in str(x.message)
        ]
        assert len(relevant_warnings) == 0


@pytest.fixture()
def postgres_adbc_db(test_db):
    if not test_db.startswith("postgres"):
        pytest.skip("ADBC integration tests run against PostgreSQL only")
    pytest.importorskip("adbc_driver_postgresql")
    return test_db


@pytest.mark.integration()
@pytest.mark.parametrize("write_mode", ["append", "overwrite", "fail"])
def test_write_sql_adbc_write_modes(postgres_adbc_db, write_mode):
    table_name = f"write_test_adbc_modes_{uuid.uuid4().hex}"

    initial_df = daft.from_pydict({"id": [1], "name": ["A"]})
    initial_df.write_sql(table_name, postgres_adbc_db, backend="adbc")
    new_df = daft.from_pydict({"id": [2], "name": ["B"]})

    if write_mode == "fail":
        with pytest.raises(ValueError, match="Table .* already exists"):
            new_df.write_sql(table_name, postgres_adbc_db, write_mode=write_mode, backend="adbc")
        expected = {"id": [1], "name": ["A"]}
    else:
        new_df.write_sql(table_name, postgres_adbc_db, write_mode=write_mode, backend="adbc")
        expected = {"id": [1, 2], "name": ["A", "B"]} if write_mode == "append" else {"id": [2], "name": ["B"]}

    read_df = daft.read_sql(f"SELECT * FROM {table_name}", postgres_adbc_db).sort("id")
    assert read_df.to_pydict() == expected


@pytest.mark.integration()
def test_write_sql_adbc_types_roundtrip(postgres_adbc_db):
    table_name = f"write_test_adbc_types_{uuid.uuid4().hex}"
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", None],
        "score": [1.5, None, 3.25],
        "flag": [True, False, None],
        "day": [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2), None],
        "created_at": [
            datetime.datetime(2024, 1, 1, 0, 0, 0),
            datetime.datetime(2024, 1, 2, 12, 30, 0),
            None,
        ],
        "payload": [b"\x00\x01", b"abc", None],
    }
    df = daft.from_pydict(data)

    df.write_sql(table_name, postgres_adbc_db, backend="adbc")

    read_df = daft.read_sql(f"SELECT * FROM {table_name}", postgres_adbc_db, backend="adbc").sort("id")
    assert read_df.to_pydict() == data


@pytest.mark.integration()
def test_write_sql_adbc_matches_sqlalchemy(postgres_adbc_db):
    num_rows = 2000
    df = daft.from_pydict({"id": list(range(num_rows)), "val": [f"val_{i}" for i in range(num_rows)]})
    adbc_table = f"write_test_adbc_cmp_{uuid.uuid4().hex}"
    sqlalchemy_table = f"write_test_sa_cmp_{uuid.uuid4().hex}"

    adbc_metrics = df.write_sql(adbc_table, postgres_adbc_db, backend="adbc").to_pydict()
    sqlalchemy_metrics = df.write_sql(sqlalchemy_table, postgres_adbc_db).to_pydict()

    assert adbc_metrics["total_written_rows"] == sqlalchemy_metrics["total_written_rows"] == [num_rows]
    adbc_rows = daft.read_sql(f"SELECT * FROM {adbc_table}", postgres_adbc_db).sort("id").to_pydict()
    sqlalchemy_rows = daft.read_sql(f"SELECT * FROM {sqlalchemy_table}", postgres_adbc_db).sort("id").to_pydict()
    assert adbc_rows == sqlalchemy_rows


@pytest.mark.integration()
def test_write_sql_adbc_appends_to_sqlalchemy_table(postgres_adbc_db):
    table_name = f"write_test_adbc_append_{uuid.uuid4().hex}"
    daft.from_pydict({"id": [1], "name": ["A"]}).write_sql(table_name, postgres_adbc_db)
    daft.from_pydict({"id": [2], "name": ["B"]}).write_sql(table_name, postgres_adbc_db, backend="adbc")

    read_df = daft.read_sql(f"SELECT * FROM {table_name}", postgres_adbc_db).sort("id")
    assert read_df.to_pydict() == {"id": [1, 2], "name": ["A", "B"]}


@pytest.mark.integration()
def test_write_sql_adbc_non_primitive_columns(postgres_adbc_db):
    table_name = f"write_test_adbc_non_primitive_{uuid.uuid4().hex}"
    df = daft.from_pydict({"id": [1, 2], "list_col": [[1, 2], [3]], "struct_col": [{"a": 1}, {"a": 2}]})

    df.write_sql(table_name, postgres_adbc_db, non_primitive_handling="str", backend="adbc")

    read_df = daft.read_sql(f"SELECT * FROM {table_name}", postgres_adbc_db).sort("id")
    assert read_df.to_pydict() == {"id": [1, 2], "list_col": ["[1, 2]", "[3]"], "struct_col": ['{"a": 1}', '{"a": 2}']}
