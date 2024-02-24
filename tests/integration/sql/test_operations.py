from __future__ import annotations

import sqlite3
import tempfile

import pandas as pd
import pytest

import daft


# Fixture for temporary SQLite database
@pytest.fixture(scope="module")
def temp_sqllite_db(test_items):
    data = list(
        zip(
            test_items["sepal_length"],
            test_items["sepal_width"],
            test_items["petal_length"],
            test_items["petal_width"],
            test_items["variety"],
        )
    )
    with tempfile.NamedTemporaryFile(suffix=".db") as file:
        connection = sqlite3.connect(file.name)
        connection.execute(
            "CREATE TABLE iris (sepal_length REAL, sepal_width REAL, petal_length REAL, petal_width REAL, variety TEXT)"
        )
        connection.executemany("INSERT INTO iris VALUES (?, ?, ?, ?, ?)", data)
        connection.commit()
        connection.close()
        yield file.name


@pytest.mark.integration()
def test_sqllite_create_dataframe_ok(temp_sqllite_db) -> None:
    df = daft.read_sql(
        "SELECT * FROM iris", f"sqlite://{temp_sqllite_db}"
    )  # path here only has 2 slashes instead of 3 because connectorx uses 2 slashes
    pd_df = pd.read_sql("SELECT * FROM iris", f"sqlite:///{temp_sqllite_db}")

    assert df.to_pandas().equals(pd_df)


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2, 3])
def test_sqllite_partitioned_read(temp_sqllite_db, num_partitions) -> None:
    df = daft.read_sql(f"SELECT * FROM iris LIMIT {50 * num_partitions}", f"sqlite://{temp_sqllite_db}")
    assert df.num_partitions() == num_partitions
    df = df.collect()
    assert len(df) == 50 * num_partitions

    # test with a number of rows that is not a multiple of 50
    df = daft.read_sql(f"SELECT * FROM iris LIMIT {50 * num_partitions + 1}", f"sqlite://{temp_sqllite_db}")
    assert df.num_partitions() == num_partitions + 1
    df = df.collect()
    assert len(df) == 50 * num_partitions + 1


@pytest.mark.integration()
def test_sqllite_read_with_filter_pushdowns(temp_sqllite_db) -> None:
    df = daft.read_sql("SELECT * FROM iris", f"sqlite://{temp_sqllite_db}")
    df = df.where(df["sepal_length"] > 5.0)
    df = df.where(df["sepal_width"] > 3.0)

    pd_df = pd.read_sql("SELECT * FROM iris", f"sqlite:///{temp_sqllite_db}")
    pd_df = pd_df[pd_df["sepal_length"] > 5.0]
    pd_df = pd_df[pd_df["sepal_width"] > 3.0]

    df = df.to_pandas().sort_values("sepal_length", ascending=False).reset_index(drop=True)
    pd_df = pd_df.sort_values("sepal_length", ascending=False).reset_index(drop=True)
    assert df.equals(pd_df)


@pytest.mark.integration()
def test_sqllite_read_with_limit_pushdown(temp_sqllite_db) -> None:
    df = daft.read_sql("SELECT * FROM iris", f"sqlite://{temp_sqllite_db}")
    df = df.limit(100)

    pd_df = pd.read_sql("SELECT * FROM iris", f"sqlite:///{temp_sqllite_db}")
    pd_df = pd_df.head(100)

    df = df.to_pandas()
    pd_df = pd_df.reset_index(drop=True)
    assert df.equals(pd_df)


@pytest.mark.integration()
def test_sqllite_read_with_projection_pushdown(temp_sqllite_db) -> None:
    df = daft.read_sql("SELECT * FROM iris", f"sqlite://{temp_sqllite_db}")
    df = df.select(df["sepal_length"], df["variety"])

    pd_df = pd.read_sql("SELECT * FROM iris", f"sqlite:///{temp_sqllite_db}")
    pd_df = pd_df[["sepal_length", "variety"]]

    df = df.to_pandas()
    assert df.equals(pd_df)


@pytest.mark.integration()
def test_sqllite_read_with_all_pushdowns(temp_sqllite_db) -> None:
    df = daft.read_sql("SELECT * FROM iris", f"sqlite://{temp_sqllite_db}")
    df = df.where(df["sepal_length"] > 5.0)
    df = df.where(df["sepal_width"] > 3.0)
    df = df.limit(100)
    df = df.select(df["sepal_length"])

    pd_df = pd.read_sql("SELECT * FROM iris", f"sqlite:///{temp_sqllite_db}")
    pd_df = pd_df[pd_df["sepal_length"] > 5.0]
    pd_df = pd_df[pd_df["sepal_width"] > 3.0]
    pd_df = pd_df.head(100)
    pd_df = pd_df[["sepal_length"]]

    df = df.to_pandas().sort_values("sepal_length", ascending=False).reset_index(drop=True)
    pd_df = pd_df.sort_values("sepal_length", ascending=False).reset_index(drop=True)
    assert df.equals(pd_df)


@pytest.mark.integration()
def test_sqllite_bad_url() -> None:
    with pytest.raises(RuntimeError, match="Failed to execute sql"):
        daft.read_sql("SELECT * FROM iris", "sqlite://")
