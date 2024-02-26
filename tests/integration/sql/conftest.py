from __future__ import annotations

import tempfile
from typing import Generator

import numpy as np
import pandas as pd
import pytest
import tenacity
from sqlalchemy import (
    Column,
    Engine,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    text,
)

URLS = [
    "trino://user@localhost:8080/memory/default",
    "postgresql://username:password@localhost:5432/postgres",
    "mysql+pymysql://username:password@localhost:3306/mysql",
    "sqlite:///",
]
NUM_TEST_ROWS = 200
NUM_ROWS_PER_PARTITION = 50
TEST_TABLE_NAME = "example"


@pytest.fixture(scope="session")
def generated_data() -> pd.DataFrame:
    data = {
        "id": np.arange(NUM_TEST_ROWS),
        "sepal_length": np.arange(NUM_TEST_ROWS, dtype=float),
        "sepal_width": np.arange(NUM_TEST_ROWS, dtype=float),
        "petal_length": np.arange(NUM_TEST_ROWS, dtype=float),
        "petal_width": np.arange(NUM_TEST_ROWS, dtype=float),
        "variety": ["setosa"] * 50 + ["versicolor"] * 50 + ["virginica"] * 50 + [None] * 50,
    }
    return pd.DataFrame(data)


@pytest.fixture(scope="session", params=URLS)
def test_db(request: pytest.FixtureRequest, generated_data: pd.DataFrame) -> Generator[str, None, None]:
    db_url = request.param
    if db_url.startswith("sqlite"):
        with tempfile.NamedTemporaryFile(suffix=".db") as file:
            db_url += file.name
            setup_database(db_url, generated_data)
            yield db_url
    else:
        setup_database(db_url, generated_data)
        yield db_url


@tenacity.retry(stop=tenacity.stop_after_delay(10), wait=tenacity.wait_fixed(5), reraise=True)
def setup_database(db_url: str, data: pd.DataFrame) -> None:
    engine = create_engine(db_url)
    create_and_populate(engine, data)

    # Ensure the table is created and populated
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {TEST_TABLE_NAME}")).fetchone()[0]
        assert result == NUM_TEST_ROWS


def create_and_populate(engine: Engine, data: pd.DataFrame) -> None:
    metadata = MetaData()
    table = Table(
        TEST_TABLE_NAME,
        metadata,
        Column("id", Integer),
        Column("sepal_length", Float),
        Column("sepal_width", Float),
        Column("petal_length", Float),
        Column("petal_width", Float),
        Column("variety", String(50)),
    )
    metadata.create_all(engine)
    data.to_sql(table.name, con=engine, if_exists="replace", index=False)
