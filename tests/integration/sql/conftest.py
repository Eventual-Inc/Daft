from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Generator

import numpy as np
import pandas as pd
import pytest
import tenacity
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
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
]
TEST_TABLE_NAME = "example"


@pytest.fixture(scope="session", params=[{"num_rows": 200}])
def generated_data(request: pytest.FixtureRequest) -> pd.DataFrame:
    num_rows = request.param["num_rows"]

    data = {
        "id": np.arange(num_rows),
        "float_col": np.arange(num_rows, dtype=float),
        "string_col": [f"row_{i}" for i in range(num_rows)],
        "bool_col": [True for _ in range(num_rows // 2)] + [False for _ in range(num_rows // 2)],
        "date_col": [date(2021, 1, 1) + timedelta(days=i) for i in range(num_rows)],
        "date_time_col": [datetime(2020, 1, 1, 10, 0, 0) + timedelta(hours=i) for i in range(num_rows)],
        "time_col": [
            (datetime.combine(datetime.today(), time(0, 0)) + timedelta(minutes=x)).time() for x in range(200)
        ],
        "null_col": [None if i % 2 == 1 else "not_null" for i in range(num_rows)],
        "non_uniformly_distributed_col": [1 for _ in range(num_rows)],
    }
    return pd.DataFrame(data)


@pytest.fixture(scope="session", params=URLS)
def test_db(request: pytest.FixtureRequest, generated_data: pd.DataFrame) -> Generator[str, None, None]:
    db_url = request.param
    setup_database(db_url, generated_data)
    yield db_url


@tenacity.retry(stop=tenacity.stop_after_delay(10), wait=tenacity.wait_fixed(5), reraise=True)
def setup_database(db_url: str, data: pd.DataFrame) -> None:
    engine = create_engine(db_url)
    create_and_populate(engine, data)

    # Ensure the table is created and populated
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {TEST_TABLE_NAME}")).fetchone()[0]
        assert result == len(data)


def create_and_populate(engine: Engine, data: pd.DataFrame) -> None:
    metadata = MetaData()
    table = Table(
        TEST_TABLE_NAME,
        metadata,
        Column("id", Integer),
        Column("float_col", Float),
        Column("string_col", String(50)),
        Column("bool_col", Boolean),
        Column("date_col", Date),
        Column("date_time_col", DateTime),
        Column("null_col", String(50)),
        Column("non_uniformly_distributed_col", Integer),
    )
    metadata.create_all(engine)
    data.to_sql(table.name, con=engine, if_exists="replace", index=False)
