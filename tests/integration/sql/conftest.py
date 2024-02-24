from __future__ import annotations

import random

import numpy as np
import pytest
import sqlalchemy
import tenacity

TRINO_URL = "trino://user@localhost:8080/tpch"

NUM_TEST_ROWS = 200


@pytest.fixture(scope="session")
def test_items():
    np.random.seed(42)
    data = {
        "sepal_length": np.round(np.random.uniform(4.3, 7.9, NUM_TEST_ROWS), 1),
        "sepal_width": np.round(np.random.uniform(2.0, 4.4, NUM_TEST_ROWS), 1),
        "petal_length": np.round(np.random.uniform(1.0, 6.9, NUM_TEST_ROWS), 1),
        "petal_width": np.round(np.random.uniform(0.1, 2.5, NUM_TEST_ROWS), 1),
        "variety": [random.choice(["Setosa", "Versicolor", "Virginica"]) for _ in range(NUM_TEST_ROWS)],
    }
    return data


@tenacity.retry(
    stop=tenacity.stop_after_delay(60),
    wait=tenacity.wait_fixed(5),
    reraise=True,
)
def check_database_connection(url) -> None:
    with sqlalchemy.create_engine(url).connect() as conn:
        conn.execute("SELECT 1")


@pytest.fixture(scope="session")
@pytest.mark.parametrize("url", [TRINO_URL])
def check_db_server_initialized(url) -> bool:
    try:
        check_database_connection(url)
        return True
    except Exception as e:
        pytest.fail(f"Failed to connect to {url}: {e}")
