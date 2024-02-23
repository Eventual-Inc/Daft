from __future__ import annotations

import pytest
import sqlalchemy
import tenacity

TRINO_URL = "trino://user@localhost:8080/tpch"


@tenacity.retry(
    stop=tenacity.stop_after_delay(60),
    wait=tenacity.wait_fixed(5),
    reraise=True,
)
@pytest.fixture(scope="session")
def check_db_server_initialized() -> None:
    try:
        with sqlalchemy.create_engine(TRINO_URL).connect() as conn:
            conn.execute(sqlalchemy.text("SELECT 1"))
    except Exception as e:
        print(f"Connection failed with exception: {e}")
        raise
