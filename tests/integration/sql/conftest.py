from __future__ import annotations

import pytest
import sqlalchemy
import tenacity

TRINO_URL = "trino://user@localhost:8080/tpch"


@tenacity.retry(
    stop=tenacity.stop_after_delay(60),
    retry=tenacity.retry_if_exception_type(sqlalchemy.exc.DBAPIError),
    wait=tenacity.wait_fixed(5),
    reraise=True,
)
@pytest.fixture(scope="session")
def check_db_server_initialized() -> None:
    with sqlalchemy.create_engine(TRINO_URL).connect() as conn:
        conn.execute(sqlalchemy.text("SELECT 1"))
