from __future__ import annotations

import subprocess
import time

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from requests.exceptions import ConnectionError, Timeout

TABLES = [
    "test_uuid_and_fixed_unpartitioned",
    "test_null_nan",
    "test_null_nan_rewritten",
    "test_limit",
    "test_positional_mor_deletes",
    "test_positional_mor_double_deletes",
    "test_all_types",
    "test_partitioned_by_identity",
    "test_partitioned_by_years",
    "test_partitioned_by_months",
    "test_partitioned_by_days",
    "test_partitioned_by_hours",
    "test_partitioned_by_truncate",
    "test_partitioned_by_bucket",
    "test_table_version",
    "test_table_sanitized_character",
]

COMPOSE_FILE = "tests/integration/iceberg/docker-compose/docker-compose.yml"


def dump_iceberg_diagnostics() -> None:
    print("Iceberg service status:")
    subprocess.run(["docker", "compose", "-f", COMPOSE_FILE, "ps"], check=False)

    print("Iceberg service logs:")
    subprocess.run(["docker", "compose", "-f", COMPOSE_FILE, "logs", "--tail=300"], check=False)


deadline = time.time() + 300
last_error: Exception | None = None

while time.time() < deadline:
    try:
        catalog = load_catalog(
            "local",
            **{
                "type": "rest",
                "uri": "http://localhost:8181",
                "s3.endpoint": "http://localhost:9000",
                "s3.access-key-id": "admin",
                "s3.secret-access-key": "password",
            },
        )
        for table in TABLES:
            catalog.load_table(f"default.{table}")
        print("Iceberg test tables are ready")
        break
    except (ConnectionError, Timeout, NoSuchTableError) as exc:
        last_error = exc
        print(f"Waiting for Iceberg tables: {type(exc).__name__}: {exc}")
        time.sleep(5)
else:
    dump_iceberg_diagnostics()
    raise RuntimeError("Timed out waiting for Iceberg test tables") from last_error
