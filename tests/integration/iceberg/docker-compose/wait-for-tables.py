from __future__ import annotations

import subprocess
import time

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from requests.exceptions import ConnectionError, Timeout

TABLES = [
    "test_all_types",
    "test_limit",
    "test_null_nan",
    "test_null_nan_rewritten",
    "test_partitioned_by_bucket",
    "test_partitioned_by_days",
    "test_partitioned_by_hours",
    "test_partitioned_by_identity",
    "test_partitioned_by_months",
    "test_partitioned_by_truncate",
    "test_partitioned_by_years",
    "test_positional_mor_deletes",
    "test_positional_mor_double_deletes",
    "test_table_sanitized_character",
    "test_table_version",
    "test_uuid_and_fixed_unpartitioned",
]

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
    subprocess.run(["docker", "logs", "pyiceberg-spark"], check=False)
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            "tests/integration/iceberg/docker-compose/docker-compose.yml",
            "ps",
        ],
        check=False,
    )
    raise RuntimeError("Timed out waiting for Iceberg test tables") from last_error
