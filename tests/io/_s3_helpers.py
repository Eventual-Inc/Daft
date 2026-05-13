"""Shared helpers for routing the checkpoint test suites through real S3.

Importing this module activates the routing if ``CHECKPOINTING_TEST_BUCKET``
is set:

* ``AWS_REGION`` is ensured in env (deltalake-rs reads it for storage routing).
* Daft's default planning IO config is set to point at the bucket's region,
  so test-body calls that don't pass ``io_config=`` still talk to S3.

Without the env var, importing is a no-op — fixtures fall back to the local
filesystem (the default ``make test`` flow).

Only the iceberg + delta checkpoint test suites import this module, so the
side effects are scoped to those suites' pytest sessions.
"""

from __future__ import annotations

import os
import uuid

import daft

S3_BUCKET = os.environ.get("CHECKPOINTING_TEST_BUCKET")
S3_REGION = os.environ.get("AWS_REGION", "us-east-2")


def s3_uri(sink: str, *parts: str) -> str:
    """Build an ``s3://`` URI under a per-call uuid prefix.

    ``sink`` ("iceberg" or "delta") namespaces the prefix so the two
    suites don't collide. Each call gets a fresh prefix, so individual
    test invocations stay isolated even when reusing the same bucket.
    """
    run = f"checkpointing-suite/{sink}/{uuid.uuid4().hex[:8]}"
    return f"s3://{S3_BUCKET}/{run}/" + "/".join(parts)


def s3_io_config() -> daft.io.IOConfig:
    return daft.io.IOConfig(s3=daft.io.S3Config(region_name=S3_REGION))


def delta_storage_options() -> dict[str, str]:
    return {"AWS_REGION": S3_REGION}


if S3_BUCKET:
    # Ensure deltalake-rs picks up the region from env in test-body calls
    # that don't pass explicit ``storage_options=``.
    os.environ.setdefault("AWS_REGION", S3_REGION)
    # Make daft's default IO config S3-aware so test bodies that don't pass
    # ``io_config=`` still talk to S3 correctly.
    daft.set_planning_config(default_io_config=s3_io_config())
