from __future__ import annotations

import random

import pytest
import s3fs

from daft.daft import io_glob

from ..conftest import minio_create_bucket

NUM_FILES = 10000
NUM_LEVELS = 8
FANOUT_PER_LEVEL = 24
BUCKET = "bucket"


def _generate_file_name(num_levels, fanout_per_level):
    level = random.randint(0, num_levels)
    return (
        "/".join([f"part_col_{l}={random.randint(0, fanout_per_level)}" for l in range(1, level + 1)]) + "/file.parquet"
    )


@pytest.fixture(scope="module")
def setup_bucket(minio_io_config):
    with minio_create_bucket(minio_io_config, bucket_name=BUCKET) as fs:
        files = [_generate_file_name(NUM_LEVELS, FANOUT_PER_LEVEL) for i in range(NUM_FILES)]
        for name in files:
            fs.touch(f"{BUCKET}/{name}")
        yield len(set(files))


@pytest.mark.benchmark(group="glob")
@pytest.mark.integration()
def test_benchmark_glob_s3fs(benchmark, setup_bucket, minio_io_config):
    fs = s3fs.S3FileSystem(
        key=minio_io_config.s3.key_id,
        password=minio_io_config.s3.access_key,
        client_kwargs={"endpoint_url": minio_io_config.s3.endpoint_url},
    )
    results = benchmark(lambda: fs.glob(f"s3://{BUCKET}/**/*.parquet"))
    assert len(results) == setup_bucket


@pytest.mark.benchmark(group="glob")
@pytest.mark.integration()
@pytest.mark.parametrize("fanout_limit", [8, 64, 512, 4096])
def test_benchmark_glob_daft(benchmark, setup_bucket, minio_io_config, fanout_limit):
    fs = s3fs.S3FileSystem(
        key=minio_io_config.s3.key_id,
        password=minio_io_config.s3.access_key,
        client_kwargs={"endpoint_url": minio_io_config.s3.endpoint_url},
    )
    results = benchmark(
        lambda: io_glob(f"s3://{BUCKET}/**/*.parquet", io_config=minio_io_config, fanout_limit=fanout_limit)
    )
    assert len(results) == setup_bucket
