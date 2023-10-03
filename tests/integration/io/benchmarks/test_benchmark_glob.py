from __future__ import annotations

import itertools

import pytest
import s3fs

from daft.daft import io_glob

from ..conftest import minio_create_bucket

NUM_FILES = 10000
NUM_LEVELS = 4
FANOUT_PER_LEVEL = 12
BUCKET = "bucket"


def generate_one_file_per_dir():
    # Total of 10k files (10^4 * 1)
    NUM_LEVELS = 4
    FANOUT_PER_LEVEL = 10
    return [
        "/".join(f"part_col_{i}={val}" for i, val in enumerate(part_vals)) + f"/0.parquet"
        for part_vals in itertools.product([str(i) for i in range(FANOUT_PER_LEVEL)], repeat=NUM_LEVELS)
    ]


def generate_balanced_partitioned_data():
    # Total of 10k files (10^3 * 10)
    NUM_LEVELS = 3
    FANOUT_PER_LEVEL = 10
    FILES_PER_PARTITION = 10
    return [
        "/".join(f"part_col_{i}={val}" for i, val in enumerate(part_vals)) + f"/{i}.parquet"
        for i in range(FILES_PER_PARTITION)
        for part_vals in itertools.product([str(i) for i in range(FANOUT_PER_LEVEL)], repeat=NUM_LEVELS)
    ]


def generate_left_skew_partitioned_data():
    # Total of 10k files (10^3 * 10)
    NUM_LEVELS = 3
    FANOUT_PER_LEVEL = 10

    # First partition contains 1009 files, and the other partitions have 9 files each
    num_files_per_partition = [1009] + [9 for i in range(999)]

    return [
        "/".join(f"part_col_{i}={val}" for i, val in enumerate(part_vals)) + f"/{i}.parquet"
        for part_vals, num_files_for_this_partition in zip(
            itertools.product([str(i) for i in range(FANOUT_PER_LEVEL)], repeat=NUM_LEVELS), num_files_per_partition
        )
        for i in range(num_files_for_this_partition)
    ]


def generate_right_skew_partitioned_data():
    # Total of 10k files (10^3 * 10)
    NUM_LEVELS = 3
    FANOUT_PER_LEVEL = 10

    # Last partition contains 1009 files, and the other partitions have 9 files each
    num_files_per_partition = [9 for i in range(999)] + [1009]

    return [
        "/".join(f"part_col_{i}={val}" for i, val in enumerate(part_vals)) + f"/{i}.parquet"
        for part_vals, num_files_for_this_partition in zip(
            itertools.product([str(i) for i in range(FANOUT_PER_LEVEL)], repeat=NUM_LEVELS), num_files_per_partition
        )
        for i in range(num_files_for_this_partition)
    ]


def generate_left_skew_dirs_partitioned_data():
    # First partition in level 0 fans out (1 * 20 * 20 * 10 files = 4000 files)
    # The rest of the partitions in level 0 fan out much smaller (1 * 8 * 8 * 10 = 10 files) * 9 = 5940 files
    first_partition_paths = [
        "/".join(f"part_col_{i}={val}" for i, val in enumerate([0, *part_vals])) + f"/{i}.parquet"
        for i in range(10)
        for part_vals in itertools.product([str(i) for i in range(20)], repeat=2)
    ]
    other_partition_paths = [
        "/".join(f"part_col_{i}={val}" for i, val in enumerate([first_level_val, *part_vals])) + f"/{i}.parquet"
        for first_level_val in range(1, 10)
        for part_vals in itertools.product([str(i) for i in range(8)], repeat=2)
        for i in range(10)
    ]

    return first_partition_paths + other_partition_paths


def generate_right_skew_dirs_partitioned_data():
    # Last partition in level 0 fans out (1 * 20 * 20 * 10 files = 4000 files)
    # The rest of the partitions in level 0 fan out much smaller (1 * 8 * 8 * 10 = 10 files) * 9 = 5940 files
    last_partition_paths = [
        "/".join(f"part_col_{i}={val}" for i, val in enumerate([9, *part_vals])) + f"/{i}.parquet"
        for i in range(10)
        for part_vals in itertools.product([str(i) for i in range(20)], repeat=2)
    ]
    other_partition_paths = [
        "/".join(f"part_col_{i}={val}" for i, val in enumerate([first_level_val, *part_vals])) + f"/{i}.parquet"
        for first_level_val in range(0, 9)
        for part_vals in itertools.product([str(i) for i in range(8)], repeat=2)
        for i in range(10)
    ]

    return other_partition_paths + last_partition_paths


def generate_bushy_late_partitioned_data():
    # Total of 10k files (10^3 * 10)
    return [f"single/single/part_col={val}" + f"/{i}.parquet" for i in range(10) for val in range(1000)]


def generate_bushy_early_partitioned_data():
    # Total of 10k files (10^3 * 10)
    return [f"part_col={val}/single/single" + f"/{i}.parquet" for i in range(10) for val in range(1000)]


FILE_NAME_GENERATORS = {
    "one-file-per-dir": generate_one_file_per_dir,
    "partitioned-data-balanced": generate_balanced_partitioned_data,
    "partitioned-data-left-skew-files": generate_left_skew_partitioned_data,
    "partitioned-data-right-skew-files": generate_right_skew_partitioned_data,
    "partitioned-data-left-skew-dirs": generate_left_skew_dirs_partitioned_data,
    "partitioned-data-right-skew-dirs": generate_right_skew_dirs_partitioned_data,
    "partitioned-data-bushy-early": generate_bushy_early_partitioned_data,
    "partitioned-data-bushy-late": generate_bushy_late_partitioned_data,
}


@pytest.fixture(
    scope="module",
    params=[
        "one-file-per-dir",
        "partitioned-data-balanced",
        "partitioned-data-left-skew-files",
        "partitioned-data-right-skew-files",
        "partitioned-data-right-skew-dirs",
        "partitioned-data-left-skew-dirs",
        "partitioned-data-bushy-early",
        "partitioned-data-bushy-late",
    ],
)
def setup_bucket(request, minio_io_config):
    test_name = request.param
    file_name_generator = FILE_NAME_GENERATORS[test_name]
    with minio_create_bucket(minio_io_config, bucket_name=BUCKET) as fs:
        files = file_name_generator()
        print(f"Num files: {len(files)}")
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
    results = benchmark(
        lambda: fs.glob(
            f"s3://{BUCKET}/**/*.parquet",
            # Can't set page size for s3fs
            # max_items=PAGE_SIZE,
        )
    )
    assert len(results) == setup_bucket


@pytest.mark.benchmark(group="glob")
@pytest.mark.integration()
@pytest.mark.parametrize("page_size", [100, 1000])
def test_benchmark_glob_boto3_list(benchmark, setup_bucket, minio_io_config, page_size):
    import boto3

    s3 = boto3.client(
        "s3",
        aws_access_key_id=minio_io_config.s3.key_id,
        aws_secret_access_key=minio_io_config.s3.access_key,
        endpoint_url=minio_io_config.s3.endpoint_url,
    )

    def f():
        continuation_token = None
        has_next = True
        opts = {"MaxKeys": page_size}
        data = []
        while has_next:
            if continuation_token is not None:
                opts["ContinuationToken"] = continuation_token

            response = s3.list_objects_v2(
                Bucket=BUCKET,
                Prefix="",
                **opts,
            )
            data.extend([{"path": f"s3://{BUCKET}/{d['Key']}", "type": "File"} for d in response.get("Contents", [])])
            continuation_token = response.get("NextContinuationToken")
            has_next = continuation_token is not None

        return data

    data = benchmark(f)
    assert len(data) == setup_bucket


@pytest.mark.benchmark(group="glob")
@pytest.mark.integration()
@pytest.mark.parametrize("fanout_limit", [128, 256])
@pytest.mark.parametrize("page_size", [100, 1000])
def test_benchmark_glob_daft(benchmark, setup_bucket, minio_io_config, fanout_limit, page_size):
    results = benchmark(
        lambda: io_glob(
            f"s3://{BUCKET}/**/*.parquet", io_config=minio_io_config, fanout_limit=fanout_limit, page_size=page_size
        )
    )
    assert len(results) == setup_bucket
