"""Run TPC-H benchmarks with Ray runner on local Parquet data and upload results to Google sheets."""

from __future__ import annotations

import asyncio
import os
import sys
import time

from ray.job_submission import JobStatus, JobSubmissionClient

import daft
from tools.ci_bench_utils import get_run_metadata, tail_logs, upload_to_google_sheets

SF_TO_S3_PATH = {
    100: "s3://eventual-dev-benchmarking-fixtures/uncompressed/tpch-dbgen/100_0/32/parquet/",
    1000: "s3://eventual-dev-benchmarking-fixtures/uncompressed/tpch-dbgen/1000_0/512/parquet/",
}


def run_benchmark():
    results = {}

    scale_factor = int(os.getenv("TPCH_SCALE_FACTOR"))
    if scale_factor not in SF_TO_S3_PATH:
        raise ValueError(
            f"TPC-H scale factor {scale_factor} not supported, expected one of the following: {SF_TO_S3_PATH.keys()}"
        )
    parquet_path = SF_TO_S3_PATH[scale_factor]

    client = JobSubmissionClient(address="http://localhost:8265")

    for q in range(1, 23):
        print(f"Running TPC-H Q{q}... ", end="", flush=True)

        start: float = time.perf_counter()

        submission_id = client.submit_job(
            entrypoint=f"DAFT_RUNNER=ray DAFT_PROGRESS_BAR=0 python answers_sql.py {parquet_path} {q}",
            runtime_env={"working_dir": "./benchmarking/tpch"},
        )

        job_details = asyncio.run(tail_logs(client, submission_id))

        end = time.perf_counter()

        if job_details.status != JobStatus.SUCCEEDED:
            print(f"\nRay job did not succeed, received job status: {job_details.status}\nJob details: {job_details}")
            sys.exit(1)

        results[q] = end - start

        print(f"done in {results[q]:.2f}s")
        print(f"Job details: {job_details}")

    return results


def main():
    daft.set_runner_native()

    metadata = get_run_metadata()
    scale_factor = int(os.getenv("TPCH_SCALE_FACTOR"))
    num_workers = int(os.getenv("RAY_NUM_WORKERS"))

    print("Starting warmup run...")
    run_benchmark()

    print("Warmup done. Running benchmark and collecting results...")
    results = run_benchmark()

    data_dict = {**metadata, "scale factor": scale_factor, "num workers": num_workers, **results}

    print("Results:")
    print(data_dict)

    upload_to_google_sheets("Distributed TPC-H", list(data_dict.values()))


if __name__ == "__main__":
    main()
