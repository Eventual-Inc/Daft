#!/usr/bin/env python3
"""Run a single AI benchmark directly using Ray job submission.

This script runs a specific AI benchmark using Daft on a Ray cluster,
similar to how the TPC-H benchmark works.
"""

from __future__ import annotations

import asyncio
import sys
import time

from ray.job_submission import JobStatus, JobSubmissionClient

import daft
from tools.ci_bench_utils import get_run_metadata, tail_logs, upload_to_google_sheets


def run_benchmark(benchmark_name: str):
    """Run a single AI benchmark and return the execution time."""
    print(f"Running {benchmark_name} benchmark... ", end="", flush=True)

    client = JobSubmissionClient(address="http://localhost:8265")

    start: float = time.perf_counter()

    submission_id = client.submit_job(
        entrypoint="DAFT_RUNNER=ray DAFT_PROGRESS_BAR=0 python daft_main.py",
        runtime_env={"working_dir": f"./benchmarking/ai/{benchmark_name}"},
    )

    job_details = asyncio.run(tail_logs(client, submission_id))

    end = time.perf_counter()

    if job_details.status != JobStatus.SUCCEEDED:
        print(f"\nRay job did not succeed, received job status: {job_details.status}\nJob details: {job_details}")
        sys.exit(1)

    execution_time = end - start
    print(f"done in {execution_time:.2f}s")
    print(f"Job details: {job_details}")

    return execution_time


def main():
    """Main function to run a single AI benchmark."""
    if len(sys.argv) != 2:
        print("Usage: python run_ai_benchmark_direct.py <benchmark_name>")
        print(
            "Available benchmarks: audio_transcription, document_embedding, image_classification, video_object_detection"
        )
        sys.exit(1)

    benchmark_name = sys.argv[1]
    valid_benchmarks = ["audio_transcription", "document_embedding", "image_classification", "video_object_detection"]

    if benchmark_name not in valid_benchmarks:
        print(f"Invalid benchmark name: {benchmark_name}")
        print(f"Available benchmarks: {', '.join(valid_benchmarks)}")
        sys.exit(1)

    daft.set_runner_native()

    metadata = get_run_metadata()

    print(f"Starting {benchmark_name} benchmark...")
    print(f"Daft version: {metadata['daft version']}")

    # Warmup run
    print("\nPerforming warmup run...")
    run_benchmark(benchmark_name)
    print("Warmup completed.\n")

    # Run benchmark multiple times and take average
    num_runs = 2
    print(f"Running benchmark {num_runs} times...")
    execution_times = []
    for i in range(num_runs):
        print(f"\nRun {i + 1}/{num_runs}:")
        execution_time = run_benchmark(benchmark_name)
        execution_times.append(execution_time)

    avg_execution_time = sum(execution_times) / len(execution_times)
    print(f"\nExecution times: {[f'{t:.2f}s' for t in execution_times]}")
    print(f"Average execution time: {avg_execution_time:.2f}s")

    # Prepare data for upload
    data_dict = {**metadata, "benchmark_type": benchmark_name, benchmark_name: avg_execution_time}

    print(f"\n{benchmark_name} benchmark completed in {avg_execution_time:.2f}s (average of {num_runs} runs)")
    print(f"Results: {data_dict}")

    # Upload results to Google Sheets
    upload_to_google_sheets(benchmark_name, list(data_dict.values()))
    print("Results uploaded to Google Sheets")

    print(f"{benchmark_name} benchmark completed successfully!")


if __name__ == "__main__":
    main()
