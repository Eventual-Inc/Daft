# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "ray[default]",
#   "daft",
# ]
# ///
from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Callable

from ray.job_submission import JobStatus, JobSubmissionClient

import daft


async def print_logs(logs):
    async for lines in logs:
        print(lines, end="")


async def wait_on_job(logs, timeout_s):
    await asyncio.wait_for(print_logs(logs), timeout=timeout_s)


def run_on_ray(ray_address: str, job_params: dict, timeout_s: int = 1500):
    """Submits a job to run in the Ray cluster."""
    print("Submitting benchmarking job to Ray cluster...")
    print("Parameters:")
    print(job_params)

    client = JobSubmissionClient(address=ray_address)
    job_id = client.submit_job(**job_params)
    print(f"Submitted job: {job_id}")

    try:
        asyncio.run(wait_on_job(client.tail_job_logs(job_id), timeout_s))
    except asyncio.TimeoutError:
        print(f"Job timed out after {timeout_s}s! Stopping job now...")
        client.stop_job(job_id)
        time.sleep(16)

    status = client.get_job_status(job_id)
    assert status.is_terminal(), "Job should have terminated"
    if status != JobStatus.SUCCEEDED:
        job_info = client.get_job_info(job_id)
        raise RuntimeError(f"Job failed with {job_info.error_type} error: {job_info.message}")
    print(f"Job completed with {status}")


def ray_job_params(
    parquet_folder_path: str,
    tpch_qnum: int,
    working_dir: pathlib.Path,
    entrypoint: pathlib.Path,
    runtime_env: dict,
) -> dict:
    return dict(
        submission_id=f"tpch-q{tpch_qnum}-{str(uuid.uuid4())[:4]}",
        entrypoint=f"python3 {entrypoint.relative_to(working_dir)!s} --parquet-folder {parquet_folder_path} --question-number {tpch_qnum}",
        runtime_env={
            "working_dir": str(working_dir),
            **runtime_env,
        },
    )


def get_df_with_parquet_folder(parquet_folder: str) -> Callable[[str], daft.DataFrame]:
    def _get_df(table_name: str) -> daft.DataFrame:
        return daft.read_parquet(os.path.join(parquet_folder, table_name, "*.parquet"))

    return _get_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet-folder", help="Path to TPC-H data stored on workers", required=True)
    parser.add_argument("--question-number", help="Question number to run", required=True)
    args = parser.parse_args()

    import answers

    get_df = get_df_with_parquet_folder(args.parquet_folder)
    answer = getattr(answers, f"q{args.question_number}")
    daft_df = answer(get_df)

    info_path = Path("/tmp") / "ray" / "session_latest" / "logs" / "info"
    info_path.mkdir(parents=True, exist_ok=True)

    explain_delta = None
    with open(info_path / f"plan-{args.question_number}.txt", "w") as f:
        explain_start = datetime.now()
        daft_df.explain(show_all=True, file=f, format="mermaid")
        explain_end = datetime.now()
        explain_delta = explain_end - explain_start

    execute_delta = None
    execute_start = datetime.now()
    daft_df.collect()
    execute_end = datetime.now()
    execute_delta = execute_end - execute_start

    with open(info_path / f"stats-{args.question_number}.txt", "w") as f:
        stats = json.dumps(
            {
                "question": args.question_number,
                "planning-time": str(explain_delta),
                "execution-time": str(execute_delta),
            }
        )
        f.write(stats)
