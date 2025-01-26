# /// script
# requires-python = ">=3.12"
# dependencies = ["ray[default]"]
# ///

import argparse
import asyncio
import csv
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from ray.job_submission import JobStatus, JobSubmissionClient

# We impose a 5min timeout here
# If any job does *not* finish in 5min, then we cancel it and mark the question as a "DNF" (did-not-finish).
TIMEOUT_S = 60 * 5


def parse_env_var_str(env_var_str: str) -> dict:
    iter = map(
        lambda s: s.strip().split("="),
        filter(lambda s: s, env_var_str.split(",")),
    )
    return {k: v for k, v in iter}


async def print_logs(logs):
    async for lines in logs:
        print(lines, end="")


async def wait_on_job(logs, timeout_s) -> bool:
    try:
        await asyncio.wait_for(print_logs(logs), timeout=timeout_s)
        return False
    except asyncio.exceptions.TimeoutError:
        return True


@dataclass
class Result:
    arguments: str
    duration: timedelta
    error_msg: Optional[str]


def submit_job(
    working_dir: Path,
    entrypoint_script: str,
    entrypoint_args: str,
    env_vars: str,
):
    if "GHA_OUTPUT_DIR" not in os.environ:
        raise RuntimeError("Output directory environment variable not found; don't know where to store outputs")
    output_dir = Path(os.environ["GHA_OUTPUT_DIR"])
    output_dir.mkdir(exist_ok=True, parents=True)

    env_vars_dict = parse_env_var_str(env_vars)

    client = JobSubmissionClient(address="http://localhost:8265")

    if entrypoint_args.startswith("[") and entrypoint_args.endswith("]"):
        # this is a json-encoded list of strings; parse accordingly
        list_of_entrypoint_args: list[str] = json.loads(entrypoint_args)
    else:
        list_of_entrypoint_args: list[str] = [entrypoint_args]

    results = []

    for args in list_of_entrypoint_args:
        entrypoint = f"DAFT_RUNNER=ray python {entrypoint_script} {args}"
        print(f"{entrypoint=}")
        start = datetime.now()
        job_id = client.submit_job(
            entrypoint=entrypoint,
            runtime_env={
                "working_dir": working_dir,
                "env_vars": env_vars_dict,
            },
        )

        timed_out = asyncio.run(wait_on_job(client.tail_job_logs(job_id), timeout_s=TIMEOUT_S))

        status = client.get_job_status(job_id)
        end = datetime.now()
        duration = end - start
        error_msg = None
        if status != JobStatus.SUCCEEDED:
            if timed_out:
                error_msg = f"Job exceeded {TIMEOUT_S} second(s)"
            else:
                job_info = client.get_job_info(job_id)
                error_msg = job_info.message

        result = Result(arguments=args, duration=duration, error_msg=error_msg)
        results.append(result)

    output_file = output_dir / "out.csv"
    with open(output_file, mode="w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=results[0].__dataclass_fields__.keys())
        writer.writeheader()
        for result in results:
            writer.writerow(asdict(result))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--working-dir", type=Path, required=True)
    parser.add_argument("--entrypoint-script", type=str, required=True)
    parser.add_argument("--entrypoint-args", type=str, required=True)
    parser.add_argument("--env-vars", type=str, required=True)

    args = parser.parse_args()

    if not (args.working_dir.exists() and args.working_dir.is_dir()):
        raise ValueError("The working-dir must exist and be a directory")

    entrypoint: Path = args.working_dir / args.entrypoint_script
    if not (entrypoint.exists() and entrypoint.is_file()):
        raise ValueError("The entrypoint script must exist and be a file")

    submit_job(
        working_dir=args.working_dir,
        entrypoint_script=args.entrypoint_script,
        entrypoint_args=args.entrypoint_args,
        env_vars=args.env_vars,
    )
