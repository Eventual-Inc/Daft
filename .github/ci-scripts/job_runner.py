# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///

import argparse
import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from ray.job_submission import JobStatus, JobSubmissionClient


def parse_env_var_str(env_var_str: str) -> dict:
    iter = map(
        lambda s: s.strip().split("="),
        filter(lambda s: s, env_var_str.split(",")),
    )
    return {k: v for k, v in iter}


async def print_logs(logs):
    async for lines in logs:
        print(lines, end="")


async def wait_on_job(logs, timeout_s):
    await asyncio.wait_for(print_logs(logs), timeout=timeout_s)


@dataclass
class Result:
    query: int
    duration: timedelta
    error_msg: Optional[str]


def submit_job(
    working_dir: Path,
    entrypoint_script: str,
    entrypoint_args: str,
    env_vars: str,
    enable_ray_tracing: bool,
):
    env_vars_dict = parse_env_var_str(env_vars)
    if enable_ray_tracing:
        env_vars_dict["DAFT_ENABLE_RAY_TRACING"] = "1"

    client = JobSubmissionClient(address="http://localhost:8265")

    if entrypoint_args.startswith("[") and entrypoint_args.endswith("]"):
        # this is a json-encoded list of strings; parse accordingly
        list_of_entrypoint_args: list[str] = json.loads(entrypoint_args)
    else:
        list_of_entrypoint_args: list[str] = [entrypoint_args]

    results = []

    for index, args in enumerate(list_of_entrypoint_args):
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

        asyncio.run(wait_on_job(client.tail_job_logs(job_id), timeout_s=60 * 30))

        status = client.get_job_status(job_id)
        assert status.is_terminal(), "Job should have terminated"
        end = datetime.now()
        duration = end - start
        error_msg = None
        if status != JobStatus.SUCCEEDED:
            job_info = client.get_job_info(job_id)
            error_msg = job_info.message

        result = Result(query=index, duration=duration, error_msg=error_msg)
        results.append(result)

    print(f"{results=}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--working-dir", type=Path, required=True)
    parser.add_argument("--entrypoint-script", type=str, required=True)
    parser.add_argument("--entrypoint-args", type=str, required=True)
    parser.add_argument("--env-vars", type=str, required=True)
    parser.add_argument("--enable-ray-tracing", action="store_true")

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
        enable_ray_tracing=args.enable_ray_tracing,
    )
