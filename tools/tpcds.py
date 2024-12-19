# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "PyGithub",
#   "boto3",
#   "inquirer",
# ]
# ///

import argparse
import json
import subprocess
import time
import typing
from typing import Optional

import gha_run_cluster_job
import inquirer
from github import Auth, Github, enable_console_debug_logging
from github.Workflow import Workflow
from github.WorkflowRun import WorkflowRun

WHEEL_NAME = "getdaft-0.3.0.dev0-cp38-abi3-manylinux_2_31_x86_64.whl"
RETRY_ATTEMPTS = 5

auth = Auth.Token(gha_run_cluster_job.get_oauth_token())
g = Github(auth=auth)
repo = g.get_repo("Eventual-Inc/Daft")


def dispatch(workflow: Workflow, branch_name: str, inputs: dict) -> WorkflowRun:
    pre_creation_latest_run = get_latest_run(workflow)

    print(f"Launching workflow '{workflow.name}' on the branch '{branch_name}' with the inputs '{inputs}'")
    created = workflow.create_dispatch(
        ref=branch_name,
        inputs=inputs,
    )
    if not created:
        raise RuntimeError("Could not create workflow, suggestion: run again with --verbose")

    post_creation_latest_run = None
    for _ in range(RETRY_ATTEMPTS):
        post_creation_latest_run = get_latest_run(workflow)
        if pre_creation_latest_run.run_number == post_creation_latest_run.run_number:
            sleep_and_then_retry()
        elif pre_creation_latest_run.run_number < post_creation_latest_run.run_number:
            break
        else:
            typing.assert_never(
                "Run numbers are always returned in sorted order and are always monotonically increasing"
            )
    if not post_creation_latest_run:
        raise RuntimeError("Unable to locate the new run request for the 'build-commit' workflow")

    print(f"Launched new '{workflow.name}' workflow with id: {post_creation_latest_run.id}")
    print(f"View the workflow run at: {post_creation_latest_run.html_url}")

    return post_creation_latest_run


def sleep_and_then_retry(sleep_amount_sec: int = 3):
    time.sleep(sleep_amount_sec)


def get_latest_run(workflow: Workflow) -> WorkflowRun:
    for _ in range(RETRY_ATTEMPTS):
        runs = workflow.get_runs()

        if runs.totalCount > 0:
            return runs[0]

        sleep_and_then_retry()

    raise RuntimeError("Unable to list all workflow invocations")


def get_name_and_commit_hash(branch_name: Optional[str]) -> tuple[str, str]:
    branch_name = branch_name or "HEAD"
    name = (
        subprocess.check_output(["git", "rev-parse", "--abbrev-ref", branch_name], stderr=subprocess.STDOUT)
        .strip()
        .decode("utf-8")
    )
    commit_hash = (
        subprocess.check_output(["git", "rev-parse", branch_name], stderr=subprocess.STDOUT).strip().decode("utf-8")
    )
    return name, commit_hash


def parse_questions(questions: str) -> list[int]:
    if not questions:
        return []

    if questions == "*":
        return list(range(1, 100))

    items = questions.split(",")
    nums = []
    for item in items:
        try:
            num = int(item)
            nums.append(str(num))
            continue
        except ValueError:
            ...

        if "-" not in item:
            raise ValueError("...")

        try:
            lower, upper = item.split("-")
            lower_int = int(lower)
            upper_int = int(upper)
            if lower_int > upper_int:
                raise ValueError
            nums.extend(range(lower_int, upper_int + 1))
        except ValueError:
            raise ValueError(f"Invalid question item; expected a number or a range, instead got {item}")

    return nums


def run(
    branch_name: str,
    commit_hash: str,
    questions: str,
    scale_factor: int,
    cluster_profile: str,
    env_vars: str,
):
    user_wants_to_run_tpcds_benchmarking = inquirer.confirm(
        message=f"Going to run the 'run-cluster' workflow on the branch '{branch_name}' (commit-hash: {commit_hash}); proceed?"
    )

    if not user_wants_to_run_tpcds_benchmarking:
        print("Workflow aborted")
        exit(1)

    expanded_questions = parse_questions(questions)
    args_as_list = [f"--question={q} --scale-factor={scale_factor}" for q in expanded_questions]
    entrypoint_args = json.dumps(args_as_list)

    workflow = repo.get_workflow("run-cluster.yaml")
    dispatch(
        workflow=workflow,
        branch_name=branch_name,
        inputs={
            "cluster_profile": cluster_profile,
            "working_dir": "benchmarking/tpcds",
            "entrypoint_script": "ray_entrypoint.py",
            "entrypoint_args": entrypoint_args,
            "env_vars": env_vars,
        },
    )


def main(
    branch_name: Optional[str],
    questions: str,
    scale_factor: int,
    cluster_profile: str,
    env_vars: str,
):
    branch_name, commit_hash = get_name_and_commit_hash(branch_name)
    run(
        branch_name=branch_name,
        commit_hash=commit_hash,
        questions=questions,
        scale_factor=scale_factor,
        cluster_profile=cluster_profile,
        env_vars=env_vars,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ref", type=str, required=False, help="The branch name to run on")
    parser.add_argument(
        "--questions", type=str, required=False, default="*", help="A comma separated list of questions to run"
    )
    parser.add_argument("--scale-factor", type=int, required=False, default=2, help="The scale factor to run on")
    parser.add_argument("--cluster-profile", type=str, required=False, help="The ray cluster configuration to run on")
    parser.add_argument(
        "--env-vars",
        type=str,
        required=False,
        help="A comma separated list of environment variables to pass to ray job",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose debugging")
    args = parser.parse_args()

    if args.verbose:
        enable_console_debug_logging()

    main(
        branch_name=args.ref,
        questions=args.questions,
        scale_factor=args.scale_factor,
        cluster_profile=args.cluster_profile,
        env_vars=args.env_vars,
    )
