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
from pathlib import Path
from typing import Optional

import boto3
import gha_run_cluster_job
import inquirer
from github import Auth, Github, enable_console_debug_logging
from github.Workflow import Workflow
from github.WorkflowJob import WorkflowJob
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


def find_wheel(commit_hash: str) -> Optional[str]:
    s3 = boto3.client("s3")
    response: dict = s3.list_objects_v2(Bucket="github-actions-artifacts-bucket", Prefix=f"builds/{commit_hash}/")
    wheel_urls = []
    for wheel in response.get("Contents", []):
        wheel: dict
        if "Key" in wheel:
            wheel_path = Path(wheel["Key"])
            wheel_name = wheel_path.name
            if wheel_name == WHEEL_NAME:
                wheel_urls.append(
                    f"https://github-actions-artifacts-bucket.s3.us-west-2.amazonaws.com/builds/{commit_hash}/{wheel_name}"
                )

    length = len(wheel_urls)
    assert length <= 1, "There should never be more than 1 object in S3 with the exact same key"

    return wheel_urls[0] if wheel_urls else None


def build(branch_name: str, commit_hash: str) -> tuple[str, str]:
    """Runs a build on the given branch.

    If the branch has already been built, it will reuse the already built wheel.
    """
    print(f"Checking if a build exists for the branch '{branch_name}' (commit-hash: {commit_hash})")

    wheel_url = find_wheel(commit_hash)

    if wheel_url:
        print(f"Wheel already found at url {wheel_url}; re-using")
        return wheel_url, commit_hash

    print(f"No wheel found for branch '{branch_name}'; attempting to build")
    user_wants_to_build_commit = inquirer.confirm(
        message=f"You are requesting to build '{branch_name}' (commit-hash: {commit_hash}) using the 'build-commit' workflow; proceed?"
    )
    if not user_wants_to_build_commit:
        print("Workflow aborted")
        exit(1)

    workflow = repo.get_workflow("build-commit.yaml")
    latest_run = dispatch(workflow=workflow, branch_name=branch_name, inputs={"arch": "x86"})

    actual_commit_hash = latest_run.head_sha
    if actual_commit_hash != commit_hash:
        print(
            f"Looks like your current branch and remote branch are out of sync (one is behind the other);\nThe workflow has been launched on the commit hash that GitHub has: '{actual_commit_hash}'",
        )
        commit_hash = actual_commit_hash

    while True:
        jobs = repo.get_workflow_run(latest_run.id).jobs()
        if not jobs:
            raise RuntimeError("The 'build-commit' workflow should have 1 job")
        elif jobs.totalCount > 1:
            raise RuntimeError("The 'build-commit' workflow should only have 1 job")

        build_commit_job: WorkflowJob = jobs[0]
        if build_commit_job.conclusion:
            break

        print(f"Job is still running with status: {build_commit_job.status}")
        sleep_and_then_retry(10)

    print(f"Job completed with status {build_commit_job.conclusion}")

    if build_commit_job.conclusion != "success":
        raise RuntimeError(f"The 'build-commit' workflow failed; view the results here: {latest_run.url}")

    wheel_url = find_wheel(commit_hash)

    if not wheel_url:
        raise RuntimeError("The wheel was not able to be found in AWS S3; internal error")

    return wheel_url, commit_hash


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
            nums.push(str(num))
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
    wheel_url: str,
    branch_name: str,
    commit_hash: str,
    questions: str,
    scale_factor: int,
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
            "daft_wheel_url": wheel_url,
            "working_dir": "benchmarking/tpcds",
            "entrypoint_script": "ray_entrypoint.py",
            "entrypoint_args": entrypoint_args,
        },
    )


def main(
    branch_name: Optional[str],
    questions: str,
    scale_factor: int,
):
    branch_name, commit_hash = get_name_and_commit_hash(branch_name)
    wheel_url, commit_hash = build(branch_name=branch_name, commit_hash=commit_hash)
    run(
        wheel_url=wheel_url,
        branch_name=branch_name,
        commit_hash=commit_hash,
        questions=questions,
        scale_factor=scale_factor,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ref", type=str, required=False, help="The branch name to run on")
    parser.add_argument(
        "--questions", type=str, required=False, default="*", help="A comma separated list of questions to run"
    )
    parser.add_argument("--scale-factor", type=int, required=False, default=2, help="The scale factor to run on")
    parser.add_argument("--verbose", action="store_true", help="Verbose debugging")
    args = parser.parse_args()

    if args.verbose:
        enable_console_debug_logging()

    main(
        branch_name=args.ref,
        questions=args.questions,
        scale_factor=args.scale_factor,
    )
