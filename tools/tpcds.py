# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "PyGithub",
#   "boto3",
#   "inquirer",
# ]
# ///

import argparse
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


auth = Auth.Token(gha_run_cluster_job.get_oauth_token())
g = Github(auth=auth)


def build(branch_name: Optional[str]):
    """Runs a build on the given branch.

    If the branch has already been built, it will reuse the already built wheel.
    """
    s3 = boto3.client("s3")

    branch_name, commit_hash = get_name_and_commit_hash(branch_name)
    response: dict = s3.list_objects_v2(Bucket="github-actions-artifacts-bucket", Prefix=f"builds/{commit_hash}/")
    wheel_urls = []
    for wheel in response.get("Contents", []):
        wheel: dict
        if "Key" in wheel:
            wheel_path = Path(wheel["Key"])
            wheel_name = wheel_path.name
            print(wheel_name)
            if wheel_name == WHEEL_NAME:
                wheel_urls.append(
                    f"https://github-actions-artifacts-bucket.s3.us-west-2.amazonaws.com/builds/{commit_hash}/{wheel_name}"
                )

    length = len(wheel_urls)
    assert length <= 1, "There should never be more than 1 object in S3 with the exact same key"

    print(f"Checking if a build exists for the branch '{branch_name}' (commit-hash: {commit_hash})")

    if length == 0:
        user_wants_to_build_commit = inquirer.confirm(message="No build found; would you like to build this branch?")
        if not user_wants_to_build_commit:
            print("Workflow aborted")
            exit(1)

        repo = g.get_repo("Eventual-Inc/Daft")
        workflow = repo.get_workflow("build-commit.yaml")

        pre_creation_latest_run = get_latest_run(workflow)

        inputs = {"arch": "x86"}
        print(f"Launching new 'build-commit' workflow with the following inputs: {inputs}")
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

        print(f"Latest 'build-commit' workflow run found with id: {post_creation_latest_run.id}")
        print(f"View the workflow run at: {post_creation_latest_run.url}")

        while True:
            jobs = repo.get_workflow_run(post_creation_latest_run.id).jobs()
            if not jobs:
                raise RuntimeError("The 'build-commit' workflow should have 1 job")
            elif len(jobs) > 1:
                raise RuntimeError("The 'build-commit' workflow should only have 1 job")

            build_commit_job: WorkflowJob = jobs[0]
            if build_commit_job.conclusion:
                break
            else:
                print(f"Job is still running with status: {build_commit_job.status}")

            sleep_and_then_retry(10)

        print(f"Job completed with status {build_commit_job.conclusion}")

        if build_commit_job.conclusion != "success":
            raise RuntimeError(
                f"The 'build-commit' workflow failed; view the results here: {post_creation_latest_run.url}"
            )
    elif length == 1:
        print("Build found; re-using build")

    # repo = g.get_repo("Eventual-Inc/Daft")
    # workflow = repo.get_workflow("build-commit.yaml")
    #
    # created = workflow.create_dispatch(
    #     ref=branch,
    #     inputs={
    #         "arch": "x86",
    #     },
    # )
    #
    # if not created:
    #     raise RuntimeError("Could not create workflow, suggestion: run again with --verbose")
    #
    # print("Workflow created, view it at: https://github.com/Eventual-Inc/Daft/actions/workflows/run-cluster.yaml")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ref", type=str, required=False, help="The branch name to run on")
    parser.add_argument("--verbose", action="store_true", help="Verbose debugging")
    args = parser.parse_args()

    if args.verbose:
        enable_console_debug_logging()

    build(branch_name=args.ref)
