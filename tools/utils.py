import subprocess
import time
import typing
from typing import Optional

import gha_run_cluster_job
from github import Auth, Github
from github.Workflow import Workflow
from github.WorkflowRun import WorkflowRun

RETRY_ATTEMPTS = 5

auth = Auth.Token(gha_run_cluster_job.get_oauth_token())
g = Github(auth=auth)
repo = g.get_repo("Eventual-Inc/Daft")


def dispatch(workflow: Workflow, branch_name: str, inputs: dict) -> WorkflowRun:
    pre_creation_latest_run = get_latest_run(workflow)

    print(f"Launching workflow '{workflow.name}' on the branch '{branch_name}'")
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
        raise RuntimeError(f"Unable to locate the new run request for the '{workflow.name}' workflow")

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


def parse_questions(questions: str, total_number_of_questions: int) -> list[int]:
    if not questions:
        return []

    if questions == "*":
        return list(range(1, total_number_of_questions + 1))

    items = questions.split(",")
    nums = []
    for item in items:
        try:
            num = int(item)
            if num > total_number_of_questions:
                raise RuntimeError(
                    f"Requested question number ({num}) is greater than the total number of questions available ({total_number_of_questions})"
                )
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
            if upper_int > total_number_of_questions:
                raise RuntimeError(
                    f"Requested question number ({upper_int}) is greater than the total number of questions available ({total_number_of_questions})"
                )
            nums.extend(range(lower_int, upper_int + 1))
        except ValueError:
            raise ValueError(f"Invalid question item; expected a number or a range, instead got {item}")

    return nums
