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


def get_name_and_commit_hash(local_branch_name: Optional[str]) -> tuple[str, str]:
    local_branch_name = local_branch_name or "HEAD"
    remote_branch_name = local_branch_name

    try:
        # Check if the branch has a remote tracking branch.
        local_branch_name = (
            subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", f"{local_branch_name}@{{upstream}}"], stderr=subprocess.STDOUT
            )
            .strip()
            .decode("utf-8")
        )
        # Strip the upstream name from the branch to get the branch name on the remote repo.
        remote_branch_name = local_branch_name.split("/", 1)[1]
    except subprocess.CalledProcessError:
        local_branch_name = (
            subprocess.check_output(["git", "rev-parse", "--abbrev-ref", local_branch_name], stderr=subprocess.STDOUT)
            .strip()
            .decode("utf-8")
        )
        remote_branch_name = local_branch_name

    commit_hash = (
        subprocess.check_output(["git", "rev-parse", local_branch_name], stderr=subprocess.STDOUT)
        .strip()
        .decode("utf-8")
    )
    # Return the remote branch name for the github action.
    return remote_branch_name, commit_hash


def parse_questions(questions: Optional[str], total_number_of_questions: int) -> list[int]:
    if questions is None:
        return list(range(1, total_number_of_questions + 1))
    else:

        def to_int(q: str) -> int:
            question = int(q)
            if question > total_number_of_questions:
                raise ValueError(
                    f"Question number should be less than {total_number_of_questions}, instead got {question}"
                )
            return question

        return list(map(to_int, filter(lambda q: q, questions.split(","))))
