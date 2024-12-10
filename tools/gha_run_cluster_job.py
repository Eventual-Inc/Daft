# /// script
# requires-python = ">=3.12"
# dependencies = ["PyGithub"]
# ///

import argparse
import os
import pathlib

from github import Auth, Github, enable_console_debug_logging

GITHUB_OAUTH_TOKEN_ENVVAR = "GITHUB_OAUTH_TOKEN"


def get_oauth_token() -> str:
    token = os.getenv(GITHUB_OAUTH_TOKEN_ENVVAR)
    if token is None:
        raise RuntimeError(
            "Unable to find github token at $GITHUB_OAUTH_TOKEN. Please generate one and assign it to the environment variable. "
            "We recommend creating a Classic token (the fine-grained ones seem to not really work) and give it at least Workflow as well as Repository access. "
            "https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens"
        )
    return token


def main(
    daft_wheel_url: str | None,
    daft_version: str,
    path_to_script: pathlib.Path,
    script_args: str | None,
    branch: str = "main",
):
    auth = Auth.Token(get_oauth_token())
    g = Github(auth=auth)

    daft_package = {"daft_wheel_url": daft_wheel_url} if daft_wheel_url is not None else {"daft_version": daft_version}
    entrypoint_args = {"entrypoint_args": script_args} if script_args is not None else {}

    inputs = {
        **daft_package,
        "entrypoint_script": str(path_to_script.name),
        "working_dir": str(path_to_script.parent),
        **entrypoint_args,
    }

    repo = g.get_repo("Eventual-Inc/Daft")
    workflow = repo.get_workflow("run-cluster.yaml")

    created = workflow.create_dispatch(
        ref=branch,
        inputs={**inputs},
    )

    if not created:
        raise RuntimeError("Could not create workflow, suggestion: run again with --verbose")

    print("Workflow created, view it at: https://github.com/Eventual-Inc/Daft/actions/workflows/run-cluster.yaml")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run Daft cluster workflow on github actions",
        usage="uv run gha_run_cluster_job.py [-h] [--branch BRANCH] [--daft-wheel-url DAFT_WHEEL_URL | --daft-version DAFT_VERSION] path_to_script [-- script_args ...]",
    )
    parser.add_argument("path_to_script", type=pathlib.Path, help="Path to the script to run")
    parser.add_argument("--branch", default="main", help="GitHub branch to use")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--daft-wheel-url", help="URL of the Daft wheel")
    group.add_argument("--daft-version", default="0.3.15", help="Released version of daft (default: 0.3.15)")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose mode")
    args, extra = parser.parse_known_args()

    if args.verbose:
        enable_console_debug_logging()

    script_args = None
    if extra:
        if extra[0] != "--":
            raise ValueError(f"Arguments to the script must be passed in after a '--', received: {' '.join(extra)}")
        script_args = " ".join(extra[1:])

    main(
        daft_wheel_url=args.daft_wheel_url,
        daft_version=args.daft_version,
        path_to_script=args.path_to_script,
        script_args=script_args,
        branch=args.branch,
    )
