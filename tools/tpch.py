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

import github
import inquirer
import utils


def run(
    branch_name: str,
    questions: str,
    scale_factor: int,
    num_partitions: int,
    cluster_profile: str,
    env_vars: str,
):
    branch_name, commit_hash = utils.get_name_and_commit_hash(branch_name)
    user_wants_to_run_tpch_benchmarking = inquirer.confirm(
        message=f"Going to run TPCH benchmarking with the 'run-cluster' workflow on the branch '{branch_name}' (commit-hash: {commit_hash}); proceed?"
    )
    if not user_wants_to_run_tpch_benchmarking:
        print("Workflow aborted")
        exit(1)

    expanded_questions = utils.parse_questions(questions)
    print(
        f"Running scale-factor of {scale_factor}GB with {num_partitions} partitions on questions: {', '.join(map(str, expanded_questions))}"
    )
    args_as_list = [
        f"--question-number={q} --parquet-folder=s3://eventual-dev-benchmarking-fixtures/uncompressed/tpch-dbgen/{scale_factor}_0/{num_partitions}/parquet/"
        for q in expanded_questions
    ]
    entrypoint_args = json.dumps(args_as_list)

    workflow = utils.repo.get_workflow("run-cluster.yaml")
    utils.dispatch(
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ref", type=str, required=False, help="The branch name to run on")
    parser.add_argument(
        "--questions", type=str, required=False, default="*", help="A comma separated list of questions to run"
    )
    parser.add_argument("--scale-factor", type=int, required=False, default=2, help="The scale factor to run on")
    parser.add_argument("--num-partitions", type=int, required=False, default=2, help="The number of partitions")
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
        github.enable_console_debug_logging()

    run(
        branch_name=args.ref,
        questions=args.questions,
        scale_factor=args.scale_factor,
        num_partitions=args.num_partitions,
        cluster_profile=args.cluster_profile,
        env_vars=args.env_vars,
    )
