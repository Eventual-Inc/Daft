# /// script
# requires-python = ">=3.12"
# dependencies = ["requests"]
# ///

from argparse import ArgumentParser

import requests


def main(submission_id: str):
    r = requests.get("http://localhost:8265/api/v0/jobs")
    r.status_code
    jobs = r.json()
    results = jobs["data"]["result"]["result"]
    for result in results:
        if result["submission_id"] == submission_id:
            print(result["job_id"])
            break

    raise RuntimeError(f"Failed to find a job-id mapping to the submission-id '{submission_id}'")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("submission_id", type=str)

    args = parser.parse_args()

    main(args.submission_id)
