"""Shared utilities for benchmark scripts."""

from __future__ import annotations

import os
from datetime import datetime, timezone

from ray.job_submission import JobDetails, JobSubmissionClient

import daft


def daft_uv_runtime_env() -> dict:
    """Build the uv runtime_env config, pinning the exact Daft version if available."""
    daft_version = os.getenv("DAFT_VERSION")
    daft_index_url = os.getenv("DAFT_INDEX_URL")
    daft_pkg = f"daft[aws]=={daft_version}" if daft_version else "daft[aws]"
    uv_env: dict = {"packages": [daft_pkg]}
    if daft_index_url:
        uv_env["uv_pip_install_options"] = ["--index-url", daft_index_url, "--extra-index-url", "https://pypi.org/simple/"]
    return uv_env


def upload_to_google_sheets(worksheet, data):
    import gspread

    gc = gspread.service_account()

    sh = gc.open_by_url(
        "https://docs.google.com/spreadsheets/d/1d6pXsIsBkjjM93GYtoiF83WXvJXR4vFgFQdmG05u8eE/edit?gid=0#gid=0"
    )

    try:
        ws = sh.worksheet(worksheet)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet, rows=100, cols=20)

    ws.append_row(data)


def get_run_metadata():
    return {
        "started at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
        "daft version": daft.__version__,
        "github ref": os.getenv("GITHUB_REF_NAME"),
        "github sha": os.getenv("GITHUB_SHA"),
    }


async def tail_logs(client: JobSubmissionClient, submission_id: str) -> JobDetails:
    async for lines in client.tail_job_logs(submission_id):
        print(lines, end="")

    return client.get_job_info(submission_id)
