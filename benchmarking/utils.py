"""Shared utilities for benchmark scripts."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from ray.job_submission import JobDetails, JobSubmissionClient
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

import daft

logger = logging.getLogger(__name__)


def _is_retryable_gspread_error(exc: BaseException) -> bool:
    from gspread.exceptions import APIError

    if not isinstance(exc, APIError):
        return False
    code = exc.response.status_code
    return code in (429, 500, 502, 503, 504)


def daft_uv_runtime_env() -> dict:
    """Build the uv runtime_env config, pinning the exact Daft version if available."""
    daft_version = os.getenv("DAFT_VERSION")
    daft_index_url = os.getenv("DAFT_INDEX_URL")
    daft_pkg = f"daft[aws]=={daft_version}" if daft_version else "daft[aws]"
    uv_env: dict = {"packages": [daft_pkg]}
    if daft_index_url:
        uv_env["uv_pip_install_options"] = ["--extra-index-url", daft_index_url, "--index-strategy", "unsafe-best-match"]
    return uv_env


@retry(
    retry=retry_if_exception(_is_retryable_gspread_error),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    before_sleep=lambda rs: logger.warning("Retrying Google Sheets upload (attempt %d) after %s", rs.attempt_number, rs.outcome.exception()),
)
def upload_to_google_sheets(worksheet, data):
    import gspread

    gc = gspread.service_account()

    sh = gc.open_by_url(
        "https://docs.google.com/spreadsheets/d/1d6pXsIsBkjjM93GYtoiF83WXvJXR4vFgFQdmG05u8eE/edit?gid=0#gid=0"
    )

    try:
        ws = sh.worksheet(worksheet)
    except gspread.exceptions.WorksheetNotFound:
        # Create the worksheet if it doesn't exist
        ws = sh.add_worksheet(title=worksheet, rows=100, cols=20)

    ws.append_row(data)


def get_run_metadata():
    return {
        "started at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
        "daft version": daft.__version__,
        "github ref": os.getenv("DAFT_REF_NAME", os.getenv("GITHUB_REF_NAME")),
        "github sha": os.getenv("DAFT_SHA", os.getenv("GITHUB_SHA")),
    }


async def tail_logs(client: JobSubmissionClient, submission_id: str) -> JobDetails:
    async for lines in client.tail_job_logs(submission_id):
        print(lines, end="")

    return client.get_job_info(submission_id)
