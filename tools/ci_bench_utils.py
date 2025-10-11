import os
from datetime import datetime, timezone

import gspread
from ray.job_submission import JobDetails, JobSubmissionClient

import daft


def upload_to_google_sheets(worksheet, data):
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
        "github ref": os.getenv("GITHUB_REF_NAME"),
        "github sha": os.getenv("GITHUB_SHA"),
    }


async def tail_logs(client: JobSubmissionClient, submission_id: str) -> JobDetails:
    async for lines in client.tail_job_logs(submission_id):
        print(lines, end="")

    return client.get_job_info(submission_id)
