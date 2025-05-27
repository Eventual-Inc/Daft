import os
from datetime import datetime, timezone

import gspread

import daft


def upload_to_google_sheets(worksheet, data):
    gc = gspread.service_account()

    sh = gc.open_by_url(
        "https://docs.google.com/spreadsheets/d/1d6pXsIsBkjjM93GYtoiF83WXvJXR4vFgFQdmG05u8eE/edit?gid=0#gid=0"
    )
    ws = sh.worksheet(worksheet)
    ws.append_row(data)


def get_run_metadata():
    return {
        "started at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
        "daft version": daft.__version__,
        "github ref": os.getenv("GITHUB_REF_NAME"),
        "github sha": os.getenv("GITHUB_SHA"),
    }
