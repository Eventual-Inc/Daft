from __future__ import annotations

import os
from typing import Annotated

from fastapi import FastAPI, Header, Request, Response

from ..utils.parquet_generation import generate_parquet_file
from ..utils.responses import get_response

BUCKET_NAME = "head-retries-parquet-bucket"
OBJECT_KEY_URL = "/{status_code}/{status_code_str}/{num_errors}/{item_id}"
MOCK_PARQUET_DATA_PATH = generate_parquet_file()

ITEM_ID_TO_NUM_RETRIES: dict[str, int] = {}

route = f"/{BUCKET_NAME}"
app = FastAPI()


@app.head(OBJECT_KEY_URL)
async def retryable_bucket_head(
    request: Request, status_code: int, status_code_str: str, num_errors: int, item_id: str
):
    """Reading of Parquet starts with a head request, which potentially must be retried as well"""
    key = item_id
    if key not in ITEM_ID_TO_NUM_RETRIES:
        ITEM_ID_TO_NUM_RETRIES[key] = 1
    else:
        ITEM_ID_TO_NUM_RETRIES[key] += 1
    if ITEM_ID_TO_NUM_RETRIES[key] <= num_errors:
        return get_response(request.url, status_code, status_code_str)

    return Response(
        headers={
            "Content-Length": str(os.path.getsize(MOCK_PARQUET_DATA_PATH.name)),
            "Content-Type": "binary/octet-stream",
            "Accept-Ranges": "bytes",
        },
    )


@app.get(OBJECT_KEY_URL)
async def bucket_get(
    status_code: int,
    status_code_str: str,
    num_errors: int,
    item_id: str,
    range: Annotated[str, Header()],
):
    start, end = (int(i) for i in range[len("bytes=") :].split("-"))
    with open(MOCK_PARQUET_DATA_PATH.name, "rb") as f:
        f.seek(start)
        data = f.read(end - start + 1)

    return Response(
        status_code=206,
        content=data,
        headers={
            "Content-Length": str(len(data)),
            "Content-Type": "binary/octet-stream",
            "Content-Range": f"bytes {start}-{end}/{os.path.getsize(MOCK_PARQUET_DATA_PATH.name)}",
        },
    )
