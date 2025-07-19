from __future__ import annotations

import os
from typing import Annotated

from fastapi import FastAPI, Header, Request, Response

from ..utils.parquet_generation import generate_parquet_file
from ..utils.request_range import parse_range_from_header
from ..utils.responses import get_response

BUCKET_NAME = "retries-parquet-bucket"
OBJECT_KEY_URL = "/{status_code}/{status_code_str}/{num_errors}/{item_id}"
MOCK_PARQUET_DATA_PATH = generate_parquet_file()

HEAD_NUM_RETRIES: dict[str, int] = {}
GET_NUM_RETRIES: dict[tuple[str, tuple[int, int]], int] = {}
NON_RETRYABLE_ERRORS = [400, 403, 404]

route = f"/{BUCKET_NAME}"
app = FastAPI()


@app.head(OBJECT_KEY_URL)
async def bucket_head(request: Request, status_code: int, status_code_str: str, num_errors: int, item_id: str):
    if status_code in NON_RETRYABLE_ERRORS:
        return get_response(request.url, status_code, status_code_str)

    key = item_id
    if key not in HEAD_NUM_RETRIES:
        HEAD_NUM_RETRIES[key] = 1
    else:
        HEAD_NUM_RETRIES[key] += 1
    if HEAD_NUM_RETRIES[key] <= num_errors:
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
    request: Request,
    status_code: int,
    status_code_str: str,
    num_errors: int,
    item_id: str,
    range: Annotated[str, Header()],
):
    if status_code in NON_RETRYABLE_ERRORS:
        return get_response(request.url, status_code, status_code_str)

    # If we've only seen this range request <= num_errors times, we throw an error
    start, end = parse_range_from_header(range, os.path.getsize(MOCK_PARQUET_DATA_PATH.name))

    key = (item_id, (start, end))
    if key not in GET_NUM_RETRIES:
        GET_NUM_RETRIES[key] = 1
    else:
        GET_NUM_RETRIES[key] += 1
    if GET_NUM_RETRIES[key] <= num_errors:
        return get_response(request.url, status_code, status_code_str)

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
