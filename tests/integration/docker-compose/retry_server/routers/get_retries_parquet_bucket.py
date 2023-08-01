from __future__ import annotations

import os
from typing import Annotated

from fastapi import APIRouter, Header, Response

from ..utils.parquet_generation import generate_parquet_file
from ..utils.responses import get_response

BUCKET_NAME = "get-retries-parquet-bucket"
OBJECT_KEY_URL = "/{status_code}/{num_errors}/{item_id}"
MOCK_PARQUET_DATA_PATH = generate_parquet_file()

ITEM_ID_TO_NUM_RETRIES: dict[tuple[str, tuple[int, int]], int] = {}

router = APIRouter(prefix=f"/{BUCKET_NAME}")


@router.head(OBJECT_KEY_URL)
async def bucket_head(status_code: int, num_errors: int, item_id: str):
    return Response(
        headers={
            "Content-Length": str(os.path.getsize(MOCK_PARQUET_DATA_PATH.name)),
            "Content-Type": "binary/octet-stream",
            "Accept-Ranges": "bytes",
        },
    )


@router.get(OBJECT_KEY_URL)
async def retryable_bucket_get(
    status_code: int,
    num_errors: int,
    item_id: str,
    range: Annotated[str, Header()],
):
    # If we've only seen this range request <= num_errors times, we throw an error
    start, end = (int(i) for i in range[len("bytes=") :].split("-"))
    key = (item_id, (start, end))
    if key not in ITEM_ID_TO_NUM_RETRIES:
        ITEM_ID_TO_NUM_RETRIES[key] = 1
    else:
        ITEM_ID_TO_NUM_RETRIES[key] += 1
    if ITEM_ID_TO_NUM_RETRIES[key] <= num_errors:
        return get_response(BUCKET_NAME, status_code, num_errors, item_id)

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
