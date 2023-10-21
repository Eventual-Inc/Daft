from __future__ import annotations

from fastapi import FastAPI, Request, Response
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from ..utils.responses import get_response

BUCKET_NAME = "80-per-second-rate-limited-gets-bucket"
OBJECT_KEY_URL = "/{item_id}"

route = f"/{BUCKET_NAME}"
app = FastAPI()


def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> Response:
    return get_response(request.url, status_code=503, status_code_str="SlowDown")


limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)


@app.get(OBJECT_KEY_URL)
@limiter.shared_limit(limit_value="80/second", scope="my_shared_limit")
async def rate_limited_bucket_get(request: Request, item_id: str):
    """This endpoint will just echo the `item_id` and return that as the response body"""
    result = item_id.encode("utf-8")
    return Response(
        status_code=200,
        content=result,
        headers={
            "Content-Length": str(len(result)),
            "Content-Type": "binary/octet-stream",
        },
    )
