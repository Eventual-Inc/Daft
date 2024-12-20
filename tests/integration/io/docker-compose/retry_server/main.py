"""This file defines a FastAPI server that emulates an S3 implementation that serves Parquet files.

This S3 implementation serves a small Parquet file at the location: `s3://{bucket-name}/{status_code}/{num_errors}/{item_id}`

1. `status_code` is the HTTP status code it returns when S3 clients hit that endpoint
2. `num_errors` is the number of times it throws that error before successfully returning data for a given requested byte range
3. `item_id` is an ID generated by the clients to uniquely identify one attempt at retrieving this Parquet file

We provide two different buckets, with slightly different behavior:

1. "head-retries-parquet-bucket": this bucket throws errors during HEAD operations
2. "get-retries-parquet-bucket": this bucket throws errors during the ranged GET operations
"""

from __future__ import annotations

from fastapi import FastAPI

from .routers import (
    get_retries_parquet_bucket,
    head_retries_parquet_bucket,
    rate_limited_echo_gets_bucket,
)

app = FastAPI()
app.mount(get_retries_parquet_bucket.route, get_retries_parquet_bucket.app)
app.mount(head_retries_parquet_bucket.route, head_retries_parquet_bucket.app)
app.mount(rate_limited_echo_gets_bucket.route, rate_limited_echo_gets_bucket.app)
