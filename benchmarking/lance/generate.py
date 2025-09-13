from __future__ import annotations

import os

import daft
from daft.io import IOConfig, S3Config

DATASET_PATH = os.getenv("DATASET_PATH", "s3://xx/ai/dataset/ILSVRC2012/")
LANCE_PATH = os.getenv("LANCE_PATH", "s3://xx/ai/dataset/ILSVRC2012/")
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT", "https://xx.xx")
AWS_KEY_ID = os.getenv("AWS_KEY_ID", "xx")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY", "xx==")
# Temporary path for local testing; it is recommended to write to S3.
LOCAL_PATH = "/tmp/test.lance"
# Dynamically adjust the number of rows to be written.
COUNT_ROWS = 1000

io_config = IOConfig(
    s3=S3Config(
        endpoint_url=AWS_ENDPOINT,
        force_virtual_addressing=True,
        key_id=AWS_KEY_ID,
        access_key=AWS_ACCESS_KEY,
        verify_ssl=True,
        region_name="xx",
    )
)

df = daft.from_glob_path(f"{DATASET_PATH}*/*.JPEG", io_config)
df = daft.sql("SELECT *  FROM df")
df = df.with_column("image", daft.col("path").url.download(io_config=io_config).image.decode(mode="RGB"))
df = df.limit(COUNT_ROWS)
df.write_lance(LOCAL_PATH)
