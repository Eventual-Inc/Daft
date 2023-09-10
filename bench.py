from __future__ import annotations

import os

import psutil

from daft.table import Table

pid = os.getpid()

# url = "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet"
url = (
    "s3://eventual-dev-benchmarking-fixtures/parquet-benchmarking/tpch/200MB-2RG/daft_200MB_lineitem_chunk.RG-2.parquet"
)

# tab = Table.read_parquet(url, columns=["L_COMMENT"])
tab = Table.read_parquet(url, multithreaded_io=True)
# print(tab.schema())

python_process = psutil.Process(pid)
memoryUse = python_process.memory_info()[0] / 2.0**30  # memory use in GB...I think
print("memory use:", memoryUse)
