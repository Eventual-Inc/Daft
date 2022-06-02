from typing import List, Optional

import numpy as np
import ray
from ray.data.impl.arrow_block import ArrowRow

from daft.datarepo import DatarepoClient, get_client
from daft.datarepo.client import DatarepoClient


def read_datarepo(
    datarepo_id: str,
    *,
    datarepo_client: Optional[DatarepoClient] = None,
    parallelism: int = 200,
    columns: Optional[List[str]] = None,
) -> ray.data.Dataset[ArrowRow]:
    if datarepo_client is None:
        datarepo_client = get_client()

    parquet_filepaths = datarepo_client.get_parquet_filepaths(datarepo_id)

    # Ray API does not recognize file:// as a protocol
    if parquet_filepaths and parquet_filepaths[0].startswith("file://"):
        parquet_filepaths = [path.replace("file://", "") for path in parquet_filepaths]

    # TODO(jaychia): Use .read_parquet_bulk instead in Ray 1.13 after its release for various performance improvements
    ds: ray.data.Dataset[ArrowRow] = ray.data.read_parquet(
        parquet_filepaths,
        parallelism=parallelism,
        # Arrow read_table **kwargs
        # See: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html
        columns=columns,
        # TODO(jaychia): Need to get schema from the datarepo logs when that is ready
        # This will currently just infer the schema from the Parquet files
        # schema=datarepo_client.get_schema(datarepo_id),
    )
    return ds
