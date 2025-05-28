from __future__ import annotations

from flytekit import current_context, task, workflow
from flytekitplugins.ray import (  # noqa # pylint: disable=unused-import
    RayJobConfig,
    WorkerNodeConfig,
)

import daft


###
# Run Daft on a Ray cluster
###
# @task(
#     task_config=RayJobConfig(
#         worker_node_config=[
#             WorkerNodeConfig(
#                 group_name="ray-group", replicas=1,
#             )
#         ],
#     )
# )
#
###
# Run Daft on a (big!) Flyte task
###
@task()
def produce_resized_image_dataset(limit: int) -> list[str]:
    # NOTE: Use Ray Runner:
    #
    #     1. If Ray connection has been set up already by Flyte, it will use the initialized Ray cluster connection
    #     2. Otherwise, it will create a local Ray cluster on the Flyte task
    #
    daft.context.set_runner_ray()

    written_df = daft_notebook_code(limit)

    # Return the number of rows written
    return written_df.to_pydict()["file_path"]


@workflow()
def wf(limit: int):
    produce_resized_image_dataset(limit=limit)


def daft_notebook_code(limit: int):
    # Download data from Parquet file
    IO_CONFIG = IO_CONFIG = daft.io.IOConfig(
        s3=daft.io.S3Config(anonymous=True, region_name="us-west-2")
    )  # Use anonymous-mode for accessing AWS S3
    PARQUET_PATH = "s3://daft-public-data/tutorials/laion-parquet/train-00000-of-00001-6f24a7497df494ae.parquet"
    parquet_df = daft.read_parquet(PARQUET_PATH, io_config=IO_CONFIG)
    parquet_df = parquet_df.select(parquet_df["URL"], parquet_df["TEXT"], parquet_df["AESTHETIC_SCORE"])

    # Filter for only "pretty" images
    filtered_df = parquet_df.where(parquet_df["TEXT"].str.contains("darkness"))

    # Download and resize images
    filtered_df = filtered_df.with_column(
        "image",
        filtered_df["URL"].url.download(on_error="null").image.decode(),
    )
    filtered_df = filtered_df.with_column(
        "resized_image",
        filtered_df["image"].image.resize(32, 32),
    )

    # Write processed data out to Parquet file
    written_df = (
        filtered_df.select("URL", "TEXT", "resized_image")
        .limit(limit)
        .write_parquet(
            # NOTE: In practice, you will be writing to cloud storage such as AWS S3.
            # This currently writes to a local (ephemeral) file.
            f"my-s3-bucket/{current_context().execution_id.name}/resized_images.parquet"
        )
    )
    return written_df
