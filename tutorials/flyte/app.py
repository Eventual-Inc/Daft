from __future__ import annotations

from flytekit import current_context, task, workflow

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
def wf():
    produce_resized_image_dataset(limit=5)


def daft_notebook_code(limit: int):
    # Download data from Parquet file
    PARQUET_PATH = "https://huggingface.co/datasets/ChristophSchuhmann/improved_aesthetics_6.5plus/resolve/main/data/train-00000-of-00001-6f24a7497df494ae.parquet"
    parquet_df = daft.read_parquet(PARQUET_PATH)
    parquet_df = parquet_df.select(parquet_df["URL"], parquet_df["TEXT"], parquet_df["AESTHETIC_SCORE"])

    # Filter for only "pretty" images
    pretty_df = parquet_df.where(parquet_df["AESTHETIC_SCORE"] > 7)

    # Download and resize images
    pretty_df = pretty_df.with_column(
        "image",
        pretty_df["URL"].url.download(on_error="null").image.decode(),
    )
    pretty_df = pretty_df.with_column(
        "resized_image",
        pretty_df["image"].image.resize(32, 32),
    )

    # Write processed data out to Parquet file
    written_df = (
        pretty_df.select("URL", "TEXT", "resized_image")
        .limit(limit)
        .write_parquet(
            # NOTE: In practice, you will be writing to cloud storage such as AWS S3.
            # This currently writes to a local (ephemeral) file.
            f"my-s3-bucket/{current_context().execution_id.name}/resized_images.parquet"
        )
    )
    return written_df
