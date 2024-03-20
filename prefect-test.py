from __future__ import annotations

from prefect import flow, task
from prefect.filesystems import LocalFileSystem
from prefect_aws import AwsCredentials, S3Bucket

import daft


@task
def extract(bucket: S3Bucket) -> daft.DataFrame:
    return daft.read_parquet(bucket)


@task
def transform(df: daft.DataFrame) -> daft.DataFrame:
    df = df.where((df["WIDTH"] > 100) & (df["HEIGHT"] < 900))
    df = df.agg(df["similarity"].mean(), df["AESTHETIC_SCORE"].sum())
    return df


@task
def load(df: daft.DataFrame, fs: LocalFileSystem) -> str:
    return df.write_parquet(fs)


@flow(log_prints=True)
def etl_flow(source: S3Bucket, sink: LocalFileSystem):
    daft.context.set_runner_ray()

    df = extract(source)
    transformed_df = transform(df)
    file_paths = load(transformed_df, sink)

    print(file_paths)


if __name__ == "__main__":
    creds = AwsCredentials.load("aws-creds")
    bucket = S3Bucket(bucket_name="daft-public-data", credentials=creds, bucket_folder="tutorials/laion-parquet")
    fs = LocalFileSystem(basepath="z")
    etl_flow(bucket, fs)
