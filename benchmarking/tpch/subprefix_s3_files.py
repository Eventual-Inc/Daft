"""
Introduces more prefixes into TPCH data files hosted on S3.
This improves S3 read performance. For more details, see:
https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html

Does this by copying existing files into subfolders, based on file prefix.
e.g. copies

    customer/01387.parquet
    customer/01516.parquet
    customer/03493.parquet
    ...

to

    customer/01/387.parquet
    customer/01/516.parquet
    customer/03/493.parquet
    ...
"""

from __future__ import annotations

import argparse
import pathlib
import urllib

import boto3
import ray

FOLDERS = [
    # "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
]


@ray.remote(num_cpus=0.1)
class S3Client:
    def __init__(self) -> None:
        import boto3

        self.s3 = boto3.client("s3")

    def insert_prefix(
        self,
        bucket: str,
        prefix: str,
        length: int,
        old_key: str,
    ) -> None:
        insert_slash_at = len(prefix) + 1 + length  # +1 for /
        new_key = old_key[:insert_slash_at] + "/" + old_key[insert_slash_at:]
        self.s3.copy(
            CopySource={"Bucket": bucket, "Key": old_key},
            Bucket=bucket,
            Key=new_key,
        )
        print(f"Copied: {old_key} -> {new_key}")


def subpartition_s3_prefix(
    bucket: str,
    root_prefix: str,
    length: int,
) -> None:
    s3 = boto3.client("s3")

    pool = ray.util.ActorPool([S3Client.remote() for _ in range(32)])

    for folder in FOLDERS:
        prefix = root_prefix + "/" + folder
        list_resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        full_keys = [content["Key"] for content in list_resp["Contents"]]

        list(
            pool.map(
                lambda actor, old_key: actor.insert_prefix.remote(
                    bucket=bucket, prefix=prefix, length=length, old_key=old_key
                ),
                full_keys,
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--s3-uri",
        help="S3 path to TPCH folders (where customer, lineitem, etc. reside).",
    )
    parser.add_argument(
        "--prefix-length",
        default=2,
        type=int,
        help="Number of characters to insert a new prefix after.",
    )
    args = parser.parse_args()

    s3_url = urllib.parse.urlparse(args.s3_uri)
    bucket = s3_url.netloc
    prefix = str(pathlib.Path(s3_url.path[1:]))  # Remove leading slash, trailing slash if present
    subpartition_s3_prefix(bucket=bucket, root_prefix=prefix, length=args.prefix_length)
