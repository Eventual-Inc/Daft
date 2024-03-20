from __future__ import annotations

from typing import Union

from prefect.filesystems import LocalFileSystem
from prefect_aws import AwsClientParameters, AwsCredentials, S3Bucket

from daft.daft import IOConfig, S3Config

PrefectBlocks = (S3Bucket, LocalFileSystem)
PrefectBlockType = Union[S3Bucket, LocalFileSystem]


def prefect_block_to_path_and_io_config(prefect_block: PrefectBlockType) -> tuple[str, IOConfig | None]:
    """Converts a PrefectBlockType to a path string

    Args:
        prefect_block (PrefectBlockType): PrefectBlockType to convert to path

    Returns:
        str: Path string
    """
    if isinstance(prefect_block, S3Bucket):
        creds: AwsCredentials = prefect_block.credentials
        if (
            creds.aws_access_key_id is not None
            or creds.aws_secret_access_key is not None
            or creds.aws_session_token is not None
            or creds.region_name is not None
            or creds.aws_client_parameters is not None
        ):
            params: AwsClientParameters = creds.aws_client_parameters
            io_config = IOConfig(
                s3=S3Config(
                    key_id=creds.aws_access_key_id,
                    access_key=(
                        creds.aws_secret_access_key.get_secret_value()
                        if creds.aws_secret_access_key is not None
                        else None
                    ),
                    session_token=creds.aws_session_token,
                    region_name=creds.region_name,
                    endpoint_url=params.endpoint_url if params is not None else None,
                    use_ssl=params.use_ssl if params is not None else None,
                    verify_ssl=params.verify if params is not None else None,
                )
            )
        else:
            io_config = None

        if prefect_block.bucket_folder != "":
            path = f"s3://{prefect_block.bucket_name}/{prefect_block.bucket_folder}"
        else:
            path = f"s3://{prefect_block.bucket_name}"

    elif isinstance(prefect_block, LocalFileSystem):
        path = prefect_block.basepath
        io_config = None
    else:
        raise ValueError(f"Unsupported PrefectBlockType: {prefect_block}")

    return path, io_config
