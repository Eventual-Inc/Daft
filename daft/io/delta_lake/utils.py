from __future__ import annotations

import os


def construct_delta_file_path(scheme: str, table_uri: str, relative_path: str) -> str:
    """Construct full file path for Delta Lake files based on URI scheme.

    Args:
        scheme: The URI scheme (e.g., 's3', 'file', etc.)
        table_uri: The base table URI
        relative_path: The relative path from the Delta log

    Returns:
        The full file path
    """
    if scheme in (
        "s3",
        "s3a",
        "gcs",
        "gs",
        "az",
        "abfs",
        "abfss",
    ):  # object storage does not use os path.join, but instead always uses `/`
        return table_uri.rstrip("/") + "/" + relative_path
    else:
        return os.path.join(table_uri, relative_path)
