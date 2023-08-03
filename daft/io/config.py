from __future__ import annotations

import json

from daft.daft import IOConfig, S3Config


def _io_config_from_json(io_config_json: str) -> IOConfig:
    """Used when deserializing a serialized IOConfig object"""
    data = json.loads(io_config_json)
    s3_config = S3Config(**data["s3"]) if "s3" in data else None
    return IOConfig(s3=s3_config)
