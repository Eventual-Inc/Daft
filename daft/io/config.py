from __future__ import annotations

from daft.daft import IOConfig


def _io_config_from_json(io_config_json: str) -> IOConfig:
    """Used when deserializing a serialized IOConfig object"""
    return IOConfig.from_json(io_config_json)
