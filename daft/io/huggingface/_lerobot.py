from __future__ import annotations

import logging
import re
import warnings
from typing import TYPE_CHECKING

from daft.exceptions import DaftCoreException
from daft.io.huggingface._metadata import read_json, repo_info, repo_root

if TYPE_CHECKING:
    from daft.daft import IOConfig


logger = logging.getLogger(__name__)


def warn_if_lerobot(repo: str, revision: str | None, io_config: IOConfig) -> None:
    """Best-effort identification of LeRobot, without changing generic-reader results."""
    try:
        info = repo_info(repo, revision, io_config)
        paths = {entry["rfilename"] for entry in info.get("siblings", [])}
        if "meta/info.json" not in paths:
            return
        tags = {tag.lower() for tag in info.get("tags", [])}
        is_lerobot = "lerobot" in tags
        if not is_lerobot:
            metadata = read_json(f"{repo_root(repo, revision)}/meta/info.json", io_config)
            is_lerobot = (
                re.fullmatch(r"v\d+(?:\.\d+)?", metadata.get("codebase_version", "")) is not None
                and isinstance(metadata.get("features"), dict)
                and isinstance(metadata.get("data_path"), str)
                and isinstance(metadata.get("fps"), (int, float))
            )
    except (FileNotFoundError, DaftCoreException, ValueError, TypeError, KeyError):
        # An advisory probe must not make a working native data read fail. The actual
        # discovery/scan still reports its own authentication, network, or data errors.
        logger.debug("Could not inspect Hugging Face dataset metadata for LeRobot detection", exc_info=True)
        return
    if is_lerobot:
        warnings.warn(
            f"{repo!r} is a LeRobot dataset. read_huggingface reads generic records and does not perform "
            "LeRobot episode/video timestamp alignment. Use "
            f"daft.datasets.lerobot.read({repo!r}, io_config=...) for the dedicated reader.",
            UserWarning,
            stacklevel=3,
        )
