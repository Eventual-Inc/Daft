from __future__ import annotations

from typing import List, Optional

from loguru import logger

from daft import filesystem
from daft.udf import udf


def _download_udf(urls: list[str | None]) -> list[bytes | None]:
    """Downloads the contents of the supplied URLs."""
    results: list[bytes | None] = []
    filesystems: dict[str, filesystem.AbstractFileSystem] = {}

    for path in urls:
        if path is None:
            results.append(None)
            continue

        protocol = filesystem.get_protocol_from_path(path)
        if protocol not in filesystems:
            filesystems[protocol] = filesystem.get_filesystem(protocol)
        fs = filesystems[protocol]

        try:
            result = fs.cat_file(path)
        except Exception as e:
            logger.error(f"Encountered error during download from URL {path}: {str(e)}")
            result = None
        results.append(result)

    return results


# HACK: Workaround for Ray pickling issues if we use the @polars_udf decorator instead.
# There may be some issues around runtime imports and Ray pickling of decorated functions
download_udf = udf(_download_udf, return_type=bytes, type_hints={"urls": List[Optional[str]]})
