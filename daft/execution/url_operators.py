from __future__ import annotations

import pathlib
from urllib.parse import urlparse

import pyarrow as pa
from loguru import logger

from daft import filesystem
from daft.runners.blocks import ArrowDataBlock


def download(url_block: ArrowDataBlock) -> list[bytes | None]:
    assert isinstance(
        url_block, ArrowDataBlock
    ), f"Can only download from columns containing strings, found non-arrow block"
    assert pa.types.is_string(
        url_block.data.type
    ), f"Can only download from columns containing strings, found {url_block.data.type}"

    results: list[bytes | None] = [None for _ in range(len(url_block))]

    path_to_result_idx: dict[str, list[int]] = {}
    to_download: dict[str, list[str]] = {}
    for i, path in enumerate(url_block.iter_py()):
        if path is None:
            continue
        protocol = filesystem.get_protocol_from_path(path)

        # fsspec returns data keyed by absolute paths
        if protocol == "file":
            path = str(pathlib.Path(path).resolve())
        # fsspec strips the s3:// scheme from the path after .cat
        elif protocol == "s3":
            parsed_url = urlparse(path)
            path = parsed_url.netloc + parsed_url.path

        if protocol not in to_download:
            to_download[protocol] = []
        to_download[protocol].append(path)

        if path not in path_to_result_idx:
            path_to_result_idx[path] = []
        path_to_result_idx[path].append(i)

    for protocol in to_download:
        fs = filesystem.get_filesystem(protocol)
        data = fs.cat(to_download[protocol], on_error="return")
        for path in data:
            if isinstance(data[path], bytes):
                for i in path_to_result_idx[path]:
                    results[i] = data[path]
            else:
                logger.error(f"Encountered error during download from URL {path}: {str(data[path])}")
                for i in path_to_result_idx[path]:
                    results[i] = None

    return results
