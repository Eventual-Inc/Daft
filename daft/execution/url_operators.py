import pathlib
from typing import Dict, List, Optional

import pyarrow as pa
from loguru import logger

from daft import filesystem
from daft.runners.blocks import ArrowDataBlock


def download(url_block: ArrowDataBlock) -> List[Optional[bytes]]:
    assert isinstance(
        url_block, ArrowDataBlock
    ), f"Can only download from columns containing strings, found non-arrow block"
    assert pa.types.is_string(
        url_block.data.type
    ), f"Can only download from columns containing strings, found {url_block.data.type}"

    results: List[Optional[bytes]] = [None for _ in range(len(url_block))]

    path_to_result_idx = {}
    to_download: Dict[str, List[str]] = {}
    for i, path in enumerate(url_block.iter_py()):
        if path is None:
            continue
        protocol = filesystem.get_protocol_from_path(path)

        # fsspec returns data keyed by absolute paths, so we do a conversion here to match up later
        if protocol == "file":
            path = str(pathlib.Path(path).resolve())

        if protocol not in to_download:
            to_download[protocol] = []
        to_download[protocol].append(path)
        path_to_result_idx[path] = i

    for protocol in to_download:
        fs = filesystem.get_filesystem(protocol)
        data = fs.cat(to_download[protocol], on_error="return")
        for path in data:
            if isinstance(data[path], bytes):
                results[path_to_result_idx[path]] = data[path]
            else:
                logger.error(f"Encountered error during download from URL {path}: {str(data[path])}")
                results[path_to_result_idx[path]] = None

    return results
