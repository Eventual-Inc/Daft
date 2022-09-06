from typing import Dict, List

import pyarrow as pa

from daft import filesystem
from daft.runners.blocks import ArrowDataBlock, DataBlock


def download(url_block: ArrowDataBlock) -> DataBlock:
    assert isinstance(
        url_block, ArrowDataBlock
    ), f"Can only download from columns containing strings, found non-arrow block"
    assert pa.types.is_string(
        url_block.data.type
    ), f"Can only download from columns containing strings, found {url_block.data.type}"

    results = [None for _ in range(len(url_block))]

    path_to_result_idx = {}
    to_download: Dict[str, List[str]] = {}
    for i, path in enumerate(url_block.iter_py()):
        if path is None:
            continue
        protocol = filesystem.get_protocol_from_path(path)
        if protocol not in to_download:
            to_download[protocol] = []
        to_download[protocol].append(path)
        path_to_result_idx[path] = i

    for protocol in to_download:
        fs = filesystem.get_filesystem(protocol)
        data = fs.cat(to_download[protocol])
        for path in data:
            results[path_to_result_idx[path]] = data[path]

    return DataBlock.make_block(results)
