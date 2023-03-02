from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

from daft import filesystem
from daft.udf import udf

thread_local = threading.local()


def _worker_thread_initializer() -> None:
    """Initializes per-thread local state"""
    thread_local.filesystems_cache = {}


def _download(path: str | None) -> bytes | None:
    if path is None:
        return None
    protocol = filesystem.get_protocol_from_path(path)
    fs = thread_local.filesystems_cache.get(protocol, None)
    if fs is None:
        fs = filesystem.get_filesystem(protocol)
        thread_local.filesystems_cache[protocol] = fs
    return fs.cat_file(path)


@udf(return_dtype=bytes, input_columns={"urls": List[Optional[str]]})
def download_udf(urls: list[str | None], max_worker_threads: int = 8) -> list[bytes | None]:
    """Downloads the contents of the supplied URLs."""

    from loguru import logger

    results: list[bytes | None] = []

    executor = ThreadPoolExecutor(max_workers=max_worker_threads, initializer=_worker_thread_initializer)
    results = [None for _ in range(len(urls))]
    future_to_idx = {executor.submit(_download, urls[i]): i for i in range(len(urls))}
    for future in as_completed(future_to_idx):
        try:
            results[future_to_idx[future]] = future.result()
        except Exception as e:
            logger.error(f"Encountered error during download from URL {urls[future_to_idx[future]]}: {str(e)}")

    return results
