from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from daft import filesystem
from daft.datatype import DataType
from daft.series import Series
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


def _warmup_fsspec_registry(urls_pylist: list[str | None]) -> None:
    """HACK: filesystem.get_filesystem calls fsspec.get_filesystem_class under the hood, which throws an error
    if accessed concurrently for the first time. We "warm" it up in a single-threaded fashion here

    This should be fixed in the next release of FSSpec
    See: https://github.com/Eventual-Inc/Daft/issues/892
    """
    import fsspec

    protocols = {filesystem.get_protocol_from_path(url) for url in urls_pylist if url is not None}
    for protocol in protocols:
        fsspec.get_filesystem_class(protocol)


@udf(return_dtype=DataType.binary())
def download_udf(urls, max_worker_threads: int = 8):
    """Downloads the contents of the supplied URLs."""

    from loguru import logger

    urls_pylist = urls.to_arrow().to_pylist()

    _warmup_fsspec_registry(urls_pylist)

    executor = ThreadPoolExecutor(max_workers=max_worker_threads, initializer=_worker_thread_initializer)
    results: list[bytes | None] = [None for _ in range(len(urls))]
    future_to_idx = {executor.submit(_download, urls_pylist[i]): i for i in range(len(urls))}
    for future in as_completed(future_to_idx):
        try:
            results[future_to_idx[future]] = future.result()
        except Exception as e:
            logger.error(f"Encountered error during download from URL {urls_pylist[future_to_idx[future]]}: {str(e)}")

    return Series.from_pylist(results)
