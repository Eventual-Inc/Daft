from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Literal

from daft import filesystem
from daft.datatype import DataType
from daft.series import Series
from daft.udf import udf

thread_local = threading.local()


logger = logging.getLogger(__name__)


def _worker_thread_initializer() -> None:
    """Initializes per-thread local state"""
    thread_local.filesystems_cache = {}


def _download(path: str | None, on_error: Literal["raise"] | Literal["null"]) -> bytes | None:
    if path is None:
        return None
    protocol = filesystem.get_protocol_from_path(path)

    # If no fsspec filesystem provided, first check the cache.
    # If none in the cache, create one based on the path protocol.
    fs = thread_local.filesystems_cache.get(protocol, None)
    fs = filesystem.get_filesystem(protocol)
    thread_local.filesystems_cache[protocol] = fs

    try:
        return fs.cat_file(path)
    except Exception as e:
        if on_error == "raise":
            raise
        elif on_error == "null":
            logger.error(
                "Encountered error during download from URL %s and falling back to Null\n\n%s: %s", path, e, str(e)
            )
            return None
        else:
            raise NotImplementedError(f"Unimplemented on_error option: {on_error}.\n\nEncountered error: {e}")


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
def download_udf(
    urls,
    max_worker_threads: int = 8,
    on_error: Literal["raise"] | Literal["null"] = "raise",
):
    """Downloads the contents of the supplied URLs.

    Args:
        urls: URLs as a UTF8 string series
        max_worker_threads: max number of worker threads to use, defaults to 8
        on_error: Behavior when a URL download error is encountered - "raise" to raise the error immediately or "null" to log
            the error but fallback to a Null value. Defaults to "raise".
        fs (fsspec.AbstractFileSystem): fsspec FileSystem to use for downloading data.
            By default, Daft will automatically construct a FileSystem instance internally.
    """

    urls_pylist = urls.to_arrow().to_pylist()

    _warmup_fsspec_registry(urls_pylist)

    executor = ThreadPoolExecutor(max_workers=max_worker_threads, initializer=_worker_thread_initializer)
    results: list[bytes | None] = [None for _ in range(len(urls))]
    future_to_idx = {executor.submit(_download, urls_pylist[i], on_error): i for i in range(len(urls))}
    for future in as_completed(future_to_idx):
        results[future_to_idx[future]] = future.result()

    return Series.from_pylist(results)
