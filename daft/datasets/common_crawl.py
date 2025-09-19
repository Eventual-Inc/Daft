from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from daft.io import from_glob_path

if TYPE_CHECKING:
    from daft.io import IOConfig


def common_crawl(
    crawl: str,
    segment: str | None = None,
    content: Literal["raw", "text", "metadata", "warc", "wet", "wat"] = "raw",
    num_files: int | None = None,
    io_config: IOConfig | None = None,
) -> str | list[str]:
    """A helper that resolves crawl dates and segment strings into a list of common crawl URLs.

    Args:
        crawl: The crawl date, e.g. "CC-MAIN-2025-33".
        segment: Specific segment to fetch within the crawl. If not provided, defaults to all segments in the crawl.
        content: Specifies whether to return the WARC, WET, or WAT files. "raw" = warc, "text" = wet, "metadata" = wat.
        num_files: Limit the number of files returned.
        io_config: IO configuration for accessing S3.

    Returns:
        List of Common Crawl data URLs corresponding to the given crawl date and segment(s).

    Examples:
        >>> daft.read_warc(common_crawl("CC-MAIN-2025-33")) + SKIP
        >>> daft.read_warc(common_crawl("CC-MAIN-2025-33", content="text")) + SKIP
        >>> daft.read_warc(common_crawl("CC-MAIN-2025-33", segment="1234567890.1", num_files=1)) + SKIP
    """
    if num_files is not None and num_files <= 0:
        raise ValueError("num_files must be a positive integer")

    content_type_map = {"raw": "warc", "text": "wet", "metadata": "wat", "warc": "warc", "wet": "wet", "wat": "wat"}
    if content not in content_type_map:
        raise ValueError(f"Invalid content type for daft.datasets.common_crawl: {content}")
    file_type = content_type_map[content]

    extension_map = {"warc": "*.warc.gz", "wet": "*.warc.wet.gz", "wat": "*.warc.wat.gz"}
    file_extension = extension_map[file_type]

    segment_glob = "**"
    if segment is not None:
        segment_glob = segment

    glob_pattern = f"s3://commoncrawl/crawl-data/{crawl}/segments/{segment_glob}/{file_type}/{file_extension}"

    if num_files is not None:
        return from_glob_path(glob_pattern, io_config=io_config).select("path").limit(num_files).to_pydict()["path"]

    return glob_pattern
