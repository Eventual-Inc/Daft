from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from daft.convert import from_pydict
from daft.datatype import DataType
from daft.expressions import col
from daft.functions import cast, contains, decompress, download, explode, split
from daft.io import read_warc

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.io import IOConfig


def _get_manifest_path(crawl: str, file_type: Literal["warc", "wet", "wat"]) -> str:
    return f"s3://commoncrawl/crawl-data/{crawl}/{file_type}.paths.gz"


def _get_common_crawl_paths(
    crawl: str,
    segment: str | None,
    file_type: Literal["warc", "wet", "wat"],
    num_files: int | None,
) -> list[str]:
    """Get the paths to the Common Crawl files for a given crawl, segment, file type. Limited by `num_files`."""
    paths_url = _get_manifest_path(crawl, file_type)

    # The manifest file is a gzipped plaintext file with one path per line.
    # Technically, this is equivalent to a CSV file with one column, "url", with no headers, and we could use read_csv.
    # But from a preliminary microbenchmark on a local machine, this approach turns out to be 20-30% faster than read_csv.
    paths = from_pydict({"url": [paths_url]}).select(
        explode(split(cast(decompress(download(col("url")), codec="gzip"), DataType.string()), "\n"))
    )

    if segment is not None:
        paths = paths.where(contains(col("url"), segment))

    if num_files is not None:
        paths = paths.limit(num_files)

    path_list = paths.select("url").to_pydict()["url"]

    full_urls = []
    for path in path_list:
        if path:
            # The paths in paths.gz are relative, so we need to construct full URLs.
            # They look like: crawl-data/CC-MAIN-2025-38/segments/1757047533033.70/warc/CC-MAIN-20250909055722-20250909085722-00031.warc.gz
            full_urls.append(f"s3://commoncrawl/{path}")

    return full_urls


def common_crawl(
    crawl: str,
    segment: str | None = None,
    content: Literal["raw", "text", "metadata", "warc", "wet", "wat"] = "raw",
    num_files: int | None = None,
    io_config: IOConfig | None = None,
) -> DataFrame:
    """Load Common Crawl data as a DataFrame.

    This function automatically resolves the specified crawl and segment into the appropriate Common Crawl files
    and loads them as a DataFrame, handling the WARC reading process internally.

    Args:
        crawl: The crawl identifier, e.g. "CC-MAIN-2025-33".
        segment: Specific segment to fetch within the crawl. If not provided, defaults to all segments in the crawl.
        content: Specifies the type of content to load. Options are:
            - "raw" or "warc": Raw WARC files containing full HTTP responses
            - "text" or "wet": Extracted plain text content
            - "metadata" or "wat": Metadata about crawled pages
        num_files: Limit the number of files to process. If not provided, processes all matching files.
        io_config: IO configuration for accessing S3.

    Returns:
        A DataFrame containing the requested Common Crawl data.

    Examples:
        >>> # Load raw WARC data from a specific crawl
        >>> daft.datasets.common_crawl("CC-MAIN-2025-33")  # doctest: +SKIP

        >>> # Load extracted text content
        >>> daft.datasets.common_crawl("CC-MAIN-2025-33", content="text")  # doctest: +SKIP

        >>> # Load data from a specific segment with file limit for sampling/testing
        >>> daft.datasets.common_crawl("CC-MAIN-2025-33", segment="1234567890.1", num_files=1)  # doctest: +SKIP
    """
    if num_files is not None and num_files <= 0:
        raise ValueError("num_files must be a positive integer")

    content_type_map: dict[str, Literal["warc", "wet", "wat"]] = {
        "raw": "warc",
        "text": "wet",
        "metadata": "wat",
        "warc": "warc",
        "wet": "wet",
        "wat": "wat",
    }
    if content not in content_type_map:
        raise ValueError(f"Invalid content type for daft.datasets.common_crawl: {content}")
    file_type = content_type_map[content]

    warc_paths = _get_common_crawl_paths(
        crawl=crawl,
        segment=segment,
        file_type=file_type,
        num_files=num_files,
    )

    return read_warc(warc_paths, io_config=io_config)
