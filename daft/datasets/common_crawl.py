from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Literal

import daft
from daft.expressions import col
from daft.functions import contains, format
from daft.io import read_warc

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.io import IOConfig


def _get_common_crawl_paths(
    crawl: str,
    segment: str | None,
    file_type: Literal["warc", "wet", "wat"],
    num_files: int | None,
    io_config: IOConfig | None,
    source: Literal["s3", "hf", "http"] | None,
) -> list[str]:
    """Get the paths to the Common Crawl files for a given crawl, segment, file type. Limited by `num_files`."""
    if source == "s3":
        paths_url = f"s3://commoncrawl/crawl-data/{crawl}/{file_type}.paths.gz"
        prefix = "s3://commoncrawl/"
    elif source == "hf" or source is None:
        paths_url = f"hf://buckets/commoncrawl/commoncrawl/crawl-data/{crawl}/{file_type}.paths.gz"
        prefix = "hf://buckets/commoncrawl/commoncrawl/"
    else:
        paths_url = f"https://data.commoncrawl.org/crawl-data/{crawl}/{file_type}.paths.gz"
        prefix = "https://data.commoncrawl.org/"

    paths = daft.read_text(paths_url, io_config=io_config).select(format(f"{prefix}{{}}", col("url")).alias("url"))

    if segment is not None:
        paths = paths.where(contains(col("url"), segment))

    if num_files is not None:
        paths = paths.limit(num_files)

    path_list = paths.select("url").to_pydict()["url"]

    return path_list


def common_crawl(
    crawl: str,
    segment: str | None = None,
    content: Literal["raw", "text", "metadata", "warc", "wet", "wat"] = "raw",
    num_files: int | None = None,
    io_config: IOConfig | None = None,
    *,
    in_aws: bool = False,
    source: Literal["s3", "hf", "http"] | None = None,
) -> DataFrame:
    r"""Load Common Crawl data as a DataFrame.

    This function automatically resolves the specified crawl and segment into the appropriate Common Crawl files
    and loads them as a DataFrame, handling the WARC reading process internally.

    Args:
        crawl: The crawl identifier, e.g. "CC-MAIN-2025-33".
        segment: Specific segment to fetch within the crawl. If not provided, defaults to all segments in the crawl.
        content: Specifies the type of content to load. Options are:
            + "raw" or "warc": Raw WARC files containing full HTTP responses
            + "text" or "wet": Extracted plain text content
            + "metadata" or "wat": Metadata about crawled pages
        num_files: Limit the number of files to process. If not provided, processes all matching files.
        io_config: IO configuration for accessing storage.
        in_aws: DEPRECATED - please use ``source="s3"`` instead.
                Fetch from AWS S3 (default: ``s3://commoncrawl/...\`). If running in AWS, set to ``True`` for optimal
                performance. Set to ``False`` when running outside AWS to avoid S3 egress fees.
                If running in AWS, make sure you're in the "us-east-1" region.
        source: Source of the Common Crawl data. Options are:
            + "s3": AWS S3
            + "hf": HuggingFace
            + "http": HTTP
            + None: Automatically chooses HuggingFace if the crawl is available, otherwise uses HTTP. S3 is an explicit
            choice due to S3 egress fees.

    Returns:
        A DataFrame containing the requested Common Crawl data.

    Examples:
        >>> # Create a dataframe from raw WARC data from a specific crawl
        >>> daft.datasets.common_crawl("CC-MAIN-2025-33")  # doctest: +SKIP
        ╭────────────────┬─────────────────┬───────────┬────────────────────┬────────────┬────────────────────┬──────────────┬──────────────╮
        │ WARC-Record-ID ┆ WARC-Target-URI ┆ WARC-Type ┆ WARC-Date          ┆      …     ┆ WARC-Identified-Pa ┆ warc_content ┆ warc_headers │
        │ ---            ┆ ---             ┆ ---       ┆ ---                ┆            ┆ yload-Type         ┆ ---          ┆ ---          │
        │ String         ┆ String          ┆ String    ┆ Timestamp[ns,      ┆ (1 hidden) ┆ ---                ┆ Binary       ┆ String       │
        │                ┆                 ┆           ┆ "Etc/UTC"]         ┆            ┆ String             ┆              ┆              │
        ╰────────────────┴─────────────────┴───────────┴────────────────────┴────────────┴────────────────────┴──────────────┴──────────────╯
        <BLANKLINE>
        (No data to display: Dataframe not materialized, use .collect() to materialize)

        >>> # Show a sample of extracted text content
        >>> daft.datasets.common_crawl("CC-MAIN-2025-33", content="text").limit(2).show()  # doctest: +SKIP
        ╭─────────────────┬─────────────────┬────────────┬─────────────────┬────────────┬─────────────────┬────────────────┬────────────────╮
        │ WARC-Record-ID  ┆ WARC-Target-URI ┆ WARC-Type  ┆ WARC-Date       ┆      …     ┆ WARC-Identified ┆ warc_content   ┆ warc_headers   │
        │ ---             ┆ ---             ┆ ---        ┆ ---             ┆            ┆ -Payload-Type   ┆ ---            ┆ ---            │
        │ String          ┆ String          ┆ String     ┆ Timestamp[ns    ┆ (1 hidden) ┆ ---             ┆ Binary         ┆ String         │
        │                 ┆                 ┆            ┆ "Etc/UTC"]      ┆            ┆ String          ┆                ┆                │
        ╞═════════════════╪═════════════════╪════════════╪═════════════════╪════════════╪═════════════════╪════════════════╪════════════════╡
        │ 0cb039e8-d357-4 ┆ None            ┆ warcinfo   ┆ 2025-08-16      ┆ …          ┆ None            ┆ b"Software-Inf ┆ {"Content-Type │
        │ 85f-95dd-cdfdb… ┆                 ┆            ┆ 01:03:20 UTC    ┆            ┆                 ┆ o:             ┆ ":"application │
        │                 ┆                 ┆            ┆                 ┆            ┆                 ┆ ia-web-commo…  ┆ /…             │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ af55e6ef-eeda-4 ┆ http://010ganji ┆ conversion ┆ 2025-08-02      ┆ …          ┆ None            ┆ b"ETF\xe9\x80\ ┆ {"Content-Type │
        │ bf7-a599-581bc… ┆ .com/html/ying… ┆            ┆ 23:06:24 UTC    ┆            ┆                 ┆ x89\xe6\x8b\xa ┆ ":"text/plain" │
        │                 ┆                 ┆            ┆                 ┆            ┆                 ┆ 9…             ┆ ,…             │
        ╰─────────────────┴─────────────────┴────────────┴─────────────────┴────────────┴─────────────────┴────────────────┴────────────────╯
        <BLANKLINE>
        (Showing first 2 of 2 rows)

        >>> # Sample a single file from a specific segment in a crawl for testing
        >>> (
        ...     daft.datasets.common_crawl("CC-MAIN-2025-33", segment="1754151579063.98", num_files=1).limit(2).show()
        ... )  # doctest: +SKIP
        ╭─────────────────┬─────────────────┬───────────┬─────────────────┬────────────┬─────────────────┬─────────────────┬────────────────╮
        │ WARC-Record-ID  ┆ WARC-Target-URI ┆ WARC-Type ┆ WARC-Date       ┆      …     ┆ WARC-Identified ┆ warc_content    ┆ warc_headers   │
        │ ---             ┆ ---             ┆ ---       ┆ ---             ┆            ┆ -Payload-Type   ┆ ---             ┆ ---            │
        │ String          ┆ String          ┆ String    ┆ Timestamp[ns    ┆ (1 hidden) ┆ ---             ┆ Binary          ┆ String         │
        │                 ┆                 ┆           ┆ "Etc/UTC"]      ┆            ┆ String          ┆                 ┆                │
        ╞═════════════════╪═════════════════╪═══════════╪═════════════════╪════════════╪═════════════════╪═════════════════╪════════════════╡
        │ b6238b9c-8db0-4 ┆ None            ┆ warcinfo  ┆ 2025-08-15      ┆ …          ┆ None            ┆ b"isPartOf: CC- ┆ {"Content-Type │
        │ 5ac-a6ef-c3cb0… ┆                 ┆           ┆ 20:42:38 UTC    ┆            ┆                 ┆ MAIN-2025-33\r… ┆ ":"application │
        │                 ┆                 ┆           ┆                 ┆            ┆                 ┆                 ┆ /…             │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ b29da11b-5976-4 ┆ http://0.woxav. ┆ request   ┆ 2025-08-15      ┆ …          ┆ None            ┆ b"GET /forum-12 ┆ {"Content-Type │
        │ f3b-82c4-71fdd… ┆ com/forum-120-… ┆           ┆ 22:33:40 UTC    ┆            ┆                 ┆ 0-1.html HTTP/… ┆ ":"application │
        │                 ┆                 ┆           ┆                 ┆            ┆                 ┆                 ┆ /…             │
        ╰─────────────────┴─────────────────┴───────────┴─────────────────┴────────────┴─────────────────┴─────────────────┴────────────────╯
        <BLANKLINE>
        (Showing first 2 of 2 rows)
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

    if in_aws and source is not None:
        warnings.warn(
            "`daft.datasets.common_crawl`: Both keyword arguments `in_aws` and `source` were set. Currently, `in_aws` takes precedence, so `daft.datasets.common_crawl` will read from S3. `in_aws` is deprecated and will be removed in v0.9.0, so please set argument `source='s3'` instead."
        )
    elif in_aws:
        warnings.warn(
            "`daft.datasets.common_crawl`: Keyword argument `in_aws` is deprecated and will be removed in v0.9.0. Please set argument `source='s3'` instead."
        )

    if in_aws:
        source = "s3"

    warc_paths = _get_common_crawl_paths(
        crawl=crawl,
        segment=segment,
        file_type=file_type,
        num_files=num_files,
        io_config=io_config,
        source=source,
    )

    return read_warc(warc_paths, io_config=io_config)


__all__: list[str] = ["common_crawl"]
