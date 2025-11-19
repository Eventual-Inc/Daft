from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from daft.convert import from_pydict
from daft.datatype import DataType
from daft.expressions import col
from daft.functions import cast, contains, decompress, download, format, split
from daft.io import read_warc

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.io import IOConfig


__all__: tuple[str, ...] = ("common_crawl",)


def _get_s3_manifest_path(crawl: str, file_type: Literal["warc", "wet", "wat"]) -> str:
    return f"s3://commoncrawl/crawl-data/{crawl}/{file_type}.paths.gz"


def _get_http_manifest_path(crawl: str, file_type: Literal["warc", "wet", "wat"]) -> str:
    return f"https://data.commoncrawl.org/crawl-data/{crawl}/{file_type}.paths.gz"


def _unique_cc_file_paths(paths_url: str, io_config: IOConfig | None) -> DataFrame:
    # The manifest file is a gzipped plaintext file with one path per line.
    # Technically, this is equivalent to a CSV file with one column, "url", with no headers, and we could use read_csv.
    # But from a preliminary microbenchmark on a local machine, this approach turns out to be 20-30% faster than read_csv.
    paths = from_pydict({"url": [paths_url]}).select(
        split(cast(decompress(download(col("url"), io_config=io_config), codec="gzip"), DataType.string()), "\n")
    )

    # now, paths is just the list of CC files -- no s3/http protocol nor other things
    # *just* the unique parts of each file
    paths = paths.explode("url")

    return paths


def _get_common_crawl_paths(
    crawl: str,
    segment: str | None,
    file_type: Literal["warc", "wet", "wat"],
    num_files: int | None,
    io_config: IOConfig | None,
    *,
    in_aws: bool,
) -> list[str]:
    """Get the paths to the Common Crawl files for a given crawl, segment, file type. Limited by `num_files`."""
    if in_aws:
        paths_url = _get_s3_manifest_path(crawl, file_type)
    else:
        paths_url = _get_http_manifest_path(crawl, file_type)

    paths = _unique_cc_file_paths(paths_url, io_config)

    if in_aws:
        paths = paths.select(format("s3://commoncrawl/{}", col("url")).alias("url"))
    else:
        paths = paths.select(format("https://data.commoncrawl.org/{}", col("url")).alias("url"))

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
    in_aws: bool,
) -> DataFrame:
    r"""Load Common Crawl data as a DataFrame.

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
        in_aws: Where to fetch the common crawl data from. If running in AWS, this must be set to True. If outside of AWS,
                then this must be set to False. Setting this flag correctly is required for **optimal download performance**.
                If running in AWS, then make sure you're in the "us-east-1" region so you don't incur S3 egress fees!
                See [the Common Crawl docs](https://commoncrawl.org/get-started) for more specific instructions.

    Returns:
        A DataFrame containing the requested Common Crawl data.

    Examples:
        >>> # Create a dataframe from raw WARC data from a specific crawl
        >>> daft.datasets.common_crawl("CC-MAIN-2025-33", in_aws=True)  # doctest: +SKIP
        ╭────────────────┬─────────────────┬───────────┬────────────────────┬────────────┬────────────────────┬──────────────┬──────────────╮
        │ WARC-Record-ID ┆ WARC-Target-URI ┆ WARC-Type ┆ WARC-Date          ┆      …     ┆ WARC-Identified-Pa ┆ warc_content ┆ warc_headers │
        │ ---            ┆ ---             ┆ ---       ┆ ---                ┆            ┆ yload-Type         ┆ ---          ┆ ---          │
        │ String         ┆ String          ┆ String    ┆ Timestamp[ns,      ┆ (1 hidden) ┆ ---                ┆ Binary       ┆ String       │
        │                ┆                 ┆           ┆ "Etc/UTC"]         ┆            ┆ String             ┆              ┆              │
        ╰────────────────┴─────────────────┴───────────┴────────────────────┴────────────┴────────────────────┴──────────────┴──────────────╯
        <BLANKLINE>
        (No data to display: Dataframe not materialized, use .collect() to materialize)

        >>> # Show a sample of extracted text content
        >>> daft.datasets.common_crawl("CC-MAIN-2025-33", content="text", in_aws=True).limit(2).show()  # doctest: +SKIP
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
        ...     daft.datasets.common_crawl("CC-MAIN-2025-33", segment="1754151579063.98", num_files=1, in_aws=True)
        ...     .limit(2)
        ...     .show()
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

    warc_paths = _get_common_crawl_paths(
        crawl=crawl,
        segment=segment,
        file_type=file_type,
        num_files=num_files,
        io_config=io_config,
        in_aws=in_aws,
    )

    return read_warc(warc_paths, io_config=io_config)
