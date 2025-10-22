# How to use Common Crawl with Daft

[Common Crawl](https://commoncrawl.org/get-started) is one of the most important open web datasets, containing more than 250 billion web pages that span 18 years of crawls. Since 2020, it has become a critical source of training data for generative AI, with the vast majority of data used to train models like GPT-3 coming from Common Crawl. [Mozilla Foundation's research](https://www.mozillafoundation.org/en/research/library/generative-ai-training-data/common-crawl/) noted that "Generative AI in its current form would probably not be possible without Common Crawl".

Daft provides a simple, performant, and responsible way to access Common Crawl data.

!!! warning "Warning"

    These APIs are in beta and may be subject to change as the Common Crawl dataset continues to be developed.

## Prerequisites for access within the AWS Cloud

Common Crawl data is hosted by [Amazon Web Services' Open Data Sets Sponsorships program](https://aws.amazon.com/opendata/) which makes it freely accessible.
However, access does require AWS authentication when downloading Common Crawl data from S3 directly. (_Outside of AWS, you can access Common Crawl without an AWS account_).

**NOTE**: When using `daft.datasets.common_crawl`, you _must_ provide `in_aws=True` when accessing data within the AWS Cloud!

All Common Crawl data is stored in the `us-east-1` region. It's recommended to access the data from that same region. From the [Common Crawl website](https://commoncrawl.org/get-started):

> The connection to S3 should be faster and you avoid the minimal fees for inter-region data transfer (you have to send requests which are charged as outgoing traffic).

### Authentication option 1: AWS SSO Login

```bash
aws sso login
```

If your environment has AWS credentials configured, Daft will automatically detect and use them.

### Authentication option 2: Configure IOConfig

```python
import daft
from daft.io import IOConfig, S3Config

io_config = IOConfig(
    s3=S3Config(
        key_id="your_access_key",
        access_key="your_secret_key",
        session_token="your_session_token",
        region_name="us-east-1",  # Access Common Crawl data where it's located.
    )
)

# Use io_config when reading from the Common Crawl dataset
daft.datasets.common_crawl("CC-MAIN-2025-33", io_config=io_config, in_aws=True)
# NOTE: When using `daft.datasets.common_crawl`, you _must_ provide `in_aws=True` when accessing data within the AWS Cloud!
```

## Prerequisites for access outside the AWS Cloud

If you are running _outside_ of AWS, then the most optimal way to download Common Crawl data is to use their HTTPS links.
From the [Common Crawl website](https://commoncrawl.org/get-started):

> If you want to download the data to your local machine or local cluster, you can use any HTTP download agent, such as cURL or wget.

**NOTE**: When using `daft.datasets.common_crawl`, you _must_ provide `in_aws=False` when accessing data outside the AWS Cloud!

Here's an example of how to use Common Crawl with Daft when outside of AWS:

```python
import daft

daft.datasets.common_crawl("CC-MAIN-2025-33", in_aws=False)
```

## Quickstart

The simplest way to get started with Common Crawl is to load a small sample of data:

```python
import daft

# If you are running this code locally, set `in_aws = True`. This will use S3.
# Otherwise, set `in_aws = False`. This will use HTTPS URLs for the files.
# You must **explicitly** set the `in_aws` parameter.
in_aws: bool = ...


# Load a sample of raw WARC data from the CC-MAIN-2025-33 crawl
daft.datasets.common_crawl("CC-MAIN-2025-33", num_files=1, in_aws=in_aws).show()
```

```{title="Output"}
╭────────────────────────────────┬────────────────────────────────┬───────────┬─────────────────────────────────────────┬────────────────┬──────────────────────────────┬────────────────────────────────┬────────────────────────────────╮
│ WARC-Record-ID                 ┆ WARC-Target-URI                ┆ WARC-Type ┆ WARC-Date                               ┆ Content-Length ┆ WARC-Identified-Payload-Type ┆ warc_content                   ┆ warc_headers                   │
│ ---                            ┆ ---                            ┆ ---       ┆ ---                                     ┆ ---            ┆ ---                          ┆ ---                            ┆ ---                            │
│ Utf8                           ┆ Utf8                           ┆ Utf8      ┆ Timestamp(Nanoseconds, Some("Etc/UTC")) ┆ Int64          ┆ Utf8                         ┆ Binary                         ┆ Utf8                           │
╞════════════════════════════════╪════════════════════════════════╪═══════════╪═════════════════════════════════════════╪════════════════╪══════════════════════════════╪════════════════════════════════╪════════════════════════════════╡
│ 526c37b2-f535-4015-b8dd-bfa8e… ┆ None                           ┆ warcinfo  ┆ 2025-08-02 22:09:07 UTC                 ┆ 489            ┆ None                         ┆ b"isPartOf: CC-MAIN-2025-33\r… ┆ {"Content-Type":"application/… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ f99237da-09e9-4bf0-838e-826e2… ┆ http://0014housingrental.shop… ┆ request   ┆ 2025-08-02 23:15:49 UTC                 ┆ 308            ┆ None                         ┆ b"GET / HTTP/1.1\r\nUser-Agen… ┆ {"Content-Type":"application/… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 77dac6f5-296e-4bdc-80f2-538c1… ┆ http://0014housingrental.shop… ┆ response  ┆ 2025-08-02 23:15:49 UTC                 ┆ 1751           ┆ text/html                    ┆ b"HTTP/1.1 200 OK\r\nDate: Sa… ┆ {"Content-Type":"application/… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ b09ed72d-7556-4d17-9e04-72a4d… ┆ http://0014housingrental.shop… ┆ metadata  ┆ 2025-08-02 23:15:49 UTC                 ┆ 94             ┆ None                         ┆ b"fetchTimeMs: 4\r\ncharset-d… ┆ {"Content-Type":"application/… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 6021bff5-d623-498a-abcb-640c6… ┆ http://010ganji.com/html/ying… ┆ request   ┆ 2025-08-02 23:06:24 UTC                 ┆ 293            ┆ None                         ┆ b"GET /html/yingjianchanpin/c… ┆ {"Content-Type":"application/… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 4b001100-08a0-4afc-8e35-634fa… ┆ http://010ganji.com/html/ying… ┆ response  ┆ 2025-08-02 23:06:24 UTC                 ┆ 21130          ┆ text/html                    ┆ b"HTTP/1.1 200 OK\r\nDate: Sa… ┆ {"Content-Type":"application/… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 4855bde6-21c5-45f9-bfa2-bd50f… ┆ http://010ganji.com/html/ying… ┆ metadata  ┆ 2025-08-02 23:06:24 UTC                 ┆ 201            ┆ None                         ┆ b"fetchTimeMs: 233\r\ncharset… ┆ {"Content-Type":"application/… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 4653985e-0446-47bc-8f55-bfe65… ┆ http://01dom.ru/sale/prodleni… ┆ request   ┆ 2025-08-02 22:29:13 UTC                 ┆ 349            ┆ None                         ┆ b"GET /sale/prodlenie_aktsii_… ┆ {"Content-Type":"application/… │
╰────────────────────────────────┴────────────────────────────────┴───────────┴─────────────────────────────────────────┴────────────────┴──────────────────────────────┴────────────────────────────────┴────────────────────────────────╯
```

<!-- TODO: we should provide an easy way for users to list crawls available. -->

## Basic usage

### Loading different content types (WARC, WET, WAT)

Common Crawl provides three types of content:

**Raw [Web ARChive (WARC)](<https://en.wikipedia.org/wiki/WARC_(file_format)>) files (default)** - Full HTTP responses with headers and content:

```python
# Raw WARC data (default)
daft.datasets.common_crawl("CC-MAIN-2025-33", content="raw", in_aws=in_aws)
# or equivalently
daft.datasets.common_crawl("CC-MAIN-2025-33", content="warc", in_aws=in_aws)
```

**Extracted text, aka WET files** - Plain text content extracted from web pages:

```python
# Extracted text content
daft.datasets.common_crawl("CC-MAIN-2025-33", content="text", in_aws=in_aws)
# or equivalently
daft.datasets.common_crawl("CC-MAIN-2025-33", content="wet", in_aws=in_aws)
```

**Metadata, aka WAT files** - Information about crawled pages without content:

```python
# Metadata only
daft.datasets.common_crawl("CC-MAIN-2025-33", content="metadata", in_aws=in_aws)
# or equivalently
daft.datasets.common_crawl("CC-MAIN-2025-33", content="wat", in_aws=in_aws)
```

### Loading a subset of data

For quick testing and development, it's helpful to limit the number of crawl files accessed:

```python
# Process only 1 crawl file for testing
daft.datasets.common_crawl("CC-MAIN-2025-33", num_files=1, in_aws=in_aws)
```

### Working with specific segments

Each crawl is split into 100 segments. You can target a specific segment:

```python
daft.datasets.common_crawl(
    "CC-MAIN-2025-33",
    segment="1754151279521.11",
    in_aws=in_aws,
)
```

## Data schema

Daft's Common Crawl dataset includes these key columns:

| Column                         | Type      | Description                                        |
| ------------------------------ | --------- | -------------------------------------------------- |
| `WARC-Record-ID`               | String    | Unique identifier for each WARC record             |
| `WARC-Target-URI`              | String    | The URL that was crawled                           |
| `WARC-Type`                    | String    | Type of record (response, request, warcinfo, etc.) |
| `WARC-Date`                    | Timestamp | When the page was crawled                          |
| `WARC-Identified-Payload-Type` | String    | MIME type of the content                           |
| `warc_content`                 | Binary    | The actual content (HTML, text, etc.)              |
| `warc_headers`                 | String    | All WARC record headers as JSON                    |

For more details on the WARC file format, check out the [WARC specification](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/).

## Examples

### Analyzing content types

Find the most common MIME types in a crawl:

```python
(
    daft.datasets.common_crawl("CC-MAIN-2025-33", num_files=1, in_aws=in_aws)
    .select(daft.col("WARC-Identified-Payload-Type"))
    .groupby("WARC-Identified-Payload-Type")
    .agg(daft.col("WARC-Identified-Payload-Type").count().alias("count"))
    .sort("count", desc=True)
    .show()
)
```

```{title="Output"}
╭──────────────────────────────┬────────╮
│ WARC-Identified-Payload-Type ┆ count  │
│ ---                          ┆ ---    │
│ Utf8                         ┆ UInt64 │
╞══════════════════════════════╪════════╡
│ text/html                    ┆ 21907  │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ application/xhtml+xml        ┆ 2063   │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ application/pdf              ┆ 143    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ application/atom+xml         ┆ 28     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ text/plain                   ┆ 23     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ application/rss+xml          ┆ 14     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ application/xml              ┆ 14     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ text/calendar                ┆ 7      │
╰──────────────────────────────┴────────╯
```

### Extracting text for language models

Content in Common Crawl WARC files are UTF-8 encoded. Use Daft's [try_decode][daft.functions.try_decode] function to extract clean text content for training:

```python
from daft.functions import try_decode

(
    daft.datasets.common_crawl("CC-MAIN-2025-33", content="text", num_files=1, in_aws=in_aws)
    .with_column("text_content", try_decode(daft.col("warc_content"), charset="utf-8"))
    .where(daft.col("text_content").not_null())
    .select("WARC-Target-URI", "text_content")
    .limit(3)
    .show()
)

```

```{title="Output"}
╭────────────────────────────────┬──────────────────────────────────────────────────╮
│ WARC-Target-URI                ┆ text_content                                     │
│ ---                            ┆ ---                                              │
│ Utf8                           ┆ Utf8                                             │
╞════════════════════════════════╪══════════════════════════════════════════════════╡
│ None                           ┆ Software-Info: ia-web-commons…                   │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ http://010ganji.com/html/ying… ┆ ETF选择困难？易方达基金划分四大类助您轻松投资！_ │
│                                ┆ 首页…                                            │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ http://01dom.ru/sale/prodleni… ┆ Скидка до 23% на керамические…                   │
╰────────────────────────────────┴──────────────────────────────────────────────────╯
```

## Next steps

- If you want a runnable example of using this dataset and embedding text using Qwen3, take a look at our [Getting Started with Common Crawl in Daft](../examples/common-crawl-daft-tutorial.md) tutorial.
- When using datasets like Common Crawl for pre-training, content deduplication is essential for model performance.
  Check out our [MinHash deduplication example](../examples/minhash-dedupe.md) to see how this can be done in Daft.
- See the [Common Crawl Dataset API reference](../api/datasets.md#common-crawl) for complete parameter documentation
