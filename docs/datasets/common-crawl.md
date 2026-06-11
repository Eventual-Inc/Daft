# How to use Common Crawl with Daft

[Common Crawl](https://commoncrawl.org/get-started) is one of the most important open web datasets, containing more than 250 billion web pages that span 18 years of crawls. Since 2020, it has become a critical source of training data for generative AI, with the vast majority of data used to train models like GPT-3 coming from Common Crawl. [Mozilla Foundation's research](https://www.mozillafoundation.org/en/research/library/generative-ai-training-data/common-crawl/) noted that "Generative AI in its current form would probably not be possible without Common Crawl".

Daft provides a simple, performant, and responsible way to access Common Crawl data.

!!! warning "Warning"

    These APIs are in beta and may be subject to change as the Common Crawl dataset continues to be developed.

## Access from Hugging Face Buckets

As of 2025, Common Crawl data is also hosted on [Hugging Face Buckets](https://huggingface.co/buckets/commoncrawl), providing a cheaper and more accessible alternative to AWS S3. Unlike S3, which restricts access to AWS machines in the same region (and charges inter-region egress fees), HF Buckets are accessible from any cloud provider and region using the ``hf://`` protocol.

When using `daft.datasets.common_crawl`, set ``hf_buckets=True`` to fetch data from HF Buckets. This is the **recommended** option for most users, especially when running outside AWS.

### Reading with the Common Crawl dataset helper

```python
import daft

# Read from HF Buckets (recommended)
daft.datasets.common_crawl("CC-MAIN-2025-33", hf_buckets=True)
```

### Reading WARC files directly

You can also read WARC files directly from HF Buckets using the ``hf://`` protocol:

```python
import daft

# Read a single WARC file from HF Buckets
daft.read_warc("hf://buckets/commoncrawl/commoncrawl/crawl-data/CC-MAIN-2026-17/segments/1775805908305.14/warc/CC-MAIN-20260410081153-20260410111153-00000.warc.gz")

# Read WARC files matching a glob pattern
daft.read_warc("hf://buckets/commoncrawl/commoncrawl/crawl-data/CC-MAIN-2026-17/segments/*/warc/*.warc.gz")
```

### Authentication

For public Common Crawl data, no authentication is needed. For private Hugging Face repos or datasets, pass a token via ``IOConfig``:

```python
from daft.io import IOConfig

daft.read_warc(
    "hf://buckets/commoncrawl/commoncrawl/crawl-data/CC-MAIN-2025-33/segments/...",
    io_config=IOConfig(hf=daft.io.HuggingFaceConfig(token="hf_xxxxxxxxxxxxxxxxxxxx"))
)
```

You can also authenticate using ``hf auth login`` — the token is picked up automatically.

### Exploring HF Buckets

Use the [`huggingface_hub`](https://huggingface.co/docs/huggingface_hub) library to explore available data:

```python
from huggingface_hub import hffs

# List crawl archives
for path in hffs.ls('hf://buckets/commoncrawl/commoncrawl/crawl-data/'):
    print(path)

# List segments in a specific crawl
for path in hffs.ls('hf://buckets/commoncrawl/commoncrawl/crawl-data/CC-MAIN-2026-17/'):
    print(path)

# List WARC files in a segment
for path in hffs.ls('hf://buckets/commoncrawl/commoncrawl/crawl-data/CC-MAIN-2026-17/segments/1775805908305.14/warc/'):
    print(path)
```

**Note:** When using ``hffs.ls()``, paths are returned without the ``hf://`` prefix. To open files returned by ``ls()``, prepend the protocol:

```python
files = hffs.ls('hf://buckets/commoncrawl/commoncrawl/crawl-data/CC-MAIN-2026-17/segments/1775805908305.14/warc/')
for file_path in files[:1]:
    with daft.file.open_file('hf://' + file_path, 'rb') as fh:
        # ... read the WARC file
        ...
```

### Access Method Comparison

| Method | URL Scheme | Best For | Credentials Required |
|--------|-----------|----------|---------------------|
| Hugging Face Buckets | ``hf://buckets/...`` | Cross-region, outside AWS, cheapest | No (public) |
| AWS S3 | ``s3://commoncrawl/...`` | Inside AWS (us-east-1) | Yes |
| HTTPS | ``https://data.commoncrawl.org/...`` | Fallback when no credentials | No |

Common Crawl path structure on HF Buckets: ``hf://buckets/commoncrawl/commoncrawl/crawl-data/<crawl-name>/<path>``

## Prerequisites for access within the AWS Cloud

Common Crawl data is hosted by [Amazon Web Services' Open Data Sets Sponsorships program](https://aws.amazon.com/opendata/) which makes it freely accessible.
However, access does require AWS authentication when downloading Common Crawl data from S3 directly. (_Outside of AWS, you can access Common Crawl without an AWS account_).

**NOTE**: When using `daft.datasets.common_crawl`, you _must_ provide `in_aws=True` when accessing data within the AWS Cloud!

All Common Crawl data is stored in the `us-east-1` region. It's recommended to access the data from that same region. From the [Common Crawl website](https://commoncrawl.org/get-started):

> The connection to S3 should be faster and you avoid the minimal fees for inter-region data transfer (you have to send requests which are charged as outgoing traffic).

!!! warning

    Using S3 from outside AWS incurs data transfer egress fees. Consider using [Hugging Face Buckets](#access-from-hugging-face-buckets) as a cheaper alternative.

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

If you are running _outside_ of AWS, we recommend using [Hugging Face Buckets](#access-from-hugging-face-buckets) (`hf_buckets=True`) as the optimal access method.

As a fallback, you can also use HTTPS links (slower but no credentials required):

```python
import daft

# Use HTTPS as a fallback (slower than HF Buckets)
daft.datasets.common_crawl("CC-MAIN-2025-33", in_aws=False)
```

## Quickstart

The simplest way to get started with Common Crawl is to load a small sample of data:

```python
import daft

# Read from Hugging Face Buckets (recommended - free, fast from anywhere)
daft.datasets.common_crawl("CC-MAIN-2025-33", num_files=1, hf_buckets=True).show()
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
daft.datasets.common_crawl("CC-MAIN-2025-33", content="raw", hf_buckets=True)
# or equivalently
daft.datasets.common_crawl("CC-MAIN-2025-33", content="warc", hf_buckets=True)
```

**Extracted text, aka WET files** - Plain text content extracted from web pages:

```python
# Extracted text content
daft.datasets.common_crawl("CC-MAIN-2025-33", content="text", hf_buckets=True)
# or equivalently
daft.datasets.common_crawl("CC-MAIN-2025-33", content="wet", hf_buckets=True)
```

**Metadata, aka WAT files** - Information about crawled pages without content:

```python
# Metadata only
daft.datasets.common_crawl("CC-MAIN-2025-33", content="metadata", hf_buckets=True)
# or equivalently
daft.datasets.common_crawl("CC-MAIN-2025-33", content="wat", hf_buckets=True)
```

### Loading a subset of data

For quick testing and development, it's helpful to limit the number of crawl files accessed:

```python
# Process only 1 crawl file for testing
daft.datasets.common_crawl("CC-MAIN-2025-33", num_files=1, hf_buckets=True)
```

### Working with specific segments

Each crawl is split into 100 segments. You can target a specific segment:

```python
daft.datasets.common_crawl(
    "CC-MAIN-2025-33",
    segment="1754151279521.11",
    hf_buckets=True,
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
    daft.datasets.common_crawl("CC-MAIN-2025-33", num_files=1, hf_buckets=True)
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
    daft.datasets.common_crawl("CC-MAIN-2025-33", content="text", num_files=1, hf_buckets=True)
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
