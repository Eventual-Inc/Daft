# Working with URLs and Files

Daft provides powerful capabilities for working with URLs, file paths, and remote resources. Whether you're loading data from local files, cloud storage, or web URLs, Daft's URL and file handling makes it seamless to work with distributed data sources.

Daft supports working with:

- **Local file paths**: `file:///path/to/file`, `/path/to/file`
- **S3**: `s3://bucket/path`, `s3a://bucket/path`, `s3n://bucket/path`
- **GCS**: `gs://bucket/path`
- **Azure**: `az://container/path`, `abfs://container/path`, `abfss://container/path`
- **HTTP/HTTPS URLs**: `http://example.com/path`, `https://example.com/path`
- **Hugging Face datasets**: `hf://dataset/name`
- **Unity Catalog volumes**: `vol+dbfs:/Volumes/unity/path`

## Two Ways to Work with Files in Daft

### 1. URL Functions

URL functions are ideal when your data will fit into memory or when you need the entire file content at once. Daft provides the [`.url.*`](../api/expressions.md#daft.expressions.expressions.ExpressionUrlNamespace) method namespace with functionality for working with URL strings. For example, to download data from URLs:

<!-- todo(docs - cc): add relative path to url.download after figure out url namespace-->

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({
        "urls": [
            "https://www.google.com",
            "s3://daft-public-data/open-images/validation-images/0001eeaf4aed83f9.jpg",
        ],
    })
    df = df.with_column("data", df["urls"].url.download())
    df.collect()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.from_pydict({
        "urls": [
            "https://www.google.com",
            "s3://daft-public-data/open-images/validation-images/0001eeaf4aed83f9.jpg",
        ],
    })
    df = daft.sql("""
        SELECT
            urls,
            url_download(urls) AS data
        FROM df
    """)
    df.collect()
    ```

``` {title="Output"}

╭────────────────────────────────┬────────────────────────────────╮
│ urls                           ┆ data                           │
│ ---                            ┆ ---                            │
│ Utf8                           ┆ Binary                         │
╞════════════════════════════════╪════════════════════════════════╡
│ https://www.google.com         ┆ b"<!doctype html><html itemsc… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ s3://daft-public-data/open-im… ┆ b"\xff\xd8\xff\xe0\x00\x10JFI… │
╰────────────────────────────────┴────────────────────────────────╯

(Showing first 2 of 2 rows)
```

This works well for URLs which are HTTP paths to non-HTML files (e.g. jpeg), local filepaths or even paths to a file in an object store such as AWS S3 as well!


### 2. File Datatype

The `File` datatype is preferable when dealing with large files that don't fit in memory or when you only need to access specific portions of a file. It provides a file-like interface with random access capabilities:

=== "🐍 Python"
    ``` python
    import daft
    from daft.functions import file
    from daft.io import IOConfig, S3Config

    io_config = IOConfig(s3=S3Config(anonymous=True))

    df = daft.from_pydict(
        {
            "urls": [
                "https://www.google.com",
                "s3://daft-public-data/open-images/validation-images/0001eeaf4aed83f9.jpg",
            ],
        }
    )

    @daft.func
    def detect_file_type(file: daft.File) -> str:
        # Read just the first 12 bytes to identify file type
        with file.open() as f:
            header = f.read(12)

        # Common file signatures (magic numbers)
        if header.startswith(b"\xff\xd8\xff"):
            return "JPEG"
        elif header.startswith(b"\x89PNG\r\n\x1a\n"):
            return "PNG"
        elif header.startswith(b"GIF87a") or header.startswith(b"GIF89a"):
            return "GIF"
        elif header.startswith(b"<!") or header.startswith(b"<html"):
            return "HTML"
        elif header.startswith(b"HTTP/"):
            return "HTTP"
        else:
            return None

    df = df.with_column(
        "file_type",
        detect_file_type(file(df["urls"], io_config=io_config))
    )

    df.collect()
    ```

``` {title="Output"}
╭────────────────────────────────┬───────────╮
│ urls                           ┆ file_type │
│ ---                            ┆ ---       │
│ Utf8                           ┆ Utf8      │
╞════════════════════════════════╪═══════════╡
│ https://www.google.com         ┆ HTML      │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ s3://daft-public-data/open-im… ┆ JPEG      │
╰────────────────────────────────┴───────────╯

(Showing first 2 of 2 rows)
```
