# Working with Files and URLs in Daft

Daft provides powerful capabilities for working with URLs, file paths, and remote storage systems.

Whether you're loading data from local files, cloud storage, or the web, Daft's URL and file handling makes it seamless to work with distributed data sources. Daft supports working with:

- **Local file paths**: `file:///path/to/file`, `/path/to/file`
- **S3**: `s3://bucket/path`, `s3a://bucket/path`, `s3n://bucket/path`
- **GCS**: `gs://bucket/path`
- **Azure**: `az://container/path`, `abfs://container/path`, `abfss://container/path`
- **HTTP/HTTPS URLs**: `http://example.com/path`, `https://example.com/path`
- **Hugging Face datasets**: `hf://dataset/name`
- **Unity Catalog volumes**: `vol+dbfs:/Volumes/unity/path`

## Using file discovery with optimized distributed reads

[`daft.from_glob_path`](../api/io/file_path.md) helps discover and size files, accepting wildcards and lists of paths. When paired with [`daft.functions.download`](../api/functions/download.md), the two functions enable optimized distributed reads of binary data from storage. This is ideal when your data will fit into memory or when you need the entire file content at once.

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({
        "urls": [
            "https://www.google.com",
            "s3://daft-public-data/open-images/validation-images/0001eeaf4aed83f9.jpg",
        ],
    })
    df = df.with_column("data", df["urls"].download())
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


## The [`daft.File`](../api/datatypes/file_types.md) Datatype

[`daft.File`](../api/datatypes/file_types.md) is particularly useful for working with large files that don't fit in memory or when you only need to access specific portions of a file. This is a common use case when working with audio or video data where loading the entire object is prohibitive. The `daft.File` Type is subclassed by the [`daft.AudioFile`](../api/datatypes/file_types.md) and [`daft.VideoFile`](../api/datatypes/file_types.md) types which streamline common operations. It provides a [pythonic file-like interface](https://docs.python.org/3/library/functions.html#open) with random access capabilities:

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

The [`daft.File`](../api/datatypes/file_types.md) datatype provides first-class support for handling file data across local and remote storage, enabling seamless file operations in distributed environments.

While the Python classes provide the interface, the actual implementation lives in Rust-based `PyDaftFile`, which maintains optimized backends for different storage types:

- Local filesystem access
- Remote object stores with buffered reading

This architecture allows us to implement storage-specific optimizations (like network buffering for S3 or HTTP) while presenting a consistent interface.

## Core Design Principles

1. `daft.File` works both within dataframes and as standalone objects
2. Optimized backend readers for different sources (buffered network access, etc.)
3. Consistent API regardless of storage location

`daft.File` mirrors the [file interface in Python](https://docs.python.org/3/library/functions.html#open), but is optimized for distributed computing. Due to its lazy nature, `daft.File` does not read the file into memory until it is needed. To enforce this pattern, `daft.File` must be used inside a context manager like `with file.open() as f:` This works within a [`daft.func`](../api/custom-code/func.md) or [`daft.cls`](../api/custom-code/cls.md) user-defined functions or in native Python code.

## Basic Usage

```python
import daft

# Basic python usage (outside a dataframe context)
file = daft.File("s3://bucket/image.jpg", io_config)
content = file.read()

# Custom user defined functions usage
@daft.func
def read_file(file: daft.File):
    with file.open() as f:
        return f.read()

# DataFrame usage
df = daft.from_pydict({"path": ["path/to/file.txt"]})
df = df.select(read_file(daft.col("path")))
df.show()
```

``` {title="Output"}
╭────────────────────────────────┬────────────────────────────────╮
│ path                           ┆ file                           │
│ ---                            ┆ ---                            │
│ String                         ┆ String                         │
╞════════════════════════════════╪════════════════════════════════╡
│ path/to/file.txt               ┆ Hello, world!                  │
╰────────────────────────────────┴────────────────────────────────╯
```

## Using daft.File to read code and walk the AST

Since `daft.File` works with any file type with a read method, we can use it to read code and walk the AST for use cases like extracting functions and their signatures for codebase intelligence.

```python
import daft

@daft.func(
    return_dtype=daft.DataType.list(
        daft.DataType.struct(
            {
                "name": daft.DataType.string(),
                "signature": daft.DataType.string(),
                "docstring": daft.DataType.string(),
                "start_line": daft.DataType.int64(),
            }
        )
    )
)
def extract_functions(file: daft.File):
    """Extract all function definitions from a Python file."""
    import ast

    with file.open() as f:
        file_content = f.read().decode("utf-8")

    tree = ast.parse(file_content)
    results = []

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            signature = f"def {node.name}({ast.unparse(node.args)})"
            if node.returns:
                signature += f" -> {ast.unparse(node.returns)}"

            results.append({
                "name": node.name,
                "signature": signature,
                "docstring": ast.get_docstring(node),
                "start_line": node.lineno,
                "end_line": node.end_lineno,
            })

    return results


if __name__ == "__main__":
    from daft.functions import file, unnest

    # Discover Python files
    df = (
        daft.from_glob_path("~/git/Daft/daft/functions/**/*.py") # Add your own path here
        .with_column("file", file(daft.col("path")))
        .with_column("functions", extract_functions(daft.col("file")))
        .explode("functions")
        .select(daft.col("path"), unnest(daft.col("functions")))
    )

    df.show(3) # Show the first 3 rows of the dataframe
```

``` {title="Output"}
╭────────────────────────────────┬─────────────────────────────┬────────────────────────────────┬────────────────────────────────┬────────────╮
│ path                           ┆ name                        ┆ signature                      ┆ docstring                      ┆ start_line │
│ ---                            ┆ ---                         ┆ ---                            ┆ ---                            ┆ ---        │
│ String                         ┆ String                      ┆ String                         ┆ String                         ┆ Int64      │
╞════════════════════════════════╪═════════════════════════════╪════════════════════════════════╪════════════════════════════════╪════════════╡
│ file:///Users/myusername007/g… ┆ monotonically_increasing_id ┆ def monotonically_increasing_… ┆ Generates a column of monoton… ┆ 14         │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
│ file:///Users/myusername007/g… ┆ eq_null_safe                ┆ def eq_null_safe(left: Expre…  ┆ Performs a null-safe equality… ┆ 52         │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
│ file:///Users/myusername007/g… ┆ cast                        ┆ def cast(expr: Expression, dt… ┆ Casts an expression to the gi… ┆ 68         │
╰────────────────────────────────┴─────────────────────────────┴────────────────────────────────┴────────────────────────────────┴────────────╯

(Showing first 3 rows)
```
