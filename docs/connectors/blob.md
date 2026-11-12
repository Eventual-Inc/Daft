# Reading Files as Blobs

Daft can read files as raw bytes into DataFrames using [`daft.read_blob()`][daft.io.read_blob], similar to DuckDB's `read_blob` function. This is useful for loading non-tabular files such as images, audio, PDFs, or any other binary data into a DataFrame, with one row per file.

## Basic Usage

Read files matching a path or glob pattern, where each file becomes a row:

=== "Local Files"

    ```python
    import daft

    df = daft.read_blob("/path/to/files/*.jpeg")
    df.show()
    ```

=== "Remote Files (S3)"

    ```python
    import daft
    from daft.io import S3Config, IOConfig

    io_config = IOConfig(s3=S3Config(region_name="us-west-2", anonymous=True))
    df = daft.read_blob("s3://my-bucket/images/*.jpeg", io_config=io_config)
    df.show()
    ```

=== "Remote Files (GCS)"

    ```python
    import daft
    from daft.io import GCSConfig, IOConfig

    io_config = IOConfig(gcs=GCSConfig(anonymous=True))
    df = daft.read_blob("gs://my-bucket/images/*.jpeg", io_config=io_config)
    df.show()
    ```

## Output Schema

The `read_blob` function returns a DataFrame with the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `path` | `string` | Path to the file |
| `size` | `int64` | Size of the file in bytes |
| `content` | `binary` | Raw bytes contents of the file |

## Glob Patterns

`read_blob` supports the same wildcards as other Daft readers:

```python
# All .png files in a directory
df = daft.read_blob("/data/*.png")

# Recursive search
df = daft.read_blob("/data/**/*.png")

# Multiple paths
df = daft.read_blob(["/data/a.bin", "/data/b.bin"])
```

## Error Handling

By default, a download error for any file raises immediately. Set `on_error="null"` to log the error and fall back to a null `content` value instead:

```python
df = daft.read_blob("/data/*.bin", on_error="null")
```

## Common Use Cases

### Decoding Images

Read image files and decode them for downstream processing:

```python
import daft
from daft.functions import decode_image

df = daft.read_blob("s3://my-bucket/images/*.jpeg", io_config=io_config)
df = df.with_column("image", decode_image(df["content"]))
```

### Detecting File Types

Inspect magic bytes to determine the MIME type of each file:

```python
import daft
from daft.functions import guess_mime_type

df = daft.read_blob("/data/**/*")
df = df.with_column("mime", guess_mime_type(df["content"]))
```

## Relationship to Other APIs

`read_blob` is a convenience API composed of [`daft.from_glob_path()`][daft.from_glob_path] (which lists files with their `path` and `size`) and the [`download`][daft.functions.download] expression (which fetches file contents as bytes). Use those building blocks directly if you need more control, such as downloading only a filtered subset of files.
