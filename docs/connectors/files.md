# Creating File References with from_files

Daft provides [`daft.from_files()`][daft.io.from_files] to create a DataFrame of lazy file references from glob patterns. Unlike other read functions that immediately load file contents, `from_files` creates `File` objects that can be read on demand.

## Basic Usage

=== "Local Files"

    ```python
    import daft

    df = daft.from_files("/path/to/files/*.jpeg")
    df.show()
    ```

=== "Remote Files (S3)"

    ```python
    import daft

    df = daft.from_files("s3://my-bucket/images/*.png")
    df.show()
    ```

=== "Remote Files (GCS)"

    ```python
    import daft

    df = daft.from_files("gs://my-bucket/images/*.png")
    df.show()
    ```

## Output Schema

The `from_files` function returns a DataFrame with a single column:

| Column | Type | Description |
|--------|------|-------------|
| `file` | `File` | Lazy file references that can be read on demand |

## Wildcard Patterns

`from_files` supports standard glob patterns:

| Pattern | Description |
|---------|-------------|
| `*` | Matches any number of characters |
| `?` | Matches any single character |
| `[...]` | Matches any single character in the brackets |
| `**` | Recursively matches directories |

=== "Examples"

    ```python
    # All JPEG files in a directory
    df = daft.from_files("/images/*.jpeg")

    # Recursive search
    df = daft.from_files("/images/**/*.png")

    # Multiple patterns
    df = daft.from_files(["/images/*.jpeg", "/photos/*.jpeg"])
    ```

## Working with File Objects

The `File` type is a lazy reference that provides access to file metadata and content:

```python
import daft
from daft import col

df = daft.from_files("/path/to/files/*")

# Access file properties
df = df.select(
    col("file").file.path().alias("path"),
    col("file").file.size().alias("size_bytes"),
)
df.show()
```

## Use Cases

### Image Processing Pipeline

```python
import daft
from daft import col

# Create file references for images
df = daft.from_files("s3://bucket/images/**/*.jpg")

# Decode images when needed
df = df.select(
    col("file").file.path().alias("path"),
    col("file").image.decode().alias("image"),
)
df.show()
```

### Batch File Operations

```python
import daft
from daft import col

# Get file references with metadata
df = daft.from_files("/data/**/*")

# Filter by file properties before reading content
large_files = df.where(col("file").file.size() > 1_000_000)
large_files.show()
```

## Comparison with from_glob_path

`from_files` is similar to [`daft.from_glob_path()`][daft.from_glob_path] but returns `File` objects instead of path strings:

| Function | Returns | Use Case |
|----------|---------|----------|
| `from_glob_path` | `path` column (string) | When you need file paths only |
| `from_files` | `file` column (File) | When you need to read file content or access file properties |

```python
# from_glob_path returns paths
paths_df = daft.from_glob_path("/images/*.jpg")  # Column: path (string)

# from_files returns File objects
files_df = daft.from_files("/images/*.jpg")  # Column: file (File)
```

## Empty Results

If no files match the glob pattern(s), an empty DataFrame is returned instead of raising an error:

```python
df = daft.from_files("/nonexistent/*.txt")
df.show()  # Empty DataFrame with "file" column
```
