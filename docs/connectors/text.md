# Reading Text Files

Daft can read line-oriented text files into DataFrames using [`daft.read_text()`][daft.io.read_text]. This is useful for processing log files, configuration files, or any other plain text data.

## Basic Usage

Read a text file where each line becomes a row:

=== "Local File"

    ```python
    import daft

    df = daft.read_text("/path/to/file.txt")
    df.show()
    ```

=== "Remote File (S3)"

    ```python
    import daft
    from daft.io import S3Config, IOConfig

    io_config = IOConfig(s3=S3Config(region_name="us-west-2", anonymous=True))
    df = daft.read_text("s3://my-bucket/logs/*.txt", io_config=io_config)
    df.show()
    ```

=== "Remote File (GCS)"

    ```python
    import daft
    from daft.io import GCSConfig, IOConfig

    io_config = IOConfig(gcs=GCSConfig(anonymous=True))
    df = daft.read_text("gs://my-bucket/logs/*.txt", io_config=io_config)
    df.show()
    ```

## Output Schema

The `read_text` function returns a DataFrame with a single column:

| Column    | Type     | Description                                                                  |
|-----------|----------|------------------------------------------------------------------------------|
| `content` | `string` | Lines from the input files (or entire file content when `whole_text=True`)   |

## Reading Options

### Whole File Mode

Read each file as a single row instead of splitting by lines:

    ```python
    # Each file becomes a single row
    df = daft.read_text("/path/to/files/*.txt", whole_text=True)
    df.show()
    ```

This is useful when you need to process entire file contents at once, such as for document processing or when the file content shouldn't be split by newlines.

### Skip Blank Lines

By default, empty lines (after stripping whitespace) are skipped. To include them:

    ```python
    df = daft.read_text("/path/to/file.txt", skip_blank_lines=False)
    ```

When `whole_text=True`, this option skips files that are entirely blank.

### File Encoding

Specify the character encoding of input files (defaults to UTF-8):

    ```python
    df = daft.read_text("/path/to/file.txt", encoding="latin-1")
    ```

### Include File Path

Add a column with the source file path:

    ```python
    df = daft.read_text("/path/to/files/*.txt", file_path_column="source_file")
    df.show()
    ```

### Hive Partitioning

Infer partition columns from the file path:

    ```python
    # For paths like /data/year=2024/month=01/file.txt
    df = daft.read_text("/data/**/*.txt", hive_partitioning=True)
    df.show()  # Includes 'year' and 'month' columns
    ```

## Wildcard Patterns

`read_text` supports glob patterns for reading multiple files:

| Pattern | Description                                   |
|---------|-----------------------------------------------|
| `*`     | Matches any number of characters              |
| `?`     | Matches any single character                  |
| `[...]` | Matches any single character in the brackets  |
| `**`    | Recursively matches directories               |

=== "Examples"

    ```python
    # All .txt files in a directory
    df = daft.read_text("/logs/*.txt")

    # All .log files recursively
    df = daft.read_text("/logs/**/*.log")

    # Files matching a pattern
    df = daft.read_text("/logs/server-[0-9].txt")
    ```

## Use Cases

### Processing Log Files

    ```python
    import daft
    from daft import col

    # Read log files and filter for errors
    df = daft.read_text("/var/log/app/*.log", file_path_column="log_file")
    errors = df.where(col("content").str.contains("ERROR"))
    errors.show()
    ```

### Document Processing

    ```python
    import daft

    # Read entire documents
    df = daft.read_text("/documents/*.txt", whole_text=True, file_path_column="doc_path")

    # Process with embeddings or other analysis
    df.show()
    ```
