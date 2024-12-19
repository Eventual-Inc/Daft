# Reading/Writing Data

!!! failure "todo(docs): Should this page also have sql examples?"

Daft can read data from a variety of sources, and write data to many destinations.

## Reading Data

### From Files

DataFrames can be loaded from file(s) on some filesystem, commonly your local filesystem or a remote cloud object store such as AWS S3. Additionally, Daft can read data from a variety of container file formats, including CSV, line-delimited JSON and Parquet.

Daft supports file paths to a single file, a directory of files, and wildcards. It also supports paths to remote object storage such as AWS S3.
=== "🐍 Python"
    ```python
    import daft

    # You can read a single CSV file from your local filesystem
    df = daft.read_csv("path/to/file.csv")

    # You can also read folders of CSV files, or include wildcards to select for patterns of file paths
    df = daft.read_csv("path/to/*.csv")

    # Other formats such as parquet and line-delimited JSON are also supported
    df = daft.read_parquet("path/to/*.parquet")
    df = daft.read_json("path/to/*.json")

    # Remote filesystems such as AWS S3 are also supported, and can be specified with their protocols
    df = daft.read_csv("s3://mybucket/path/to/*.csv")
    ```

To learn more about each of these constructors, as well as the options that they support, consult the API documentation on [`creating DataFrames from files`](https://www.getdaft.io/projects/docs/en/stable/api_docs/creation.html#df-io-files).

### From Data Catalogs

If you use catalogs such as Apache Iceberg or Hive, you may wish to consult our user guide on integrations with Data Catalogs: [`Daft integration with Data Catalogs`](https://www.getdaft.io/projects/docs/en/stable/user_guide/integrations.html).

### From File Paths

Daft also provides an easy utility to create a DataFrame from globbing a path. You can use the [`daft.from_glob_path`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/io_functions/daft.from_glob_path.html#daft.from_glob_path) method which will read a DataFrame of globbed filepaths.

=== "🐍 Python"
    ``` python
    df = daft.from_glob_path("s3://mybucket/path/to/images/*.jpeg")

    # +----------+------+-----+
    # | name     | size | ... |
    # +----------+------+-----+
    #   ...
    ```

This is especially useful for reading things such as a folder of images or documents into Daft. A common pattern is to then download data from these files into your DataFrame as bytes, using the [`.url.download()`](https://getdaft.io/projects/docs/en/stable/api_docs/doc_gen/expression_methods/daft.Expression.url.download.html#daft.Expression.url.download) method.


### From Memory

For testing, or small datasets that fit in memory, you may also create DataFrames using Python lists and dictionaries.

=== "🐍 Python"
    ``` python
    # Create DataFrame using a dictionary of {column_name: list_of_values}
    df = daft.from_pydict({"A": [1, 2, 3], "B": ["foo", "bar", "baz"]})

    # Create DataFrame using a list of rows, where each row is a dictionary of {column_name: value}
    df = daft.from_pylist([{"A": 1, "B": "foo"}, {"A": 2, "B": "bar"}, {"A": 3, "B": "baz"}])
    ```

To learn more, consult the API documentation on [`creating DataFrames from in-memory data structures`](https://www.getdaft.io/projects/docs/en/stable/api_docs/creation.html#df-io-in-memory).

### From Databases

Daft can also read data from a variety of databases, including PostgreSQL, MySQL, Trino, and SQLite using the [`daft.read_sql`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/io_functions/daft.read_sql.html#daft.read_sql) method. In order to partition the data, you can specify a partition column, which will allow Daft to read the data in parallel.

=== "🐍 Python"
    ``` python
    # Read from a PostgreSQL database
    uri = "postgresql://user:password@host:port/database"
    df = daft.read_sql("SELECT * FROM my_table", uri)

    # Read with a partition column
    df = daft.read_sql("SELECT * FROM my_table", partition_col="date", uri)
    ```

To learn more, consult the [`SQL User Guide`](https://www.getdaft.io/projects/docs/en/stable/user_guide/integrations/sql.html) or the API documentation on [`daft.read_sql`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/io_functions/daft.read_sql.html#daft.read_sql).

## Reading a column of URLs

Daft provides a convenient way to read data from a column of URLs using the [`.url.download()`](https://getdaft.io/projects/docs/en/stable/api_docs/doc_gen/expression_methods/daft.Expression.url.download.html#daft.Expression.url.download) method. This is particularly useful when you have a DataFrame with a column containing URLs pointing to external resources that you want to fetch and incorporate into your DataFrame.

Here's an example of how to use this feature:

=== "🐍 Python"
    ```python
    # Assume we have a DataFrame with a column named 'image_urls'
    df = daft.from_pydict({
        "image_urls": [
            "https://example.com/image1.jpg",
            "https://example.com/image2.jpg",
            "https://example.com/image3.jpg"
        ]
    })

    # Download the content from the URLs and create a new column 'image_data'
    df = df.with_column("image_data", df["image_urls"].url.download())
    df.show()
    ```

``` {title="Output"}

+------------------------------------+------------------------------------+
| image_urls                         | image_data                         |
| Utf8                               | Binary                             |
+====================================+====================================+
| https://example.com/image1.jpg     | b'\xff\xd8\xff\xe0\x00\x10JFIF...' |
+------------------------------------+------------------------------------+
| https://example.com/image2.jpg     | b'\xff\xd8\xff\xe0\x00\x10JFIF...' |
+------------------------------------+------------------------------------+
| https://example.com/image3.jpg     | b'\xff\xd8\xff\xe0\x00\x10JFIF...' |
+------------------------------------+------------------------------------+

(Showing first 3 of 3 rows)
```

This approach allows you to efficiently download and process data from a large number of URLs in parallel, leveraging Daft's distributed computing capabilities.

## Writing Data

Writing data will execute your DataFrame and write the results out to the specified backend. The [`df.write_*(...)`](https://www.getdaft.io/projects/docs/en/stable/api_docs/dataframe.html#df-write-data) methods are used to write DataFrames to files or other destinations.

=== "🐍 Python"
    ``` python
    # Write to various file formats in a local folder
    df.write_csv("path/to/folder/")
    df.write_parquet("path/to/folder/")

    # Write DataFrame to a remote filesystem such as AWS S3
    df.write_csv("s3://mybucket/path/")
    ```

!!! note "Note"

    Because Daft is a distributed DataFrame library, by default it will produce multiple files (one per partition) at your specified destination. Writing your dataframe is a **blocking** operation that executes your DataFrame. It will return a new `DataFrame` that contains the filepaths to the written data.
