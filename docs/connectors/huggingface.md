# Hugging Face Datasets

Daft has native support for reading from and writing to [Hugging Face datasets](https://huggingface.co/datasets).

To install all dependencies required for Daft's Hugging Face integrations, use the `huggingface` feature:
```
pip install 'daft[huggingface]'
```

## Reading From a Dataset

Daft is able to read datasets directly from Hugging Face using the [`daft.read_huggingface()`][daft.read_huggingface] function or via the `hf://datasets/` protocol.

### Reading an Entire Dataset

Using [`daft.read_huggingface()`][daft.read_huggingface], you can easily read a Hugging Face dataset.


=== "🐍 Python"

    ```python
    import daft

    df = daft.read_huggingface("username/dataset_name")
    ```

This will read the entire dataset into a DataFrame.

!!! warning "Warning"

    This is currently limited to either public datasets, or PRO/ENTERPRISE datasets, where Hugging Face will [automatically convert](https://huggingface.co/docs/dataset-viewer/en/parquet) the dataset to Parquet.

    For other datasets, you will need to manually specify the path or glob pattern to the files you want to read, similar to how you would read from a local file system. See the next section for an example.

### Reading Specific Files

Not only can you read entire datasets, but you can also read individual files from a dataset. Using a read function that takes in a path (such as [`daft.read_parquet()`][daft.read_parquet], [`daft.read_csv()`][daft.read_csv], or [`daft.read_json()`][daft.read_json]), specify a Hugging Face dataset path via the `hf://datasets/` prefix:

=== "🐍 Python"

    ```python
    import daft

    # read a specific Parquet file
    df = daft.read_parquet("hf://datasets/username/dataset_name/file_name.parquet")

    # or a csv file
    df = daft.read_csv("hf://datasets/username/dataset_name/file_name.csv")

    # or a set of Parquet files using a glob pattern
    df = daft.read_parquet("hf://datasets/username/dataset_name/**/*.parquet")
    ```

## Writing to a Dataset

Daft is able to write Parquet files to Hugging Face datasets using [`DataFrame.write_huggingface`][daft.DataFrame.write_huggingface]. Daft supports [Content-Defined Chunking](https://huggingface.co/blog/parquet-cdc) and [Xet](https://huggingface.co/blog/xet-on-the-hub) for faster, deduplicated writes.

Basic usage:

=== "🐍 Python"

    ```python
    import daft

    df: daft.DataFrame = ...

    df.write_huggingface("username/dataset_name")
    ```

See the [`DataFrame.write_huggingface`][daft.DataFrame.write_huggingface] API page for more info.

### Configuring Writes

[`DataFrame.write_huggingface`][daft.DataFrame.write_huggingface] accepts an [`IOConfig`][daft.io.IOConfig] which can be used to configure the write behavior. Here's an example of how to use it:


=== "🐍 Python"

    ```python
    import daft
    from daft.io import IOConfig, HuggingFaceConfig

    df: daft.DataFrame = ...

    io_config = IOConfig(hf=HuggingFaceConfig(
        use_content_defined_chunking=...,
        row_group_size=...,
        target_filesize=...,
        max_operations_per_commit=...,
    ))

    df.write_huggingface("username/dataset_name", io_config=io_config)
    ```

See the [`HuggingFaceConfig`][daft.io.HuggingFaceConfig] API page for more information about each argument.


## Authentication

The `token` parameter in [`HuggingFaceConfig`][daft.io.HuggingFaceConfig] can be used to specify a Hugging Face access token for requests that require authentication (e.g. reading private datasets or writing to a dataset).

Example of reading a dataset with a specified token:

=== "🐍 Python"

    ```python
    from daft.io import IOConfig, HuggingFaceConfig

    io_config = IOConfig(hf=HuggingFaceConfig(token="your_token"))
    df = daft.read_parquet("hf://datasets/username/dataset_name", io_config=io_config)
    ```

<!-- TODO: add info about `anonymous` once we automatically grab the token from the environment. -->
