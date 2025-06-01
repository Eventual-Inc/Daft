# Hugging Face Datasets

Daft is able to read datasets directly from Hugging Face via the `hf://datasets/` protocol.

Since Hugging Face will [automatically convert](https://huggingface.co/docs/dataset-viewer/en/parquet) all public datasets to parquet format, we can read these datasets using the [`daft.read_parquet()`][daft.read_parquet] method.

!!! warning "Warning"

    This is limited to either public datasets, or PRO/ENTERPRISE datasets.

For other file formats, you will need to manually specify the path or glob pattern to the files you want to read, similar to how you would read from a local file system.


## Reading Public Datasets

=== "🐍 Python"

    ```python
    import daft

    df = daft.read_parquet("hf://datasets/username/dataset_name")
    ```

This will read the entire dataset into a DataFrame.

Not only can you read entire datasets, but you can also read individual files from a dataset.

=== "🐍 Python"

    ```python
    import daft

    df = daft.read_parquet("hf://datasets/username/dataset_name/file_name.parquet")
    # or a csv file
    df = daft.read_csv("hf://datasets/username/dataset_name/file_name.csv")

    # or a glob pattern
    df = daft.read_parquet("hf://datasets/username/dataset_name/**/*.parquet")
    ```

## Authorization

For authenticated datasets:

=== "🐍 Python"

    ```python
    from daft.io import IOConfig, HTTPConfig

    io_config = IoConfig(http=HTTPConfig(bearer_token="your_token"))
    df = daft.read_parquet("hf://datasets/username/dataset_name", io_config=io_config)
    ```

It's important to note that this will not work with standard tier private datasets.
Hugging Face does not auto convert private datasets to parquet format, so you will need to specify the path to the files you want to read.

=== "🐍 Python"

    ```python
    df = daft.read_parquet("hf://datasets/username/my_private_dataset", io_config=io_config) # Errors
    ```

to get around this, you can read all files using a glob pattern *(assuming they are in parquet format)*

=== "🐍 Python"

    ```python
    df = daft.read_parquet("hf://datasets/username/my_private_dataset/**/*.parquet", io_config=io_config) # Works
    ```
