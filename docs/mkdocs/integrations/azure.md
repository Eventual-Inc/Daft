# Microsoft Azure

Daft is able to read/write data to/from Azure Blob Store, and understands natively the URL protocols `az://` and `abfs://` as referring to data that resides in Azure Blob Store.

!!! warning "Warning"

    Daft currently only supports globbing and listing files in storage accounts with [hierarchical namespaces](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-namespace) enabled. Hierarchical namespaces enable Daft to use its embarrassingly parallel globbing algorithm to improve performance of listing large nested directories of data. Please file an [issue](https://github.com/Eventual-Inc/Daft/issues) if you need support for non-hierarchical namespace buckets! We'd love to support your use-case.

## Authorization/Authentication

In Azure Blob Service, data is stored under the hierarchy of:

1. Storage Account
2. Container (sometimes referred to as "bucket" in S3-based services)
3. Object Key

URLs to data in Azure Blob Store come in the form: `az://{CONTAINER_NAME}/{OBJECT_KEY}`.

Given that the Storage Account is not a part of the URL, you must provide this separately.

### Rely on Environment

You can rely on Azure's [environment variables](https://learn.microsoft.com/en-us/azure/storage/blobs/authorize-data-operations-cli#set-environment-variables-for-authorization-parameters) to have Daft automatically discover credentials.

Please be aware that when doing so in a distributed environment such as Ray, Daft will pick these credentials up from worker machines and thus each worker machine needs to be appropriately provisioned.

If instead you wish to have Daft use credentials from the "driver", you may wish to manually specify your credentials.

### Manually specify credentials

You may also choose to pass these values into your Daft I/O function calls using an [`daft.io.AzureConfig`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/io_configs/daft.io.AzureConfig.html#daft.io.AzureConfig) config object.

<!-- add SQL AzureConfig https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/sql_funcs/daft.sql._sql_funcs.AzureConfig.html#daft.sql._sql_funcs.AzureConfig -->

[`daft.set_planning_config`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/configuration_functions/daft.set_planning_config.html#daft.set_planning_config) is a convenient way to set your [`daft.io.IOConfig`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/io_configs/daft.io.IOConfig.html#daft.io.IOConfig) as the default config to use on any subsequent Daft method calls.

=== "🐍 Python"

    ```python
    from daft.io import IOConfig, AzureConfig

    # Supply actual values for the storage_account and access key here
    io_config = IOConfig(azure=AzureConfig(storage_account="***", access_key="***"))

    # Globally set the default IOConfig for any subsequent I/O calls
    daft.set_planning_config(default_io_config=io_config)

    # Perform some I/O operation
    df = daft.read_parquet("az://my_container/my_path/**/*")
    ```

Alternatively, Daft supports overriding the default IOConfig per-operation by passing it into the `io_config=` keyword argument. This is extremely flexible as you can pass a different [`daft.io.AzureConfig`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/io_configs/daft.io.AzureConfig.html#daft.io.AzureConfig) per function call if you wish!

=== "🐍 Python"

    ```python
    # Perform some I/O operation but override the IOConfig
    df2 = daft.read_csv("az://my_container/my_other_path/**/*", io_config=io_config)
    ```

### Connect to Microsoft Fabric/OneLake

If you are connecting to storage in OneLake or another Microsoft Fabric service, set the `use_fabric_endpoint` parameter to `True` in the [`daft.io.AzureConfig`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/io_configs/daft.io.AzureConfig.html#daft.io.AzureConfig) object.

=== "🐍 Python"

    ```python
    from daft.io import IOConfig, AzureConfig

    io_config = IOConfig(
        azure=AzureConfig(
            storage_account="onelake",
            use_fabric_endpoint=True,

            # Set credentials as needed
        )
    )

    df = daft.read_deltalake('abfss://[WORKSPACE]@onelake.dfs.fabric.microsoft.com/[LAKEHOUSE].Lakehouse/Tables/[TABLE]', io_config=io_config)
    ```
