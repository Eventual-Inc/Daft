Microsoft Azure
===============

Daft is able to read/write data to/from Azure Blob Store, and understands natively the URL protocols ``az://`` and ``abfs://`` as referring to data that resides
in Azure Blob Store.

.. WARNING::

    Daft currently only supports globbing and listing files in storage accounts with `hierarchical namespaces <https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-namespace>`_ enabled.

    Hierarchical namespaces enable Daft to use its embarrassingly parallel globbing algorithm to improve performance of listing large nested directories of data.

    Please file an issue if you need support for non-hierarchical namespace buckets! We'd love to support your use-case.

Specifying a Storage Account
----------------------------

In Azure Blob Service, data is stored under the hierarchy of:

1. Storage Account
2. Container (sometimes referred to as "bucket" in S3-based services)
3. Object Key

URLs to data in Azure Blob Store come in the form: ``az://{CONTAINER_NAME}/{OBJECT_KEY}``.

Given that the Storage Account is not a part of the URL, you must provide this separately. You can either rely on Azure's `environment variables <https://learn.microsoft.com/en-us/azure/storage/blobs/authorize-data-operations-cli#set-environment-variables-for-authorization-parameters>`_
or you may choose to pass these values into your Daft I/O function calls using an :class:`daft.io.AzureConfig` config object:

.. code:: python

    from daft.io import IOConfig, AzureConfig

    # Supply actual values for the storage_account and access key here
    io_config = IOConfig(azure=AzureConfig(storage_account="***", access_key="***"))

    # When calling any IO methods, you can pass in (potentially different!) IOConfigs
    # Daft will use the AzureConfig when it encounters URLs starting with `az://` or `abfs://`
    df = daft.read_parquet("az://my_container/my_path/**/*")
