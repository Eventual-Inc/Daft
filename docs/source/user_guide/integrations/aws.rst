Amazon Web Services
===================

Daft is able to read/write data to/from AWS S3, and understands natively the URL protocol ``s3://`` as referring to data that resides
in S3.

Authorization/Authentication
----------------------------

In AWS S3, data is stored under the hierarchy of:

1. Bucket: The container for data storage, which is the top-level namespace for data storage in S3.
2. Object Key: The unique identifier for a piece of data within a bucket.

URLs to data in S3 come in the form: ``s3://{BUCKET}/{OBJECT_KEY}``.

Rely on Environment
*******************

You can configure the AWS `CLI <https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html>`_ to have Daft automatically discover credentials.
Alternatively, you may specify your credentials in `environment variables <https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html>`_: ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``, and ``AWS_SESSION_TOKEN``.

Please be aware that when doing so in a distributed environment such as Ray, Daft will pick these credentials up from worker machines and thus each worker machine needs to be appropriately provisioned.

If instead you wish to have Daft use credentials from the "driver", you may wish to manually specify your credentials.

Manually specify credentials
****************************

You may also choose to pass these values into your Daft I/O function calls using an :class:`daft.io.S3Config` config object.

:func:`daft.set_planning_config` is a convenient way to set your :class:`daft.io.IOConfig` as the default config to use on any subsequent Daft method calls.

.. code:: python

    from daft.io import IOConfig, S3Config

    # Supply actual values for the se
    io_config = IOConfig(s3=S3Config(key_id="key_id", session_token="session_token", secret_key="secret_key"))

    # Globally set the default IOConfig for any subsequent I/O calls
    daft.set_planning_config(default_io_config=io_config)

    # Perform some I/O operation
    df = daft.read_parquet("s3://my_bucket/my_path/**/*")

Alternatively, Daft supports overriding the default IOConfig per-operation by passing it into the ``io_config=`` keyword argument. This is extremely flexible as you can
pass a different :class:`daft.io.S3Config` per function call if you wish!

.. code:: python

    # Perform some I/O operation but override the IOConfig
    df2 = daft.read_csv("s3://my_bucket/my_other_path/**/*", io_config=io_config)
