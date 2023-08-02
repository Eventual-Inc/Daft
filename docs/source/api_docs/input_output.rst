.. _df-input-output:

Input/Output
============

.. currentmodule:: daft

Configuration
-------------

.. NOTE::
    Daft is currently building out its own native code for reading/writing data. These configuration objects allow
    users to control behavior when Daft runs native code, but otherwise will have no effect.

    These configurations are currently used in:

    1. :func:`daft.read_parquet`: controls behavior when reading DataFrames Parquet files using the native downloader
    2. :func:`Expression.url.download() <daft.expressions.expressions.ExpressionUrlNamespace.download>`: controls behavior when downloading bytes from URLs using the native downloader
    3. :func:`Table.read_parquet <daft.table.table.Table.read_parquet>`: (Advanced usecase!) controls behavior when reading a Daft Table from a Parquet file

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_configs

    daft.io.IOConfig
    daft.io.S3Config

In-Memory Data
--------------

.. _df-io-in-memory:

Python Objects
~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    daft.from_pylist
    daft.from_pydict
    daft.DataFrame.to_pydict

Arrow
~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions


.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    daft.from_arrow
    daft.DataFrame.to_arrow

Pandas
~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    daft.from_pandas
    daft.DataFrame.to_pandas

File Paths
~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    daft.from_glob_path

Files
-----

.. _df-io-files:

Parquet
~~~~~~~

.. _daft-read-parquet:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    daft.read_parquet
    daft.DataFrame.write_parquet

CSV
~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    daft.read_csv
    daft.DataFrame.write_csv

JSON
~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    daft.read_json

Integrations
------------

.. _df-io-integrations:

Ray Datasets
~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    daft.from_ray_dataset
    daft.DataFrame.to_ray_dataset

Dask
~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    daft.from_dask_dataframe
    daft.DataFrame.to_dask_dataframe

