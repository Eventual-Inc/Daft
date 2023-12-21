.. _df-input-output:

Input/Output
============

.. currentmodule:: daft

Configuration
-------------

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

