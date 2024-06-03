.. _df-input-output:

Dataframe Creation
==================

.. currentmodule:: daft

In-Memory Data
--------------

.. _df-io-in-memory:

Python Objects
~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    from_pylist
    from_pydict

Arrow
~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    from_arrow

Pandas
~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    from_pandas

Files
-----

.. _df-io-files:

Parquet
~~~~~~~

.. _daft-read-parquet:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    read_parquet

CSV
~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    read_csv

JSON
~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    read_json

File Paths
~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    from_glob_path

Data Catalogs
-------------

Apache Iceberg
~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    read_iceberg

Delta Lake
~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    read_deltalake

Apache Hudi
~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    read_hudi

Integrations
------------

.. _df-io-integrations:

Ray Datasets
~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    from_ray_dataset

Dask
~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    from_dask_dataframe

Databases
~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_functions

    read_sql
    read_lance
