DataFrames
==========

Dataframes are the core unit of abstraction in Daft. They represent a table of data with Rows and Columns, and each Column has a type (learn more: :doc:`types_and_ops`).

.. _dataframe-loading-data:

Loading Data
------------

Data can be loaded with the ``DataFrame.from_*`` functions that work flexibly to load data both locally downloaded on your machine, or on cloud storage such as AWS S3.

From Files
^^^^^^^^^^

DataFrames can be loaded from file(s) on disk, or in remote storage. Daft supports file paths to a single file, a directory of files, and wildcards. It also supports paths to remote object storage such as AWS S3.

.. code:: python

    single_filepath = "/path/to/file.csv"
    directory_filepath = "/path/to/csv/folder/"
    wildcards_filepath = "/path/to/folder/*.csv"
    aws_s3_path = "s3://mybucket/path/to/files/*.csv"

The DataFrame class supports :ref:`constructors to load from file formats <df-file-construction-api>` such as Parquet, CSV, JSON and more.


From Memory
^^^^^^^^^^^

DataFrames also :ref:`constructors to load from in-memory datastructures <df-memory-construction-api>` such as Python dictionaries and lists.


From Filepaths
^^^^^^^^^^^^^^

Reading filepaths of files on disk, or in remote storage. This is useful when loading a folder of images/PDFs/Protobufs etc into a DataFrame, and will return a DataFrame of filepaths and each file's metadata.

.. code:: python

    df = DataFrame.from_filepaths("s3://mybucket/path/to/files/*.jpeg")
    df.show()

    # +----------+------+-----+
    # | name     | size | ... |
    # +----------+------+-----+
    #   ...

A common pattern is to then use the ``.url.download()`` function to download the contents of each file and manipulate them in Python!


.. From Databases
.. ^^^^^^^^^^^^^^

.. **[COMING SOON]** Reading from databases such as PostgreSQL, Snowflake, BigQuery and Apache Iceberg.


Queries and Data Processing
---------------------------

All querying and processing on DataFrames are **lazy**, meaning that they do not execute immediately but rather queues the operation up to run when the DataFrame is executed (see: :ref:`Execution <user-guide-execution>`).

Many queries and data processing tasks also involve Expressions, which are an API for expressing computations that need to be run over the columns of a DataFrame. To learn more, see: :doc:`expressions`.

Here are some of the basic querying/processing methods, for a full list please see the :ref:`DataFrame API Documentation <dataframe-api-operations>`


* :ref:`df.with_column(name, e) <df-with-column>`: Creates a new column with the provided ``name`` by evaluating the provided expression.
* :ref:`df.select(*e) <df-select>`: Selects columns in the DataFrame by evaluating the provided expressions.
* :ref:`df.where(e) <df-where>`: Filters a DataFrame according to the provided expression.
* :ref:`df.join(other_df) <df-join>`: Joins two DataFrames column-wise according to some provided key.
* :ref:`df.limit(n) <df-limit>`: Limits the DataFrame to ``n`` number of rows.
* :ref:`df.sort(e) <df-sort>`: Sorts the DataFrame according to the evaluated results of the provided expression.


Analytics and Group-Bys
-----------------------

Analytics involves running aggregations over entire columns, for example summing all elements in a column to get a total count or finding the average value of a given column.

* :ref:`df.sum(*e) <df-sum>`: Sums the provided expressions
* :ref:`df.mean(*e) <df-mean>`: Takes the mean average of the provided expressions

Often, users will also want to run an aggregation over groups instead of the entire DataFrame. For example, finding the average grade of all students but grouped by their class.

* :ref:`df.groupby(*e) <df-groupby>`: Groups the DataFrame by the provided expressions, where the resulting :doc:`GroupByDataFrame <../../api_docs/groupby>` can then be aggregated on a per-group basis.


.. _user-guide-execution:

Execution
---------

DataFrames are **lazy** - they do not execute any computation until you explicitly tell it to start running the work that you have defined for it.

To run your Dataframe, you will need to call a method that will execute the computations and dump results to the appropriate output location (whether to display in your notebook, or in storage on disk).

The following are operations that will execute the DataFrame:

Visualization
^^^^^^^^^^^^^

* :ref:`df.show(N) <df-show>`: execute dataframe and show results in a notebook
* :ref:`df.to_pandas() <df-to-pandas>`: execute dataframe and return results as a Pandas Dataframe

.. _dataframe-writing-data:

Writing Data
^^^^^^^^^^^^

* :ref:`df.write_*(...) <df-write-data>`: execute dataframe and write results in a file format such as Parquet or CSV.
