Dask Migration Guide
====================

This migration guide explains the most important points that anyone familiar with Dask should know when trying out Daft or migrating Dask workloads to Daft.

The guide includes an overview of technical, conceptual and syntax differences between the two libraries that you should be aware of. Understanding these differences will help you evaluate your choice of tooling and ease your migration from Dask to Daft.

When should I use Daft?
-----------------------

Dask and Daft are DataFrame frameworks built for distributed computing. Both libraries enable you to process large, tabular datasets in parallel, either locally or on remote instances on-prem or in the cloud.

If you are currently using Dask, you may want to consider migrating to Daft if you:

- Are working with **multimodal data types**, such as nested JSON, tensors, Images, URLs, etc.,
- Need faster computations through **query planning and optimization**,
- Are executing **machine learning workloads** at scale,
- Need deep support for **data catalogs, predicate pushdowns and metadata pruning** from Iceberg, Delta, and Hudi
- Want to benefit from **native Rust concurrency**

You may want to stick with using Dask if you:

- Want to only write **pandas-like syntax**,
- Need to parallelize **array-based workloads** or arbitrary **Python code that does not involve DataFrames** (with Dask Array, Dask Delayed and/or Dask Futures)

The following sections explain conceptual and technical differences between Dask and Daft. Whenever relevant, code snippets are provided to illustrate differences in syntax.

Daft does not use an index
--------------------------

Dask aims for as much feature-parity with pandas as possible, including maintaining the presence of an Index in the DataFrame. But keeping an Index is difficult when moving to a distributed computing environment. Dask doesn’t support row-based positional indexing (with .iloc) because it does not track the length of its partitions. It also does not support pandas MultiIndex. The argument for keeping the Index is that it makes some operations against the sorted index column very fast. In reality, resetting the Index forces a data shuffle and is an expensive operation.

Daft drops the need for an Index to make queries more readable and consistent. How you write a query should not change because of the state of an index or a reset_index call. In our opinion, eliminating the index makes things simpler, more explicit, more readable and therefore less error-prone. Daft achieves this by using the [Expressions API](/user_guide/basic_concepts/expressions.rst).

In Dask you would index your DataFrame to return row `b` as follows:

`ddf.loc[[“b”]]`

In Daft, you would accomplish the same by using a `col` Expression to refer to the column that contains `b`:

`df.where(daft.col(“alpha”)==”b”)`

More about Expressions in the sections below.

Daft does not try to copy the pandas syntax
-------------------------------------------

Dask is built as a parallelizable version of pandas and Dask partitions are in fact pandas DataFrames. When you call a Dask function you are often applying a pandas function on each partition. This makes Dask relatively easy to learn for people familiar with pandas, but it also causes complications when pandas logic (built for sequential processing) does not translate well to a distributed context. When reading the documentation, Dask users will often encounter this phrase `“This docstring was copied from pandas.core… Some inconsistencies with the Dask version may exist.”` It is often unclear what these inconsistencies are and how they might affect performance.

Daft does not try to copy the pandas syntax. Instead, we believe that efficiency is best achieved by defining logic specifically for the unique challenges of distributed computing. This means that we trade a slightly higher learning curve for pandas users against improved performance and more clarity for the developer experience.

Daft eliminates manual repartitioning of data
---------------------------------------------

In distributed settings, your data will always be partitioned for efficient parallel processing. How to partition this data is not straightforward and depends on factors like data types, query construction, and available cluster resources. While Dask often requires manual repartitioning for optimal performance, Daft abstracts this away from users so you don’t have to worry about it.

Dask leaves repartitioning up to the user with guidelines on having partitions that are “not too large and not too many”. This is hard to interpret, especially given that the optimal partitioning strategy may be different for every query. Instead, Daft automatically controls your partitions in order to execute queries faster and more efficiently. As a side-effect, this means that Daft does not support partition indexing the way Dask does (i.e. “get me partition X”). If things are working well, you shouldn't need to index partitions like this.

Daft performs Query Optimization for optimal performance
--------------------------------------------------------

Daft is built with logical query optimization by default. This means that Daft will optimize your queries and skip any files or partitions that are not required for your query. This can give you significant performance gains, especially when working with file formats that support these kinds of optimized queries.

Dask currently does not support full-featured query optimization.

> Note: As of version 2024.3.0 Dask is slowly implementing query optimization as well. As far as we can tell this is still in early development and has some rough edges. For context see `the discussion <https://github.com/dask/dask/issues/10995_>`_ in the Dask repo.

Daft uses Expressions and UDFs to perform computations in parallel
------------------------------------------------------------------

Dask provides a `map_partitions` method to map computations over the partitions in your DataFrame. Since Dask partitions are pandas DataFrames, you can pass pandas functions to `map_partitions`. You can also map arbitrary Python functions over Dask partitions using `map_partitions`.

For example:

.. code:: python
    def my_function(**kwargs):
        return …

    res = ddf.map_partitions(my_function, **kwargs)


Daft implements two APIs for mapping computations over the data in your DataFrame in parallel: :doc:`Expressions <../user_guide/basic_concepts/expressions>` and :doc:`UDFs <../user_guide/daft_in_depth/udf>`. Expressions are most useful when you need to define computation over your columns.

.. code:: python

    # Add 1 to each element in column "A"
    df = df.with_column("A_add_one", daft.col(“A”) + 1)


You can use User-Defined Functions (UDFs) to run computations over multiple rows or columns:

.. code:: python

    # apply a custom function “crop_image” to the image column
    @daft.udf(...)
    def crop_image(**kwargs):
        …
        return …

    df = df.with_column(
        "cropped",
        crop_image(daft.col(“image”), **kwargs),
    )


Daft is built for Machine Learning Workloads
--------------------------------------------

Dask offers some distributed Machine Learning functionality through the `dask-ml library <https://ml.dask.org/>`_ . This library provides parallel implementations of a few common scikit-learn algorithms. Note that `dask-ml` is not a core Dask library and is not as actively maintained. It also does not offer support for deep-learning algorithms or neural networks.

Daft is built as a DataFrame API for distributed Machine learning. You can use Daft UDFs to apply Machine Learning tasks to the data stored in your Daft DataFrame, including deep learning algorithms from libraries like PyTorch. See :doc:`our Quickstart <../10-min>` for a toy example.

Daft supports Multimodal Data Types
-----------------------------------

Dask supports the same data types as pandas. Daft is built to support many more data types, including Images, nested JSON, tensors, etc. See :doc:`the documentation <../user_guide/daft_in_depth/datatypes>` for a list of all supported data types.

Distributed Computing and Remote Clusters
-----------------------------------------

Both Dask and Daft support distributed computing on remote clusters. In Dask, you create a Dask cluster either locally or remotely and perform computations in parallel there. Currently, Daft supports distributed cluster computing :doc:`with Ray <../user_guide/poweruser/distributed-computing>`. Support for running Daft computations on Dask clusters is on the roadmap.

Cloud support for both Dask and Daft is the same.

SQL Support
-----------

Dask does not natively provide full support for running SQL queries. You can use pandas-like code to write SQL-equivalent queries, or use the external `dask-sql library <https://dask-sql.readthedocs.io/en/latest/>`_.

Daft provides a read_sql method to read SQL queries into a DataFrame. Daft uses SQLGlot to build SQL queries, so it supports all databases that SQLGlot supports. Daft pushes down operations such as filtering, projections, and limits into the SQL query when possible. Full-featured support for SQL queries (as opposed to a DataFrame API) is in progress.

Daft combines Python with Rust and Pyarrow for optimal performance
------------------------------------------------------------------

Daft combines Python with Rust and Pyarrow for optimal performance (see :doc:`benchmarks <../faq/benchmarks>`). Under the hood, Table and Series are implemented in Rust on top of the Apache Arrow specification (using the Rust arrow2 library). This architecture means that all the computationally expensive operations on Table and Series are performed in Rust, and can be heavily optimized for raw speed. Python is most useful as a user-facing API layer for ease of use and an interactive data science user experience. Read :doc:`more <../faq/technical_architecture>`.
