User Guide
==========

Welcome to the Daft Dataframe.

This guide covers Daft in depth, containing both introductions to important concepts as well as examples of Daft usage in practice. It can be read in the order presented, but users should also consult sections of the guide that are most relevant to their current needs.

For a solid understanding of core Daft concepts, we recommend reading :doc:`user_guides/intro-dataframes` and :doc:`user_guides/expressions` which cover using Daft DataFrame and Expressions.

Daft in 100 words
-----------------

Daft is a Python dataframe library. A dataframe is just a table consisting of rows and columns.

* **It is fast** - Daft kernels are written and accelerated using Rust on Apache Arrow arrays.

* **It is flexible** - you can work with any Python object in a Daft Dataframe.

* **It is interactive** - Daft provides a first-class notebook experience.

* **It is scalable** - Daft uses out-of-core algorithms to work with datasets that cannot fit in memory.

* **It is distributed** - Daft scales to a cluster of machines using Ray to crunch terabytes of data.

* **It is intelligent** - Daft performs query optimizations to speed up your work.

.. toctree::
    :maxdepth: 1

    user_guides/intro-dataframes
    user_guides/datatypes
    user_guides/expressions
    user_guides/read-write
    user_guides/dataframe-operations
    user_guides/aggregations
    user_guides/udf
    user_guides/scaling-up
    user_guides/partitioning
..  user_guides/missing-data
..  user_guides/python-columns
..  user_guides/retrieving-data
