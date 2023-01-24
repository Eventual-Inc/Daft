Aggregations and Grouping
=========================

Some operations such as the sum or the average of a column are called **aggregations**. Aggregations are operations that reduce the number of rows in a column.

An aggregation can be applied on an entire DataFrame, for example to run a sum on specific columns:

.. code:: python

    df.sum("A", "B").show()

.. code:: none

    +-----------+---------+
    |         A |       B |
    |   INTEGER |   FLOAT |
    +===========+=========+
    |        10 |      12 |
    +-----------+---------+
    (Showing first 1 of 1 rows)

Aggregations can also be called on a "Grouped DataFrame", which just means that we want to perform aggregations using every unique value in the "groupby column(s)" as a key.

For example, to run a sum on A and B again, but grouped by column C:

.. code:: python

    df.groupby("C").sum("A", "B").show()

.. code:: none

    +-----------+-----------+---------+
    | C         |         A |       B |
    | LOGICAL   |   INTEGER |   FLOAT |
    +===========+===========+=========+
    | false     |         7 |       8 |
    +-----------+-----------+---------+
    | true      |         3 |       4 |
    +-----------+-----------+---------+
    (Showing first 2 of 2 rows)

Since C is a LOGICAL column, there are only two keys: ``true`` and ``false``. However this works on other column types as well, producing one row per unique value in the key column.

For a deeper dive into Aggregations, including multi-column groupbys and mixed aggregations, skip to: :doc:`aggregations`.
