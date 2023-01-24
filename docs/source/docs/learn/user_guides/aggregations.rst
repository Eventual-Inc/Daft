Aggregations and Grouping
=========================

Some operations such as the sum or the average of a column are called **aggregations**. Aggregations are operations that reduce the number of rows in a column.

For a full list of available aggregations, see: :ref:`df-aggregations`.

Global Aggregations
-------------------

An aggregation can be applied on an entire DataFrame, for example to get the mean on a specific column:

.. code:: python

    from daft import DataFrame

    df = DataFrame.from_pydict({
        "class": ["a", "a", "b", "b"],
        "score": [10, 20., 30., 40],
    })

    df.mean("score").show()

.. code:: none

    +---------+
    |   score |
    |   FLOAT |
    +=========+
    |      25 |
    +---------+
    (Showing first 1 of 1 rows)

Grouped Aggregations
--------------------

Aggregations can also be called on a "Grouped DataFrame". For the above example, perhaps we want to get the mean "score" not for the entire DataFrame, but for each "class".

Let's run the mean of column "score" again, but this time grouped by "class":

.. code:: python

    df.groupby("class").mean("score").show()

.. code:: none

    +----------+---------+
    | class    |   score |
    | STRING   |   FLOAT |
    +==========+=========+
    | b        |      35 |
    +----------+---------+
    | a        |      15 |
    +----------+---------+
    (Showing first 2 of 2 rows)
