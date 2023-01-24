DataFrame Operations
====================

In the previous section, we covered Expressions which are ways of expressing computation on a single column.

However, the Daft DataFrame is table containing some number of equal-length columns. Many operations affect the entire table at once, affecting the ordering or sizes of all columns.

This section of the user guide covers these operations, and how to use them.

Selecting Columns
-----------------

* ``df.select`` and its alias ``df[['col1', 'col2']]``
* You can also pass in expressions to df.select, as seen before
* df.exclude
* Adding a new column with ``df.with_column``

Selecting Rows
--------------

* ``df.limit``
* ``df.where``
* ``df.distinct``

Combining DataFrames
--------------------

* ``df.join``

Reordering Rows
---------------

* ``df.sort``
* ``df.repartition``

Exploding Columns
-----------------

* ``df.explode``
