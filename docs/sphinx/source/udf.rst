User Defined Functions (UDFs)
=============================

User-Defined Functions (UDFs) are a mechanism to run Python code on the data that lives in a DataFrame.

A UDF can be used just like `Expressions <../core_concepts/#expressions>`_, allowing users to express computation that
should be executed by Daft lazily.

To write a UDF, you should use the :func:`@udf <daft.udf.udf>` decorator, which can decorate either a Python
function or a Python class, producing a :class:`UDF <daft.udf.UDF>`.

For more details, please consult the `User Guide <../core_concepts/#user-defined-functions-udf>`_.

.. currentmodule:: daft

Creating UDFs
=============

.. autofunction::
    udf

Using UDFs
==========

.. autoclass:: daft.udf.UDF
   :members:
   :special-members: __call__
