User Defined Functions (UDFs)
=============================

User-Defined Functions (UDFs) are a mechanism to run Python code on the data that lives in a DataFrame.

A UDF can be used just like :doc:`Expressions <../user_guide/expressions>`, allowing users to express computation that
should be executed by Daft lazily.

To write a UDF, you should use the :func:`@udf <daft.udfs.udf>` decorator, which can decorate either a Python
function or a Python class, producing either a :class:`StatelessUDF <daft.udfs.StatelessUDF>` or
:class:`StatefulUDF <daft.udfs.StatefulUDF>` respectively.

For more details, please consult the :doc:`UDF User Guide <../user_guide/udf>`

.. currentmodule:: daft

Creating UDFs
=============

.. autofunction::
    udf

Using UDFs
==========

.. autoclass:: daft.udfs.StatelessUDF
   :members:
   :special-members: __call__

.. autoclass:: daft.udfs.StatefulUDF
   :members:
   :special-members: __call__
